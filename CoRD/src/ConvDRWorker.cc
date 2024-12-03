#include "ConvDRWorker.hh"
#include <fstream>
#include "RSUtil.hh"

// called by fetchMulWorker()
void ConvDRWorker::readThread(redisContext* rc, int id, int totNum, const string lostBlkName) //, int coef) {
{ 
  redisReply* rReply;
  int minNum;

  for (int i = 0; i < _packetCnt; i++) 
  {
    rReply = (redisReply*)redisCommand(rc, "BLPOP tmp:%s:%d 100", lostBlkName.c_str(), id);
    //redisAppendCommand(rc, "BLPOP tmp:%s:%d 100", lostBlkName.c_str(), id);
    //redisGetReply(rc, (void**)&rReply);

    memcpy(_diskPkts3[i][id], rReply -> element[1] -> str, _packetSize);

    // critical section start -------------------------
    _waitingToPullerMtx.lock();

    _waitingToPullerNum[id]++;
    minNum = _packetCnt;
    for (int j = 0; j < totNum; j++) 
    { 
      if (minNum > _waitingToPullerNum[j]) 
        minNum = _waitingToPullerNum[j];
    }

    if (minNum > _waitingToPull) 
    {
      _waitingToPull++;
      unique_lock<mutex> lck(_mainPullerMtx);
      _mainPullerCV.notify_one();
    }
    _waitingToPullerMtx.unlock();
    // critical section end -------------------------

    freeReplyObject(rReply);
  }
}

// malloc ecK memory pointer to recv data from ecK helpers and one memory pointer to save the computed data with recved data
void ConvDRWorker::fetchMulWorker(const vector<ConvMulJob>& jobs) 
{
  if (DR_WORKER_DEBUG)
    printf("%s: starts\n", __func__);
  int pktId, nj = (int)jobs.size(), reqj;
  redisContext* locCtx = initCtx(_localIP);
  vector<redisContext*> rcs;
  //for (int id = 0; id < _ecK; id++) rcs.push_back(findCtx(_ips[id]));
  for (int id = 0; id < _ecK; id++) 
    rcs.push_back(initCtx(_localIP));

  for (int j = 0; j < nj; j++) 
    if (jobs[j].nextIP == _localIP || nj == 1) 
    {
      reqj = j; 
      break;
    }
  
  const ConvMulJob& reqJob = jobs[reqj];

  if (DR_WORKER_DEBUG)
    printf("%s: reqj = %d, reqJob.IP = .%d\n", __func__, reqj, reqJob.nextIP >> 24);

  redisReply* rReply;

  struct timeval tv1, tv2, tv3;
  gettimeofday(&tv1, NULL);
  gettimeofday(&tv3, NULL);

  // 三级指针，第一级为包数量，第二级是eck+1(ecK个接收来自helper，另一个存储编码后的数据)，第三级是一个包的大小
  _diskPkts3 = (char***)calloc(_packetCnt, sizeof(char**)); //(jobs.size() + _ecK, sizeof(char**))
  if (_diskPkts3 == NULL) 
  {
    printf("%s::%s(): ERROR: calloc null\n", typeid(this).name(), __func__);
    exit(1);
  }

  /*
  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    for (int id = 0; id < _ecK; id++) {
      redisAppendCommand(locCtx, "BLPOP tmp:%s:%d 100", reqJob.lostBlkName.c_str(), id);
    }
  }
  */

  for (pktId = 0; pktId < _packetCnt; pktId ++) 
  {
    _diskPkts3[pktId] = (char**)calloc(_ecK + nj, sizeof(char*));
    if (_diskPkts3[pktId] == NULL) 
    {
      printf("%s::%s(): ERROR: calloc null\n", typeid(this).name(), __func__);
      exit(1);
    }

    for (int i = 0; i < _ecK + jobs.size(); i++) 
    {
      _diskPkts3[pktId][i] = (char*)calloc(_packetSize, sizeof(char));
      if (_diskPkts3[pktId][i] == NULL) 
      {
        printf("%s::%s(): ERROR: calloc null\n", typeid(this).name(), __func__);
        exit(1);
      }
    }

    /*
    for (int id = 0; id < _ecK; id++) {
      redisGetReply(locCtx, (void**)&rReply);
      memcpy(_diskPkts3[pktId][id], rReply -> element[1] -> str, _packetSize);
      freeReplyObject(rReply);
    }

    {
      _waitingToPull++;
      unique_lock<mutex> lck(_mainPullerMtx);
      _mainPullerCV.notify_one();
    }*/
  }

  thread rpopThreads[_ecK];
  _waitingToPullerNum = (unsigned int*)calloc(sizeof(unsigned int), _ecK);

  for (int id = 0; id < _ecK; id++) // id < _ecK, packets from helpers
  {  
    rpopThreads[id] = thread([=]{readThread(rcs[id], id, _ecK, reqJob.lostBlkName);});
  }

  for (int id = 0; id < _ecK; id++) 
  {
    rpopThreads[id].join();
  }

  for (int id = 0; id < _ecK; id++) 
    redisFree(rcs[id]);

  free(_waitingToPullerNum);

}

// recv the notify from readThread() and notify to the sendMulWorker()
void ConvDRWorker::requestorMulCompletion(const vector<ConvMulJob>& jobs) 
{
  int pktId, nj = (int)jobs.size(), reqj;
  for (int j = 0; j < nj; j++) 
  if (jobs[j].nextIP == _localIP) 
  {
    reqj = j; 
    break;
  }
  redisReply* rReply;

  char* multiplyTmp = (char*)calloc(_packetSize, sizeof(char));
  if (multiplyTmp == NULL) 
  {
    printf("%s::%s(): ERROR: calloc null\n", typeid(this).name(), __func__);
    exit(1);
  }

  struct timeval tv1, tv2, tv3, tv4;
  gettimeofday(&tv1, NULL);
  gettimeofday(&tv3, NULL);

  for (pktId = 0; pktId < _packetCnt; pktId++) 
  {

    while (pktId >= _waitingToPull) 
    {
      unique_lock<mutex> lck(_mainPullerMtx);
      _mainPullerCV.wait(lck);
    }

    for (int j = 0; j < nj; j++) 
    {
      for (int id = 0; id < _ecK; id++) 
      {
        gettimeofday(&tv4, NULL);
        memcpy(multiplyTmp, _diskPkts3[pktId][id], _packetSize);
        RSUtil::multiply(multiplyTmp, jobs[j].coefs[id], _packetSize);
        if (id == 0) 
          memcpy(_diskPkts3[pktId][_ecK + j], multiplyTmp, _packetSize);
        else 
          Computation::XORBuffers(_diskPkts3[pktId][_ecK + j], multiplyTmp, _packetSize);

        /*
        gettimeofday(&tv2, NULL);
        printf("LOG: %s: pkt %d, id %d, time %s, from start %s, computation %s\n", __func__, pktId, id,
            getTime(tv1, tv2).c_str(), 
            getTime(tv3, tv2).c_str(), getTime(tv4, tv2).c_str()); 
        gettimeofday(&tv1, NULL);
        */
      }

      {
        _waitingToSend ++;
        unique_lock<mutex> lck(_mainSenderMtx);
        _mainSenderCondVar.notify_one();
      }
    }

  }
}

// aggredate ecK data to one encoded data
// Note: the write thread to file (testfileOut) is in ECPipeInputStream.cc (pipeForwardWorker())
void ConvDRWorker::sendMulWorker(const vector<ConvMulJob>& jobs) 
{
  if (DR_WORKER_DEBUG) 
  {
    printf("%s: starts\n", __func__);
    printf("%s: jobs: ", __func__);
    for (auto& it : jobs) 
    {
      printf("(.%d, lost %s) ", it.nextIP >> 24, it.lostBlkName.c_str()); 
    }
    printf("\n");
  }
  int nj = jobs.size();

  redisReply* rReply;
  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  printf("OK?\n");
  for (int pktId = 0; pktId < _packetCnt; pktId ++) 
  {
    for (int j = 0; j < nj; j++) 
    {
      while (pktId * nj + j >= _waitingToSend) 
      {
        unique_lock<mutex> lck(_mainSenderMtx);
        _mainSenderCondVar.wait(lck);
      }
      if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt))
        printf("%s:(), pkt %d job %d, [%x]\n", __func__, pktId, j, *(int*)(_diskPkts3[pktId][_ecK + j]));

      rReply = (redisReply*)redisCommand(_selfCtx, "RPUSH %s %b", jobs[j].lostBlkName.c_str(), _diskPkts3[pktId][_ecK + j], _packetSize);

      if (pktOutput(pktId, _packetCnt) && j == nj - 1) 
      {
        gettimeofday(&tv2, NULL);
        printf("LOG: %s::%s(), pkt %d - job %d, takes %s\n", typeid(this).name(), __func__, pktId, j, getTime(tv1, tv2).c_str());
      }

      freeReplyObject(rReply);
    }
  }

  for (int pktId = 0; pktId < _packetCnt; pktId ++) 
  {
    for (int i = 0; i < _ecK + jobs.size(); i++)
      free(_diskPkts3[pktId][i]);
    free(_diskPkts3[pktId]);
  }
  free(_diskPkts3);
  _diskPkts3 = NULL;
}

// read data from disk for helper
void ConvDRWorker::readWorker(const string& fileName) 
{
  int fd = openLocalFile(fileName);

  int subId = 0, i, subBase = 0, pktId;
  int readLen, readl, base = _conf->_packetSkipSize; // _conf->_packetSkipSize is inited to 0

  for (i = 0; i < _packetCnt; i ++) 
  {
    if (_id != _ecK || CONV_OPT) 
    {
      _diskPkts[i] = (char*)malloc(_packetSize * sizeof(char));
      if (_diskPkts[i] == NULL) 
      {
        printf("%s::%s(): ERROR: malloc null\n", typeid(this).name(), __func__);
        exit(1);
      }
      readPacketFromDisk(fd, _diskPkts[i], _packetSize, base);

      if (!CONV_OPT) 
        RSUtil::multiply(_diskPkts[i], _coefficient, _packetSize);
    } 
    else 
    {
      _diskPkts[i] = (char*)calloc(sizeof(char), _packetSize);
      if (_diskPkts[i] == NULL) 
      {
        printf("%s::%s(): ERROR: calloc null\n", typeid(this).name(), __func__);
        exit(1);
      }
    }

    // notify through conditional variable
    {
      unique_lock<mutex> lck(_diskMtx[i]);   // _diskMtx = vector<mutex>(_packetCnt) in DRWorker.cc
      _diskFlag[i] = true;
      _diskCv.notify_one(); // it will not block if no conditional variable is waitting
    }

    base += _packetSize;
    if (pktOutput(i, _packetCnt))  
      cout << "LOG: ConvDRWorker::readWorker() read packet " << i << endl;
  }
  close(fd);
}


// Send data to the requestor for helper.
void ConvDRWorker::sendWorker(const char* redisKey, redisContext* rc) 
{
  // TODO: thinking about a lazy rubbish collecion...

  if (rc == NULL) 
    cout << "ERROR: null context" << endl;
  redisReply* rReply;

  struct timeval tv1, tv2, tv3, tv4;
  gettimeofday(&tv1, NULL);
  gettimeofday(&tv3, NULL);

  for (int i = 0; i < _packetCnt; i ++) 
  {
    while (i >= _waitingToSend) 
    {
      unique_lock<mutex> lck(_mainSenderMtx);  // lock()
      _mainSenderCondVar.wait(lck);  // block and unlock the _mainSenderCondVar, it will wake up until a notify is happen in _mainSenderCondVar
    } 

    gettimeofday(&tv4, NULL);

    // TODO: error handling
    rReply = (redisReply*)redisCommand(rc, "RPUSH %s %b", redisKey, _diskPkts[i], _packetSize);

    if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) 
    {
      printf("%s(): send pkt %d\n", __func__, i);
    }

    /*
    gettimeofday(&tv2, NULL);
    printf("LOG: %s::%s(), send pkt %d, takes %s, from start %s, push %s\n", typeid(this).name(), __func__, i, 
        getTime(tv1, tv2).c_str(), getTime(tv3, tv2).c_str(), getTime(tv4, tv2).c_str());

        */
    freeReplyObject(rReply);
  }
}

// ips[] is useless
// recv the mutex notify from readWorker, and notify the sendWork
void ConvDRWorker::requestorCompletion(string& lostBlkName, unsigned int ips[]) 
{
  int ecK = _ecK, pktId;
  redisReply* rReply;

  redisContext* rC[_ecK];
  if (_id == _ecK)   // requestor
  {
    for (int i = 0; i < _ecK; i ++) 
    {
      rC[i] = findCtx(_conf -> _localIP);
    }
  }

  for (pktId = 0; pktId < _packetCnt; pktId ++) 
  {
    // helper
    if (_id != _ecK) 
    {
      while (!_diskFlag[pktId]) 
      {
        unique_lock<mutex> lck(_diskMtx[pktId]);
        _diskCv.wait(lck);
      }
    }
    // requestor: recv and compute
    if (_id == _ecK) 
    {
      for (int i = 0; i < _ecK; i ++) 
      {
        if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) 
        {
          cout << __func__ << "(): waiting for blk " << i << " packet " << pktId << endl;
        }
        rReply = (redisReply*)redisCommand(rC[i], "BLPOP tmp:%s:%d 100", lostBlkName.c_str(), i);
        Computation::XORBuffers(_diskPkts[pktId], rReply -> element[1] -> str, _packetSize);
        freeReplyObject(rReply);
      }
    }

    {
      _waitingToSend ++;
      unique_lock<mutex> lck(_mainSenderMtx);
      _mainSenderCondVar.notify_one();
    }
  }
}

/*
readWorker()
  - mutex: _diskMtx[i], _diskCv
  - variable: _diskFlag[i]

requestorCompletion()
  - mutex: _diskMtx[i], _diskCv, _mainSenderMtx
  - variable: _waitingToSend

sendWorker()
  - mutex: _mainSenderMtx
  - variable: _waitingToSend

对于helper：
readWorker() ---(_diskMtx[i], _diskCv)---> requestorCompletion() ---(_mainSenderMtx, _mainSenderMtx)---> sendWorker()


readWorker()先读完一个分片后，对 _diskMtx[i] 上锁，然后标志 _diskFlag[i] 为 True，再对 _diskCv 调用 notify_one()，接下来开始读下一个分片

requestorCompletion()先判断 _diskFlag[i] 的真假
  - 若 _diskFlag[i] == False，进入while循环，先对 _diskMtx[i] 进行上锁，然后调用 wait() 阻塞在 _diskCv，直到 readWorker() 对 _diskCv 调用 notify_one() 
  - 若 _diskFlag[i] == True，跳出while循环，先 _waitingToSend ++，再对 _mainSenderMtx 进行上锁，再对 _mainSenderMtx 调用 notify_one()

sendWorker() 先判断 i 是否大于等于 _waitingToSend
  - 若大于等于，则对 _mainSenderMtx 进行上锁，然后调用 wait() 阻塞在 _mainSenderMtx，直到 requestorCompletion() 对 _mainSenderMtx 调用 notify_one() 后跳出while
  - 若小于，则跳出while循环，对第i个分片发送到目的节点

注意：
  - 条件变量调用wait()时，当被阻塞时会对条件变量进行解锁，方便其它引用该条件变量的对象进行上锁
  - 当调用函数 notify_one() 时，若没有条件变量被阻塞，该函数不会阻塞

*/

void ConvDRWorker::doProcess() 
{
  string lostBlkName, localBlkName;
  int subId, i, subBase, pktId, ecK;
  unsigned int nextIP, requestorIP;
  redisContext *nextCtx, *requestorCtx;
  bool reqHolder, isRequestor;
  unsigned int lostBlkNameLen, localBlkNameLen;
  const char* cmd;
  redisReply* rReply;
  timeval tv1, tv2;
  thread sendThread;
  
  thread passiveThread;
  if (_conf -> _fileSysType == "HDFS") 
  {
    passiveThread = thread([=]{passiveReceiver();});
  }

  vector<ConvMulJob> convMulJobs;

  while (true) 
  {
    // loop FOREVER
    rReply = (redisReply*)redisCommand(_selfCtx, "blpop dr_cmds 100");
    gettimeofday(&tv1, NULL);

    if (rReply -> type == REDIS_REPLY_NIL) 
    {
      if (DR_WORKER_DEBUG) cout << typeid(this).name() << "::"  << __func__ << "(): empty list" << endl;
    } 
    else if (rReply -> type == REDIS_REPLY_ERROR) 
    {
      if (DR_WORKER_DEBUG) cout << typeid(this).name() << "::"  << __func__ << "(): error happens" << endl;
      exit(1);
    } 
    else 
    {
      if (DR_WORKER_DEBUG) 
        cout << typeid(this).name() << "::"  << __func__ << "(): cmd recv'd" << endl;
      /** 
       * Parsing Cmd, previously
       *
       * Cmd format: 
       * [a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)]
       * a: ecK pos: 0 // if ((ecK & 0xff00) >> 1) == 1), requestor is a holder
       * b: requestor ip start pos: 4
       * c: prev ip start pos 8
       * d: next ip start pos 12
       * e: id pos 16
       * f: lost file name (4Byte lenght + length) start pos 20, 24
       * g: corresponding filename in local start pos ?, ? + 4
       */
      
      char ccmd[rReply -> element[1] -> len];
      unsigned int id;
      memcpy(ccmd, rReply -> element[1] -> str, rReply -> element[1] -> len);
      cmd = rReply -> element[1] -> str;
      memcpy((char*)&_ecK, ccmd, 4);
      memcpy((char*)&requestorIP, ccmd + 4, 4);
      memcpy((char*)&_coefficient, ccmd + 8, 4);
      memcpy((char*)&nextIP, ccmd + 12, 4);
      memcpy((char*)&id, ccmd + 16, 4);
      //memcpy((char*)&_id, ccmd + 16, 4);
      _id = id;
      // get file names

      if (_coefficient < 0) 
      {
        _ips = (unsigned int*)calloc(4, _ecK);
        memcpy((char*)_ips, ccmd + 20, 4 * _ecK);
        memcpy((char*)&lostBlkNameLen, ccmd + 20 + 4 * _ecK, 4);
        lostBlkName = string(ccmd + 24 + 4 * _ecK, lostBlkNameLen);

        if (DR_WORKER_DEBUG) 
        {
          cout << "cmd[8]<0, requestor:" << endl;
          cout << "  0, _ecK = " << _ecK << endl
               << "  4, requestorIP = ." << (requestorIP >> 24) << endl
               << "  8, _coef = " << _coefficient << endl
               << "  12, nextIP / fails = " << nextIP << endl
               << "  16: id = " << id << endl; 
        }
        _id = _ecK;
        convMulJobs.clear();
        
        /* 
          * Cmd format of "dr_cmds" for requestor:
          * [a(4)][b(4)][0xffffffff][m(4)][0(4)][c(4K)][d,e(4+l)][f1(8+4k+l1)]...[fm(8+4k+l2)]
          * a: ecK
          * b: requestor IP, start pos 4
          * -1: requestor label, start pos 8
          * m: number of failures, start pos 12
          * c: helper ips, start pos 20
          * d, e: requestor lost file length + names, start pos (20 + 4k)
          * f1: passive requestor IP 1, start pos (24 + 4k + l) -> passive requestor
          *     coef1, ..., coefk, start pos (28 + 4k + l)
          *     d, e: requestor lost file length + names, start pos (28 + 8k + l)
          * ...
          * fm: passive requestor IP m, start pos (...) -> requestor
          *     ... 
          * */
        
        int cmdBase = 24 + 4 * _ecK + lostBlkNameLen, passIP;
        int* coefs = (int*)calloc(sizeof(int), _ecK);
        if (coefs == NULL) 
        {
          printf("%s::%s(): ERROR: calloc null\n", typeid(this).name(), __func__);
          exit(1);
        }
        int lostFileLen;

        for (int i = 0; i < nextIP; i++) // nextIP == number of fails
        { 
          memcpy(&passIP, ccmd + cmdBase, 4);
	        printf("  %d: fail %d, .%d ", cmdBase, i, (passIP >> 24));

          memcpy(&lostFileLen, ccmd + cmdBase + 4, 4);
          string lostFile(ccmd + cmdBase + 8, lostFileLen);
          printf("%d %s\n", lostFileLen, lostFile.c_str());
          
          cmdBase += 8 + lostFileLen;
          printf("  %d: ", cmdBase);
	  
          for (int j = 0; j < _ecK; j++) 
          {
            memcpy(coefs + j, ccmd + cmdBase, 4);
            printf("(%d->%d), ", cmdBase, coefs[j]);
            cmdBase += 4;
          }
          printf("\n");
          
          convMulJobs.push_back(ConvMulJob(_ecK, passIP, coefs, lostFile));

          if (DR_WORKER_DEBUG) 
          {
            printf("Req %d:", i);
            printf("   IP: .%d\n   Coef: ", passIP >> 24);
            for (int j = 0; j < _ecK; j++) 
              printf("%d ", coefs[j]);
            cout << "   lostFile: " << lostFile << endl; 
          }
        }
      }
      else 
      { 
        /** 
         * Parsing Cmd
         *
         * Cmd format: helpers
         * [a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)]
         * a: ecK pos: 0 // if ((ecK & 0xff00) >> 1) == 1), requestor is a holder
         * b: requestor ip start pos: 4
         * >=0: helper label, start pos 8
         * f: lost file name of requestor[l], start pos 20
         * g: local file name of helper, start pos (24 + l)
         */
        memcpy(&lostBlkNameLen, ccmd + 20, 4);
        lostBlkName = string(ccmd + 24, lostBlkNameLen);
        memcpy(&localBlkNameLen, ccmd + 24 + lostBlkNameLen, 4);
        localBlkName = string(ccmd + 28 + lostBlkNameLen, localBlkNameLen);
        cout << "cmd[8]>=0, helper:" << endl
             << lostBlkName << ", " << localBlkName << endl;
      }

      if (DR_WORKER_DEBUG) 
      {
        cout << " lostBlkName: " << lostBlkName << endl
          << " ecK(0): " << _ecK << endl
          << " requestorIP(4): " << ip2Str(requestorIP) << endl
          << " _coefficient(8): " << _coefficient << endl
          << " id(16): " << _id  << endl
          << " nextIP / num of fails: " << nextIP << endl;
      }

      if (CONV_OPT == 0 || _coefficient > 0) // helper
      {
        cout << "starting readworker" << endl;
        thread diskThread([=]{readWorker(localBlkName);});  // read data from disk
        _waitingToSend = 0;
        cout << "_localIP: " << ip2Str(_conf -> _localIP) << endl;
        redisContext* nC = findCtx(requestorIP);
        string sendKey;
        if (_ecK == _id) // requestor
        {
          sendKey = lostBlkName;
        } 
        else  // helper
        {
          sendKey = "tmp:" + lostBlkName + ":" + to_string(_id);  // sendKey表示每个helper插入requestor的redis的数据的key (lostBlkName + : + node_id)
        }
        if (DR_WORKER_DEBUG) 
          cout << "starting sendWorker: sendKey = " << sendKey << endl;
        sendThread = thread([=]{sendWorker(sendKey.c_str(), nC);});  // send data to requestor

        requestorCompletion(lostBlkName, _ips);

        diskThread.join();
        sendThread.join();
        gettimeofday(&tv2, NULL);
        if (DR_WORKER_DEBUG)
          printf("%s: request takes %.6lf s\n", __func__, ((tv2.tv_sec - tv1.tv_sec)*1000000 + tv2.tv_usec - tv1.tv_usec) * 1.0 / 1000000);
        cleanup();
      }
      else  // requestor
      {
        _waitingToSend = 0;
        _waitingToPull = 0;

        thread fetchThread([=]{fetchMulWorker(convMulJobs);});
        thread sendThread([=]{sendMulWorker(convMulJobs);});
        requestorMulCompletion(convMulJobs);
        fetchThread.join();
        sendThread.join();
        gettimeofday(&tv2, NULL);
        if (DR_WORKER_DEBUG)
          printf("%s: request takes %.6lf s\n", __func__, ((tv2.tv_sec - tv1.tv_sec)*1000000 + tv2.tv_usec - tv1.tv_usec) * 1.0 / 1000000);
        cleanup();
      }
      // lazy clean up
      //freeReplyObject(rReply);
    }
  }
}

