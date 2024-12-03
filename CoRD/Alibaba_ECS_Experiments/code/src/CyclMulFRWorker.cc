#include "CyclMulFRWorker.hh"

#define OPTM 1 

#define USE_ID 0 

void CyclMulFRWorker::doProcess() {
  string lostBlkName, localBlkName;
  unsigned int nextIP, prevIP, requestorIP;
  const char* cmd;
  unsigned int lostBlkNameLen, localBlkNameLen;
  struct timeval tv1, tv2;

  thread passiveThread;
  if (_conf -> _fileSysType == "HDFS")
    passiveThread = thread([=]{passiveReceiver();});

  vector<PipeMulJob> pipeMulJobs;

  unsigned int jobInd, jobNum;

  while (true) {
    // loop FOREVER
    cout << typeid(this).name() << " waiting for cmds" << endl;
    redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop dr_cmds 0");
    if (rReply -> type == REDIS_REPLY_NIL) {
      if (DR_WORKER_DEBUG) cout << "CyclMulFRWorker::doProcess(): empty list" << endl;
      freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      if (DR_WORKER_DEBUG) cout << "CyclMulFRWorker::doProcess(): error happens" << endl;
      freeReplyObject(rReply);
    } else {
      gettimeofday(&tv1, NULL);
      cout << "recv'd cmd" << tv1.tv_sec << "s" << tv1.tv_usec << "us" << endl;
      /** 
       * Parsing Cmd
       *
       * Cmd format: 
       * [a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)]
       * a: ecK pos: 0 // if ((ecK & 0xff00) >> 1) == 1), requestor is a holder
       * b: requestor ip start pos: 4
       * c: prev ip start pos 8 // not used
       * d: next ip start pos 12
       * e: id pos 16
       * f: lost file name (4Byte lenght + length) start pos 20, 24
       */
      string sendKey;
      cmd = rReply -> element[1] -> str;
      memcpy((char*)&_ecK, cmd, 4);
      memcpy((char*)&requestorIP, cmd + 4, 4);
      memcpy((char*)&prevIP, cmd + 8, 4);
      memcpy((char*)&nextIP, cmd + 12, 4);
      memcpy((char*)&_id, cmd + 16, 4);

      // get file names
      memcpy((char*)&lostBlkNameLen, cmd + 20, 4);
      _coefficient = (lostBlkNameLen >> 16);
      lostBlkNameLen = (lostBlkNameLen & 0xffff);
      lostBlkName = string(cmd + 24, lostBlkNameLen);
      memcpy((char*)&localBlkNameLen, cmd + 24 + lostBlkNameLen, 4);
      localBlkName = string(cmd + 28 + lostBlkNameLen, localBlkNameLen);

      memcpy((char*)&jobInd, cmd + 28 + lostBlkNameLen + localBlkNameLen, 4);
      memcpy((char*)&jobNum, cmd + 32 + lostBlkNameLen + localBlkNameLen, 4);

      if (jobInd == jobNum) pipeMulJobs.clear();
      pipeMulJobs.push_back(PipeMulJob(_ecK, requestorIP, prevIP, nextIP, _id, _coefficient, lostBlkName, localBlkName));

      if (DR_WORKER_DEBUG) {
        cout << "dr_cmds\n" << " lostBlkName: " << lostBlkName << endl
          << " localBlkName: " << localBlkName << endl
          << " id: " << _id  << endl
          << " ecK: " << _ecK << endl
          << " coefficient: " << _coefficient << endl
          << " requestorIP: " << ip2Str(requestorIP) << endl
          << " prevIP: " << ip2Str(prevIP) << endl
          << " nextIP: " << ip2Str(nextIP) << endl
          << " jobInd: " << jobInd << endl
          << " jobNum: " << jobNum << endl;
      }

      freeReplyObject(rReply);
      _toSendCnt = 0;
      _readCnt = 0;

      if (jobInd == 1) {
        thread diskThread([=]{readerMul(pipeMulJobs);});
        thread sendThread([=]{senderMul(pipeMulJobs);});

        // modified: NUM2
        _toSendIds = new int[pipeMulJobs.size() * _packetCnt + 4];
        _toSendIdsUsing = false;
        pullerMul(pipeMulJobs);

        diskThread.join();
        sendThread.join();
      }
      gettimeofday(&tv2, NULL);
      if (DR_WORKER_DEBUG) 
        cout << typeid(this).name() << "::" << __func__ << "() start at " << tv1.tv_sec << "." << tv1.tv_usec
          << " end at " << tv2.tv_sec << "." << tv2.tv_usec << ", takes " << getTime(tv1, tv2) << endl;
      cleanup();
    }
  }
}

void CyclMulFRWorker::pullerMul(const vector<PipeMulJob>& jobs) {

  cout << __func__ << "() start, with " << jobs.size() << " jobs \n";

  int groupSize = _ecK - 1;
  int nj = jobs.size();
  // vector<int> retrieveCnts;
  redisReply* rReply;

  vector<redisContext*> rcs;
  for (auto& it : jobs) rcs.push_back(findCtx(it.prevIP));
  struct timeval tv1, tv2, tv3, tv4;

  int lastGroupBase = _packetCnt - _packetCnt % groupSize;
  int lastGroupCnt = _packetCnt % groupSize;

  fr (pktId, 0, _packetCnt - 1) {
    fr (j, 0, nj - 1) {
      if (jobs[j].id == 0 || pktId % groupSize != 0 || (pktId >= lastGroupBase && jobs[j].id > lastGroupCnt)) { 
        if (OPTM) {
          if (USE_ID && ((jobs[j].id == 0 && pktId % groupSize == 0) || (jobs[j].id > 1 && pktId % groupSize == 1)) && pktId < lastGroupBase) 
            redisAppendCommand(rcs[j], "BLPOP tmp:%s:%d 0", jobs[j].lostBlkName.c_str(), pktId / groupSize);
          else
            redisAppendCommand(rcs[j], "BLPOP tmp:%s 0", jobs[j].lostBlkName.c_str());
        }
        else
        redisAppendCommand(rcs[j], "BLPOP tmp:%s 0", jobs[j].lostBlkName.c_str());
      }
    }
  }

  gettimeofday(&tv1, NULL);
  gettimeofday(&tv3, NULL);
  int pktId, sliId;

  for (int i = 0; i < _packetCnt; i++) {
    for (int j = 0; j < nj; j++) {
      if (jobs[j].id == 0 || i % groupSize != 0 || (i >= lastGroupBase && jobs[j].id > lastGroupCnt)) { 
        redisGetReply(rcs[j], (void**)&rReply);
        sliId = *(int*)((char*)(rReply -> element[1] -> str) + _packetSize);
        pktId = _sli2pkt[j][sliId];
      }
      else {
        continue;
      }

      while (pktId * nj + j >= _readCnt) {
        unique_lock<mutex> lck(_toPullerMtx);
        _toPullerCV.wait(lck);
      }

      gettimeofday(&tv4, NULL);
      Computation::XORBuffers(_diskPktsMul[j][pktId], rReply -> element[1] -> str, _packetSize);

      // DEBUG
      if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) {
        if (jobs[j].id == 0 || pktId % groupSize != 0 || (pktId >= lastGroupBase && jobs[j].id > lastGroupCnt)) {
          printf("%s: pkt %d, from .%d [%x], after XOR: [%x]\n", __func__, pktId, jobs[j].prevIP >> 24,  
            *(int*)(rReply -> element[1] -> str),
            *(int*)(_diskPktsMul[j][pktId]));
        }
        else printf("%s: pkt %d, [%x]\n", __func__, pktId,
            *(int*)(_diskPktsMul[j][pktId]));
      }
        
      // DEBUG
      if (1) {
        gettimeofday(&tv2, NULL);
        printf("LOG: %s: pkt %d - sli %d, time %s, from start %s, current %s, %s from .%d\n", __func__, pktId, sliId, getTime(tv1, tv2).c_str(), 
            getTime(tv3, tv2).c_str(), getTime(tv4, tv2).c_str(), 
            (jobs[j].id == 0 || pktId % groupSize != 0 || (pktId >= lastGroupBase && jobs[j].id > lastGroupCnt)) ? "" : "not",
            jobs[j].prevIP >> 24);
        gettimeofday(&tv1, NULL);
      }

      // critical section
      _toSendIdsUsingMtx.lock();

      _toSendIds[_toSendCnt] = pktId;
      _toSendCnt ++;

      _toSendIdsUsingMtx.unlock();

      unique_lock<mutex> lck2(_toSenderMtx);
      _toSenderCV.notify_one();

      freeReplyObject(rReply);
    }
  }
  cout << __func__ << " finished" << endl;
}

void CyclMulFRWorker::senderMul(const vector<PipeMulJob>& jobs) {
  redisReply* rReply;
  cout << __func__ << "() start, with " << jobs.size() << " jobs \n";
  struct timeval tv1, tv2, tv3, tv4;

  redisContext* rc = _selfCtx;
  int nj = jobs.size();

  int groupSize = _ecK - 1;
  int lastGroupCnt = _packetCnt % (_ecK - 1), lastGroupBase;
  lastGroupBase = _packetCnt - lastGroupCnt;
  
  gettimeofday(&tv3, NULL);
  gettimeofday(&tv1, NULL);

  int pktId, pktId_real;

  for (int i = 0; i < _packetCnt ; i++) {

    for (int j = 0; j < nj; j++) {
      pktId = i; 

      while (pktId * nj + j >= _toSendCnt) {
        unique_lock<mutex> lck(_toSenderMtx);
        _toSenderCV.wait(lck);
      }

      pktId = _toSendIds[pktId * nj + j]; 
      pktId_real = _toSendIds[i * nj + j]; // i;  // i: incorrect, but fast

      gettimeofday(&tv4, NULL);

      if ((jobs[j].id != _ecK - 1 && pktId_real % (_ecK - 1) == _ecK - 2) || (jobs[j].id < lastGroupCnt && pktId_real == _packetCnt - 1)) {
        // to req
        rReply = (redisReply*)redisCommand(rc, 
            "rpush %s %b", 
            jobs[j].lostBlkName.c_str(), _diskPktsMul[j][pktId], _packetSize + 4);

        freeReplyObject(rReply);
      } else {
        // to next 
        rReply = (redisReply*)redisCommand(rc, 
            "rpush tmp:%s %b", 
            jobs[j].lostBlkName.c_str(), _diskPktsMul[j][pktId], _packetSize + 4);

        freeReplyObject(rReply);
      }

    }
  }

  // clean up
  for (int j = 0; j < nj; j++) {
    for (int i = 0; i < _packetCnt; i++) {
      free(_diskPktsMul[j][i]);
    }
    free(_diskPktsMul[j]);
  }
  free(_diskPktsMul);
  free(_toSendIds);

  _pkt2sli.clear();
  _sli2pkt.clear();
}

void CyclMulFRWorker::readerMul(const vector<PipeMulJob>& jobs) {
  cout << __func__ << "() start, with " << jobs.size() << " jobs \n";
  if (jobs.empty()) {printf("empty jobs\n"); return;}
  redisContext* rc = initCtx(_localIP);

  int groupSize = _ecK - 1, base = 0, readLen, readl;
  int lastGroupBase = _packetCnt - _packetCnt % groupSize;
  int lastGroupCnt = _packetCnt % groupSize;

  int nj = jobs.size();
  int round = _packetCnt % groupSize == 0 ? _packetCnt / groupSize : _packetCnt / groupSize + 1;
  struct timeval tv1, tv2, tv3;

  int fd = openLocalFile(jobs[0].localBlkName);

  gettimeofday(&tv1, NULL);
  gettimeofday(&tv3, NULL);
  
  _diskPktsMul = new char**[nj];

  const int INTV=3;
  int* tmpQueue = new int[nj * (_packetCnt + 4)];
  int tmpQueueFront = 0, tmpQueueRear = 0;

  for (int j = 0; j < nj; j++) {
    _pkt2sli.push_back({});
    _sli2pkt.push_back({});

    _diskPktsMul[j] = new char*[_packetCnt];
    base = 0;
    for (int i = 0; i < round; i ++) {
      for (int k = jobs[j].id - 1; k >= 0; k --) if (base + k < _packetCnt) {
        _sli2pkt[j][base + k] = _pkt2sli[j].size();
        _pkt2sli[j].push_back(base + k);
      }
      for (int k = groupSize - 1; k >= jobs[j].id; k --) if (base + k < _packetCnt) {
        _sli2pkt[j][base + k] = _pkt2sli[j].size();
        _pkt2sli[j].push_back(base + k); 
      }
      base += groupSize;
    }
  }

  for (int pktId = 0; pktId < _packetCnt; pktId ++) {
    for (int j = 0; j < nj; j++) {
      _diskPktsMul[j][pktId] = new char[_packetSize + 4];

      readPacketFromDisk(fd, _diskPktsMul[j][pktId], _packetSize, _pkt2sli[j][pktId] * _packetSize);

      // attach slice ID (not packet ID) to the end of the slice
      memcpy(_diskPktsMul[j][pktId] + _packetSize, &(_pkt2sli[j][pktId]), 4);

      if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) {
        gettimeofday(&tv2, NULL);
        printf("%s() read pkt %d at index %d, job %d, _readCnt = %d, takes %s", 
            __func__, pktId, _pkt2sli[j][pktId], j, _readCnt, getTime(tv1, tv2).c_str());
        printf(" ; first 4 bytes = [%x]\n", (int)(*((int*)_diskPktsMul[j][pktId])));
      }

      RSUtil::multiply(_diskPktsMul[j][pktId], jobs[j].coefficient, _packetSize);
      _readCnt ++;

      if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) {
        printf("pkt %d, after multiply, first 4 bytes = [%x]\n", pktId, (int)(*((int*)_diskPktsMul[j][pktId])));
      }

      unique_lock<mutex> lck(_toPullerMtx);
      _toPullerCV.notify_one();

      // put optimized sender to here
      if (jobs[j].id != 0 && pktId % groupSize == 0 && (pktId < lastGroupBase || jobs[j].id <= lastGroupCnt)) { 

        tmpQueue[tmpQueueFront++] = pktId;
        
        // critical section
        _toSendIdsUsingMtx.lock();
        if (_toSendCnt / (_conf -> _ecK - 1) + 3 >= tmpQueueRear) {
          if (tmpQueueRear < tmpQueueFront) {

            _toSendIds[_toSendCnt] = tmpQueue[tmpQueueRear++];
            _toSendCnt ++;

            unique_lock<mutex> lck2(_toSenderMtx);
            _toSenderCV.notify_one();
          }
        }
        _toSendIdsUsingMtx.unlock();

      }
    }
    
  }

  printf("LOG: %s before comp: rear = %d, front = %d\n", __func__, tmpQueueRear, tmpQueueFront);
  int prev = _toSendCnt;
  while (tmpQueueRear < tmpQueueFront) {

    // critical section
    _toSendIdsUsingMtx.lock();
    /*
    if (_toSendCnt != prev)
      printf("a new value _toSendCnt = %d\n", _toSendCnt), prev = _toSendCnt;
      */

    if (_toSendCnt / (_conf -> _ecK - 1) + 10 >= tmpQueueRear) {
      // critical section
      _toSendIds[_toSendCnt] = tmpQueue[tmpQueueRear++];
      _toSendCnt ++;

      unique_lock<mutex> lck2(_toSenderMtx);
      _toSenderCV.notify_one();
    }
    _toSendIdsUsingMtx.unlock();
  }

  printf("%s() ends\n", __func__);
}

