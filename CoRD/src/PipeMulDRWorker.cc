#include "PipeMulDRWorker.hh"
#include <fstream>

void PipeMulDRWorker::readWorker(const string& fileName) {

  printf("LG: %s of %s START\n", __func__, fileName.c_str());

  string fullName = _conf -> _blkDir + '/' + fileName;
  int fd = open(fullName.c_str(), O_RDONLY);
  // add by Zuoru: debug
  if (DR_WORKER_DEBUG)
    cout << "Test by Zuoru: fd: " << fd << endl;
  int subIndex = 0;
  while (fd == -1) {
    fullName = _conf -> _blkDir + '/' + "subdir" + to_string(subIndex) + '/' + fileName;
    if (DR_WORKER_DEBUG)
      cout << "the current fullName is: " << fullName << endl;
    fd = open(fullName.c_str(), O_RDONLY);
    if (DR_WORKER_DEBUG && fd != -1) {
       cout << "Find the file in the dir: " << fullName << endl;
    }
    subIndex ++;
    if (subIndex > 10) {
       cout << "ERROR: Can not find the file in given dir!" << endl;
       exit(1);
    }    
  }

  int subId = 0, i, subBase = 0, pktId;
  int readLen, readl, base = _conf->_packetSkipSize;

  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  for (i = 0; i < _packetCnt; i ++) {
    //_diskPkts[i] = (char*) malloc(_packetSize * sizeof(char));
    readLen = 0;
    while (readLen < _packetSize) {
      if ((readl = pread(fd, 
              _diskPkts[i] + readLen, 
              _packetSize - readLen, 
              base + readLen)) < 0) {
        cerr << "ERROR During disk read " << endl;
        exit(1);
      } else {
        readLen += readl;
      }
    }
    RSUtil::multiply(_diskPkts[i], _coefficient, _packetSize);

    // notify through conditional variable
    unique_lock<mutex> lck(_diskMtx[i]);
    _diskFlag[i] = true;
    _diskCv.notify_one();
    //lck.unlock();
    base += _packetSize;
    if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) {
      gettimeofday(&tv2, NULL);
      cout << __func__ << " read packet " << i << " time = " << 
	      (tv2.tv_sec * 1000000 - tv1.tv_sec * 1000000 + tv2.tv_usec - tv1.tv_usec) * 1.0 / 1000000.0
	      << endl;
      gettimeofday(&tv1, NULL);
    }
  }
  close(fd);

  printf("LOG: %s of %s ENDS\n", __func__, fileName.c_str());
}

void PipeMulDRWorker::readMulWorker(const vector<PipeMulJob>& jobs) {
  if (jobs.empty()) {printf("empty jobs\n"); return;}
  printf("LOG: %s of %s START\n", __func__, jobs[0].lostBlkName.c_str());
  int nj = jobs.size();

  int fd = openLocalFile(jobs[0].localBlkName);

  int subId = 0, i, subBase = 0, pktId;
  int readLen, readl, base = _conf->_packetSkipSize;

  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  if (_sendMulPkt == NULL) {
    printf("%s: ERROR: sendMulPkt null\n", __func__);
    exit(1);
  }

  for (i = 0; i < _packetCnt; i ++) {
    //_diskPkts[i] = (char*) malloc(_packetSize * sizeof(char));

    readPacketFromDisk(fd, _diskPkts[i], _packetSize, base);

    for (int j = 0; j < (int)jobs.size(); j++) {
      memcpy(_sendMulPkt[i] + j * _packetSize, _diskPkts[i], _packetSize);
      RSUtil::multiply(_sendMulPkt[i] + j * _packetSize, jobs[j].coefficient, _packetSize);
    }

    // notify through conditional variable
    unique_lock<mutex> lck(_diskMtx[i]);
    _diskFlag[i] = true;
    _diskCv.notify_one();

    //lck.unlock();
    base += _packetSize;

    if (pktOutput(i, _packetCnt)) {
      gettimeofday(&tv2, NULL);
      printf("LOG: %s::%s(), pkt %d - job *, takes %s\n", typeid(this).name(), __func__, i, getTime(tv1, tv2).c_str());
    }
  }

  close(fd);

  printf("LOG: %s of %s END\n", __func__, jobs[0].lostBlkName.c_str());
}

/*
 * Send to itself. Then the next helper fetches from the previous IP.
 * */
void PipeMulDRWorker::sendWorker(const char* redisKey, redisContext* rc) {
  // TODO: thinking about a lazy rubbish collecion...

  redisReply* rReply;
  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  for (int i = 0; i < _packetCnt; i ++) {
    //cout << i << endl;
    while (i >= _waitingToSend) {
      unique_lock<mutex> lck(_mainSenderMtx);
      _mainSenderCondVar.wait(lck);
    } 

    // TODO: error handling
    rReply = (redisReply*)redisCommand(rc, "RPUSH %s %b", redisKey, _diskPkts[i], _packetSize);
    
    if (DR_WORKER_DEBUG && (i == 0 || i == _packetCnt - 1 || i % (_packetCnt / 10) == 0)) {
      gettimeofday(&tv2, NULL);
      cout << "PipeMulDRWorker send packet " << i << ", key = " << redisKey << ", time = " << 
	(tv2.tv_sec * 1000000 - tv1.tv_sec * 1000000 + tv2.tv_usec - tv1.tv_usec) * 1.0 / 1000000.0
	<< endl;
      gettimeofday(&tv1, NULL);
    }
    
    freeReplyObject(rReply);
  }
}

void PipeMulDRWorker::sendMulWorker(const vector<PipeMulJob>& jobs) {
  //vector<redisContext*> rcs;
  int nj = (int)jobs.size();

  if (jobs.empty()) {printf("empty jobs\n"); return;}
  redisReply* rReply;
  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  for (int i = 0; i < _packetCnt; i ++) {

    for (int j = 0; j < nj; j++) {
      
      if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) {
	gettimeofday(&tv2, NULL);
        printf("%s::%s(), waiting for pkt %d from .%d key = %s, takes %s\n", typeid(this).name(), __func__,
            i, jobs[j].prevIP >> 24, jobs[j].sendKey.c_str(), getTime(tv1, tv2).c_str());
      }

      while (i >= waitingToSend[j]) {
        unique_lock<mutex> lck(senderMtx[j]);
        senderCondVar[j].wait(lck);
      }

      if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) {
        printf("%s::%s(), get pkt %d, key = %s, takes %s\n", typeid(this).name(), __func__,
            i, jobs[j].sendKey.c_str(), getTime(tv1, tv2).c_str());
      }

      // TODO: error handling
      rReply = (redisReply*)redisCommand(_selfCtx, "RPUSH %s %b", 
        jobs[j].sendKey.c_str(), 
        _sendMulPkt[i] + j * _packetSize, 
        _packetSize);
      
      if (pktOutput(i, _packetCnt) && j == nj - 1) {
        gettimeofday(&tv2, NULL);
        printf("LOG: %s::%s(), pkt %d - job %d, key = %s, takes %s\n", typeid(this).name(), __func__, i, j, jobs[j].sendKey.c_str(), getTime(tv1, tv2).c_str());
      }
      
      freeReplyObject(rReply);
    }
  }

  /*
  for (int i = 0; i < _packetCnt; i++) free(_sendMulPkt[i]);
  free(_sendMulPkt);
  */
}

void PipeMulDRWorker::pullRequestorCompletion(
    string& lostBlkName, 
    redisContext* prevCtx) {
  bool reqHolder = (_ecK >> 8);
  int ecK = (_ecK & 0xff), subId = 0, subBase = 0, pktId;
  redisReply* rReply;

  if (_id != 0) {
    for (pktId = 0; pktId < _packetCnt; pktId ++) {
      redisAppendCommand(prevCtx, "BLPOP tmp:%s 100", lostBlkName.c_str());
    }
  }

  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    // read from disk
    while (!_diskFlag[pktId]) {
      unique_lock<mutex> lck(_diskMtx[pktId]);
      _diskCv.wait(lck);
    }
    // recv and compute
    if (_id != 0) {
      redisGetReply(prevCtx, (void**)&rReply);
      Computation::XORBuffers(_diskPkts[pktId], 
          rReply -> element[1] -> str,
          _packetSize);
    }
    _waitingToSend ++;
    unique_lock<mutex> lck(_mainSenderMtx);
    _mainSenderCondVar.notify_one();
    if (_id != 0) freeReplyObject(rReply);
    gettimeofday(&tv2, NULL);
    cout << "PipeMulDRWorker::pullRequestorCompletion() " << pktId << ", lost block = " << lostBlkName << ", time = " << 
      (tv2.tv_sec * 1000000 - tv1.tv_sec * 1000000 + tv2.tv_usec - tv1.tv_usec) * 1.0 / 1000000.0
      << endl;
    gettimeofday(&tv1, NULL);
  }
}

void PipeMulDRWorker::pullRequestorMulCompletion(const vector<PipeMulJob>& jobs) {
  bool reqHolder = (_ecK >> 8);
  int ecK = (_ecK & 0xff), subId = 0, subBase = 0, pktId, nj = jobs.size();
  redisReply* rReply;

  //redisContext* locCtx = initCtx(_localIP);
  vector<redisContext*> rcs;
  for (auto& it : jobs) {
    rcs.push_back(findCtx(it.prevIP));
  }

  printf("%s: lostBlkName is %s\n", __func__, jobs[0].lostBlkName.c_str());

  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    for (int j = 0; j < nj; j++) if (jobs[j].id != 0) {
      redisAppendCommand(rcs[j], "BLPOP tmp:%s 100", jobs[j].lostBlkName.c_str());
    }
  }

  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    // read from disk
    while (!_diskFlag[pktId]) {
      unique_lock<mutex> lck(_diskMtx[pktId]);
      _diskCv.wait(lck);
    }

    // recv and compute
    for (int j = 0; j < nj; j++) {
      if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) {
	gettimeofday(&tv2, NULL);
        printf("%s::%s(), waiting for pkt %d from .%d key = %s, takes %s\n", typeid(this).name(), __func__,
            pktId, jobs[j].prevIP >> 24, jobs[j].sendKey.c_str(), getTime(tv1, tv2).c_str());
      }

      if (jobs[j].id != 0) {
        if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt))
          printf("waiting pkt %d from .%d key = %s\n", pktId, jobs[j].prevIP >> 24, jobs[j].sendKey.c_str());

        redisGetReply(rcs[j], (void**)&rReply);
        Computation::XORBuffers(_sendMulPkt[pktId] + j * _packetSize, 
            rReply -> element[1] -> str,
            _packetSize);
      }

      if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) {
	gettimeofday(&tv2, NULL);
        printf("%s::%s(), get pkt %d from .%d key = %s, takes %s\n", typeid(this).name(), __func__,
            pktId, jobs[j].prevIP >> 24, jobs[j].sendKey.c_str(), getTime(tv1, tv2).c_str());
      }
      
      waitingToSend[j] ++;
      unique_lock<mutex> lck(senderMtx[j]);
      senderCondVar[j].notify_one();

      if (jobs[j].id != 0) freeReplyObject(rReply);
      if (pktOutput(pktId, _packetCnt) && j == nj - 1) {
        gettimeofday(&tv2, NULL);
        printf("LOG: %s::%s(), pkt %d - job %d, takes %s\n", typeid(this).name(), __func__, pktId, j, getTime(tv1, tv2).c_str());
      }
    }

  }

}

void PipeMulDRWorker::doProcess() {
  printf("LOG: %s::%s() starts\n", typeid(this).name(), __func__);
  string lostBlkName, localBlkName;
  int subId, i, subBase, pktId, ecK;
  unsigned int prevIP, nextIP, requestorIP;
  unsigned int jobInd, jobNum;
  redisContext *nextCtx, *requestorCtx;
  bool reqHolder, isRequestor;
  unsigned int lostBlkNameLen, localBlkNameLen;
  const char* cmd;
  redisReply* rReply;
  timeval tv1, tv2;
  thread sendThread;

  thread passiveThread;
  if (_conf -> _fileSysType == "HDFS") 
    passiveThread = thread([=]{passiveReceiver();});

  vector<PipeMulJob> pipeMulJobs;

  struct timeval tv_start;
  gettimeofday(&tv_start, NULL);

  while (true) {
    // loop FOREVER
    rReply = (redisReply*)redisCommand(_selfCtx, "blpop dr_cmds 100");
    gettimeofday(&tv1, NULL);

    if (rReply -> type == REDIS_REPLY_NIL) {
      if (DR_WORKER_DEBUG) cout << "PipeMulDRWorker::doProcess(): empty list" << endl;
      freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      if (DR_WORKER_DEBUG) cout << "PipeMulDRWorker::doProcess(): error happens" << endl;
      freeReplyObject(rReply);
    } else {
      gettimeofday(&tv2, NULL);
      printf("LOG: %s RECEIVED, length = %d, time = %s\n", __func__, rReply -> element[1] -> len, getTime(tv_start, tv2).c_str());
      /** 
       * Parsing Cmd
       *
       * Cmd format: 
       * [a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)][h(4Byte)][i(4Byte)]
       * a: ecK pos: 0 // if ((ecK & 0xff00) >> 1) == 1), requestor is a holder
       * b: requestor ip start pos: 4
       * c: prev ip start pos 8
       * d: next ip start pos 12
       * e: id pos 16
       * f: lost file name (4Byte lenght + length) start pos 20, 24
       * g: corresponding filename in local start pos ?, ? + 4
       * h: index of job number
       * i: total number of jobs
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

      if (!PIPE_MUL_OPT)
      {
        // start thread reading from disks
        thread diskThread([=]{readWorker(localBlkName);});

        _waitingToSend = 0;

        if (_ecK == 0x010000 || ((_ecK & 0x100) == 0 && _id == _ecK - 1)) {
          sendKey = lostBlkName;
        } else {
          sendKey = "tmp:" + lostBlkName;
        }
        //cout << "sendKey: " << sendKey << endl;

        sendThread = thread([=]{sendWorker(sendKey.c_str(), _selfCtx);});

        pullRequestorCompletion(lostBlkName, findCtx(prevIP));

        diskThread.join();
        sendThread.join();

        gettimeofday(&tv2, NULL);

        cout << "request starts at " << tv1.tv_sec << "." << tv1.tv_usec
          << "ends at " << tv2.tv_sec << "." << tv2.tv_usec << endl;
        cout << "id: " << _id << endl;
        cout << "next ip: " << ip2Str(nextIP) << endl;
        cout << "send key: " << sendKey << endl;

        // lazy clean up
        cleanup();
      }
      else if (jobInd == 1) {
	_sendMulPkt = (char**)calloc(_packetCnt, sizeof(char*));
	for (int i = 0; i < _packetCnt; i++) {
	  _sendMulPkt[i] = (char*)calloc(_packetSize * pipeMulJobs.size() + 4, sizeof(char));
	}

        thread diskThread([=]{readMulWorker(pipeMulJobs);});

        if (DR_WORKER_DEBUG)
          for (int i=0; i < (int)pipeMulJobs.size(); i++) {
            cout << "id: " << pipeMulJobs[i].id << ", next ip: " << pipeMulJobs[i].nextIP << ", send key:" << pipeMulJobs[i].sendKey << endl;
          }

        for (int j = 0; j < (int)pipeMulJobs.size(); j++) waitingToSend[j] = 0;

	sendThread = thread([=]{sendMulWorker(pipeMulJobs);});

        pullRequestorMulCompletion(pipeMulJobs);

        diskThread.join();
        sendThread.join();

        gettimeofday(&tv2, NULL);

        cout << "request starts at " << tv1.tv_sec << "." << tv1.tv_usec
          << "ends at " << tv2.tv_sec << "." << tv2.tv_usec << endl;

	for (int i = 0; i < _packetCnt; i++) {
	  free(_sendMulPkt[i]);
	}
	free(_sendMulPkt);

      }
      

      gettimeofday(&tv2, NULL);
      printf("LOG: %s FINISHED, time = %s\n", __func__, getTime(tv_start, tv2).c_str());
    }
  }
  if (_conf -> _fileSysType == "HDFS")
    passiveThread.join();
}



