#include "PipeDRWorker.hh"

void PipeDRWorker::sendWorker(const char* redisKey, redisContext* rc) {
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


    rReply = (redisReply*)redisCommand(rc, "RPUSH %s %b", redisKey, _diskPkts[i], _packetSize);
    //redisAppendCommand(rc, "RPUSH %s %b", redisKey, _diskPkts[i], _packetSize);
    if (DR_WORKER_DEBUG && (i == 0 || i == _packetCnt - 1 || i % (_packetCnt / 10) == 0)) {
      printf("%s: packet send %d, [%x]\n", __func__, i, *(int*)(_diskPkts[i]));
      cout << "PipeDRWorker::sendWorker(): send packet " << i << endl;
      cout << "PipeDRWorker::sendWorker(): start at " + timeval2str(tv1) + ", end at " + timeval2str(tv2) + "\n";
    }
    
    //free(_diskPkts[i]);
    freeReplyObject(rReply);
  }
}

void PipeDRWorker::readWorker(const string& fileName) {
  string fullName = _conf -> _blkDir + '/' + fileName;
  int fd = open(fullName.c_str(), O_RDONLY);
  // add by Zuoru: debug
  cout << "Test by Zuoru: fd: " << fd << endl;
  int subIndex = 0;
  while (fd == -1) {
    fullName = _conf -> _blkDir + '/' + "subdir" + to_string(subIndex) + '/' + fileName;
    cout << "the current fullName is: " << fullName << endl;
    fd = open(fullName.c_str(), O_RDONLY);
    if (fd != -1) {
       cout << "Find the file in the dir: " << fullName << endl;
    }
    subIndex ++;
    if (subIndex > 10) {
       cout << "Can not find the file in given dir!" << endl;
       break;      
    }    
  }

  int subId = 0, i, subBase = 0, pktId;
  int readLen, readl, base = _conf->_packetSkipSize;

  for (i = 0; i < _packetCnt; i ++) {
    //_diskPkts[i] = (char*) malloc(_packetSize * sizeof(char));
    readLen = 0;
    int dr = 0;
    while (readLen < _packetSize) {
      if ((readl = pread(fd, 
              _diskPkts[i] + readLen, 
              _packetSize - readLen, 
              base + readLen)) < 0) {
        if (dr < 10) {
	  cerr << "ERROR During disk read " << dr << endl;
	  dr++;
	}
      } else {
        readLen += readl;
      }
    }
    if (DR_WORKER_DEBUG && (i == 0 || i == _packetCnt - 1 || i % (_packetCnt / 10) == 0)) {
      printf("%s: packet read %d, [%x]\n", __func__, i, *(int*)(_diskPkts[i]));
    } 
    RSUtil::multiply(_diskPkts[i], _coefficient, _packetSize);
    if (DR_WORKER_DEBUG && (i == 0 || i == _packetCnt - 1 || i % (_packetCnt / 10) == 0)) {
      printf("%s: packet read after multiply %d, [%x]\n", __func__, i, *(int*)(_diskPkts[i]));
    } 

    // notify through conditional variable
    unique_lock<mutex> lck(_diskMtx[i]);
    _diskFlag[i] = true;
    _diskCv.notify_one();
    //lck.unlock();
    base += _packetSize;
    if (DR_WORKER_DEBUG && (i == 0 || i == _packetCnt - 1 || i % (_packetCnt / 10) == 0)) 
      cout << "PipeDRWorker::readWorker() read packet " << i << endl;
  }
  close(fd);
}

void PipeDRWorker::pullRequestorCompletion(
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

  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    // read from disk
    while (!_diskFlag[pktId]) {
      unique_lock<mutex> lck(_diskMtx[pktId]);
      _diskCv.wait(lck);
    }
    // recv and compute
    if (_id != 0) {
      redisGetReply(prevCtx, (void**)&rReply);
      if (DR_WORKER_DEBUG && (pktId == 0 || pktId == _packetCnt - 1 || pktId % (_packetCnt / 10) == 0)) {
	printf("%s: packet pull %d, [%x]\n", __func__, pktId, *(int*)(rReply -> element[1] -> str));
      } 
      Computation::XORBuffers(_diskPkts[pktId], 
          rReply -> element[1] -> str,
          _packetSize);
      if (DR_WORKER_DEBUG && (pktId == 0 || pktId == _packetCnt - 1 || pktId % (_packetCnt / 10) == 0)) {
	printf("%s: packet pull after XOR %d, [%x]\n", __func__, pktId, *(int*)(_diskPkts[pktId]));
      } 
    }
    _waitingToSend ++;
    unique_lock<mutex> lck(_mainSenderMtx);
    _mainSenderCondVar.notify_one();
    if (_id != 0) freeReplyObject(rReply);
  }
}


void PipeDRWorker::doProcess() {
  string lostBlkName, localBlkName;
  int subId, i, subBase, pktId, ecK;
  unsigned int prevIP, nextIP, requestorIP;
  redisContext *nextCtx, *requestorCtx;
  bool reqHolder, isRequestor;
  unsigned int lostBlkNameLen, localBlkNameLen;
  const char* cmd;
  redisReply* rReply;
  timeval tv1, tv2;
  thread sendThread;

  while (true) {
    // loop FOREVER
    rReply = (redisReply*)redisCommand(_selfCtx, "blpop dr_cmds 100");
    gettimeofday(&tv1, NULL);

    if (rReply -> type == REDIS_REPLY_NIL) {
      if (DR_WORKER_DEBUG) cout << "PipeDRWorker::doProcess(): empty list" << endl;
      freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      if (DR_WORKER_DEBUG) cout << "PipeDRWorker::doProcess(): error happens" << endl;
      freeReplyObject(rReply);
    } else {
      /** 
       * Parsing Cmd
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

      if (DR_WORKER_DEBUG) {
        cout << " lostBlkName: " << lostBlkName << endl
          << " localBlkName: " << localBlkName << endl
          << " id: " << _id  << endl
          << " ecK: " << _ecK << endl
          << " coefficient: " << _coefficient << endl
          << " requestorIP: " << ip2Str(requestorIP) << endl
          << " prevIP: " << ip2Str(prevIP) << endl
          << " nextIP: " << ip2Str(nextIP) << endl;
      }

      // start thread reading from disks
      thread diskThread([=]{readWorker(localBlkName);});

      _waitingToSend = 0;
      //redisContext* nC;
      string sendKey;

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
      //if (DR_WORKER_DEBUG)
      cout << "request starts at " << tv1.tv_sec << "." << tv1.tv_usec
        << "ends at " << tv2.tv_sec << "." << tv2.tv_usec << endl;
      cout << "id: " << _id << endl;
      cout << "next ip: " << ip2Str(nextIP) << endl;
      cout << "send key: " << sendKey << endl;
      // lazy clean up
      freeReplyObject(rReply);
      cleanup();
    }
  }
}

