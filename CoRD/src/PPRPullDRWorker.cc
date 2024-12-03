#include "PPRPullDRWorker.hh"

unsigned int PPRPullDRWorker::PPRnextIP(int id, unsigned int ecK) const {
  id ++;
  return id + (id & (- id)) - 1;
}

vector<unsigned int> PPRPullDRWorker::getChildrenIndices(int idx, unsigned int  ecK) {
  vector<unsigned int> retVal;
  idx ++;
  int lastOne = 0;
  while ((idx >> lastOne) % 2 == 0) lastOne ++;

  int id = ((idx >> lastOne) - 1 << lastOne);
  for (-- lastOne; lastOne >= 0; lastOne --) {
    id += (1 << lastOne);
    if (id <= ecK) retVal.push_back(id - 1); 
    else {
      vector<unsigned int> temp = getChildrenIndices(id - 1, ecK);
      retVal.insert(retVal.end(), temp.begin(), temp.end());
    }
  }
  //reverse(retVal.begin(), retVal.end());
  return retVal;
}

unsigned int PPRPullDRWorker::getID(int k) const {
  k ++;
  while ((k & (k - 1)) != 0) k += (k & (-k));
  return -- k;
}

void PPRPullDRWorker::doProcess() {
  struct timeval tv1, tv2, tv3;
  string lostBlkName, localBlkName;
  int idInStripe, start, pos, *subOrder;
  int subId, i, subBase, pktId, ecK;
  unsigned int prevIP, nextIP, requestorIP;
  const char* cmd;
  redisContext *nextCtx, *requestorCtx;
  bool reqHolder, isRequestor;
  unsigned int lostBlkNameLen, localBlkNameLen;

  while (true) {
    // loop FOREVER
    redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop dr_cmds 100");
    if (rReply -> type == REDIS_REPLY_NIL) {
      if (DR_WORKER_DEBUG) cout << "PPRPullDRWorker::doProcess(): empty list" << endl;
      freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      if (DR_WORKER_DEBUG) cout << "PPRPullDRWorker::doProcess(): error happens" << endl;
      freeReplyObject(rReply);
    } else {
      printf("command received\n");
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
      memcpy((char*)&_id, cmd + 4, 4);
      unsigned int holderIps[_ecK + 1];
      unsigned int* holderIpsPtr = holderIps;

      memcpy((char*)holderIps, cmd + 8, 4 * (_ecK + 1));

      // get file names
      memcpy((char*)&lostBlkNameLen, cmd + 8 + 4 * (_ecK + 1), 4);
      _coefficient = (lostBlkNameLen >> 16);
      lostBlkNameLen = (lostBlkNameLen & 0xffff);
      lostBlkName = string(cmd + 12 + 4 * (_ecK + 1), lostBlkNameLen);
      if (_ecK != _id) {
        memcpy((char*)&localBlkNameLen, cmd + 12 + 4 * (_ecK + 1) + lostBlkNameLen, 4);
        localBlkName = string(cmd + 16 + 4 * (_ecK + 1) + lostBlkNameLen, localBlkNameLen);
      }
      else localBlkName = "";
      freeReplyObject(rReply);

      if (DR_WORKER_DEBUG) {
        cout << "lostBlkName: " << lostBlkName << endl
          << " localBlkName: " << localBlkName << endl
          << " id: " << _id  << endl
          << " ecK: " << _ecK << endl;
        for (int j = 0; j < _ecK; j ++) cout << " ip of node " << j << " is " << ip2Str(holderIps[j]) << endl;
      }

      _waitingToSend = 0;
      _waitingToPull = 0;

      thread diskThread([=]{readWorker(localBlkName);});
      thread pullThread([=]{puller(localBlkName, holderIpsPtr, lostBlkName);});

      redisContext* wrtCtx = PPRnextIP(_id, _ecK) >= _ecK ? findCtx(holderIps[_ecK]) : _selfCtx;

      gettimeofday(&tv1, NULL);
      gettimeofday(&tv3, NULL);

      for (int i = 0; i < _packetCnt; i ++) {
        while (i >= _waitingToSend) {
          unique_lock<mutex> lck(_toSenderMtx);
          _toSenderCV.wait(lck);
        }

        if (_id == _ecK) {
          rReply = (redisReply*)redisCommand(wrtCtx,
              "RPUSH %s %b",
              lostBlkName.c_str(), _diskPkts[i], _packetSize);
        } else {
          rReply = (redisReply*)redisCommand(wrtCtx,
              "RPUSH tmp:%s:%d %b",
              lostBlkName.c_str(), _id, _diskPkts[i], _packetSize);
        }
        freeReplyObject(rReply);
        gettimeofday(&tv2, NULL);
        if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) {
          cout << "write pkt " << i << endl;
        }
      }

      diskThread.join();
      pullThread.join();
      // lazy garbage collection
      cleanup();
    }
  }
}

void PPRPullDRWorker::readWorker(const string& fileName) {
  if (fileName == "") {
    for (int i = 0; i < _packetCnt; i ++) {
      //_diskPkts[i] = (char*)calloc(_packetSize, sizeof(char));

      _waitingToPull++;
      unique_lock<mutex> lck(_toPullerMtx);
      _toPullerCV.notify_all();

      if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) cout << "PPRDRWorker::readWorker() processing pkt " << i << endl;
    }
    return;
  }

  string fullName = _conf -> _blkDir + '/' + fileName;
  int fd = open(fullName.c_str(), O_RDONLY);
  int subId = 0, subBase = 0, pktId;
  size_t readLen, readl, base = _conf->_packetSkipSize;

  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    readPacketFromDisk(fd, _diskPkts[pktId], _packetSize, pktId * _packetSize);

    RSUtil::multiply(_diskPkts[pktId], _coefficient, _packetSize);

    if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) cout << "PPRDRWorker::readWorker() processing pkt " << pktId << endl;

    // notify through conditional variable
    //_diskFlag[pktId] = true;
    _waitingToPull++;
    unique_lock<mutex> lck(_toPullerMtx);
    _toPullerCV.notify_all();
  }
  close(fd);
}


void PPRPullDRWorker::puller(const string& fileName, unsigned int* holderIps, const string& lostBlkName) {

  struct timeval tv1, tv2, tv3;
  gettimeofday(&tv1, NULL);
  gettimeofday(&tv3, NULL);

  vector<pair<unsigned int, redisContext*>> ctxes;
  
  vector<unsigned int> children;

  redisReply* rReply;

  if (_id < _ecK) {
    children = getChildrenIndices(_id, _ecK);
  } else {
    children = getChildrenIndices(getID(_id), _ecK);
  }

  if (DR_WORKER_DEBUG) {
    printf("children: \n");
    for (auto it : children) 
      cout << it << " " << ip2Str(holderIps[it]) << endl;
    printf("children - end\n");
  }

  for (auto it : children) {
    if (_id != _ecK) ctxes.push_back({it, findCtx(holderIps[it])});
    else ctxes.push_back({it, initCtx(_conf -> _localIP)});
  }


  if (ctxes.size() > 0) {
    thread readThreads[ctxes.size()];
    _indicesWaiting = (int*)calloc(sizeof(int), ctxes.size());

    for (int i = 0; i < ctxes.size(); i++) {
      readThreads[i] = thread([=]{readThread(lostBlkName, ctxes[i], i, (int)ctxes.size(), (int)(fileName.length() == 0));});
    }

    for (int i = 0; i < ctxes.size(); i++) readThreads[i].join();
  }
  else {
    for (int i = 0; i < _packetCnt; i++) {
      while (i >= _waitingToPull) {
        unique_lock<mutex> lck(_toPullerMtx);
        _toPullerCV.wait(lck);
      }
      _waitingToSend++;
      unique_lock<mutex> lck(_toSenderMtx);
      _toSenderCV.notify_one();
    }
  }
  /*
  for (int i = 0; i < _packetCnt; i++) {
    printf("LOG: puller: i=%d\n",i);
    while (!_diskFlag[i]) {
      unique_lock<mutex> lck(_diskMtx[i]);
      _diskCv.wait(lck);
    }
    for (auto it : ctxes) {
      if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) cout << "fetching from children " << it.first << endl;
      rReply = (redisReply*)redisCommand(it.second,
          "BLPOP tmp:%s:%d 10000",
          lostBlkName.c_str(), it.first);
      
      gettimeofday(&tv2, NULL);
      printf("LOG: %s: pkt %d, time %s, from start %s, from .%d\n", __func__, i, 
          getTime(tv1, tv2).c_str(), getTime(tv3, tv2).c_str(), holderIps[it.first] >> 24); 
      gettimeofday(&tv1, NULL);

      Computation::XORBuffers(_diskPkts[i], 
          rReply -> element[1] -> str, _packetSize);
      freeReplyObject(rReply);
    }

    _waitingToSend++;
    unique_lock<mutex> lck(_toSenderMtx);
    _toSenderCV.notify_one();
  }*/
}

void PPRPullDRWorker::readThread(const string& lostBlkName, const pair<unsigned int, redisContext*>& ctxPair, int num, int totNum, int isRequestor) { 
  redisReply* rReply;
  int minNum;
  bool flag = false;

  struct timeval tv1, tv2, tv3;
  gettimeofday(&tv1, NULL);

  for (int i = 0; i < _packetCnt; i++) {
    while (i >= _waitingToPull) {
      unique_lock<mutex> lck(_toPullerMtx);
      _toPullerCV.wait(lck);
    }
  
    rReply = (redisReply*)redisCommand(ctxPair.second, "blpop tmp:%s:%d 10000",
        lostBlkName.c_str(), ctxPair.first);

    _readThreadMtx.lock();

    gettimeofday(&tv2, NULL);
    Computation::XORBuffers(_diskPkts[i], 
        rReply -> element[1] -> str, _packetSize);

    _indicesWaiting[num]++;
    minNum = _packetCnt;
    for (int j = 0; j < totNum; j++) {
      if (minNum > _indicesWaiting[j]) minNum = _indicesWaiting[j];
    }

    if (_waitingToSend < minNum) {
      _waitingToSend = minNum;

      unique_lock<mutex> lck(_toSenderMtx);
      _toSenderCV.notify_one();
    }

    _readThreadMtx.unlock();

    freeReplyObject(rReply);
  }

}
