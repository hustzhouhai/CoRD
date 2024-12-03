#include "PipeCoordinator.hh"

void PipeCoordinator::requestHandler() {
  if (COORDINATOR_DEBUG) printf("%s::%s() starts\n", typeid(this).name(), __func__);
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
  if (rContext == NULL || rContext -> err) {
    if (rContext) {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } else {
      cerr << "Connection error: can't redis context" << endl;
      redisFree(rContext);
    }
    return;
  }

  redisReply* rReply;
  unsigned int requestorIP;
  char* reqStr;
  int reqLen;
  string filename;
  size_t requestedFileNameLen, localFileBase, localFileNameLen;
  vector<pair<string, pair<char*, size_t>>> cmds;

  vector<pair<string, int>> lostFiles;

  struct timeval tv1, tv2;

  while (true) {
    cout << "waiting for requests ..." << endl;
    /* Redis command: BLPOP (LISTNAME1) [LISTNAME2 ...] TIMEOUT */
    rReply = (redisReply*)redisCommand(rContext, "blpop dr_requests 100");
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "PipeCoordinator::requestHandler() empty queue " << endl;
      freeReplyObject(rReply);
      continue;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "PipeCoordinator::requestHandler() ERROR happens " << endl;
      freeReplyObject(rReply);
      continue;
    } else {
      gettimeofday(&tv1, NULL);

      reqStr = rReply -> element[1] -> str;
      reqLen = rReply -> element[1] -> len;

      for (int i=0; i<reqLen; i++) {
	printf("%d ", reqStr[i]);
      }
      printf("\n");

      if (reqLen < 4) {
	if (COORDINATOR_DEBUG) cout << "command format error" << endl;
	exit(1);
      }
      
      memcpy((char*)&requestorIP, reqStr, 4);
      printf("%s: requestor IP = %d\n", __func__, requestorIP);

      for (int i=0; i<reqLen; i++) {
	printf("%d ", reqStr[i]);
      }
      printf("\n");
      int index = parseFRRequest(reqStr, reqLen, requestorIP, filename, lostFiles);
      requestedFileNameLen = filename.length();

      freeReplyObject(rReply);

      // For HDFS3
      if (index) {
	if (COORDINATOR_DEBUG) printf("index = %d\n", index);
	/*
	if (_conf -> _fileSysType == "HDFS3") {
	    filename = lostFiles[index].first;
	    if (COORDINATOR_DEBUG) printf("%s: dummy reply to %s\n", __func__, filename.c_str());
	    rReply = (redisReply *)redisCommand(_selfCtx, "rpush %s %b", filename.c_str(), &(lastHelpers[index]), 4);
	} else 
	printf("%s: warning: not HDFS3. No response.\n", __func__);
	*/
	continue;
      }

      if (COORDINATOR_DEBUG) cout << "request recv'd: ip: " << requestorIP
        << "requested file name: " << filename << endl;

      // do process
      /**
       * Cmd format: 
       * [idx(4Byte)][a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)]
       * idx: idx in _ip2Ctx pos 0 // this will not be sent to ECHelper
       * a: ecK pos: 0 // if ((ecK & 0xff00) >> 8) == 1), requestor is a holder
       * b: requestor ip start pos: 4
       * c: prev ip start pos 8
       * d: next ip start pos 12
       * e: id pos 16
       * f: lost file name (4Byte lenght + length) start pos 20, 24
       * g: corresponding filename in local start pos ?, ? + 4
       */

      // multi failure detection
      vector<pair<unsigned int, string>> stripe = 
        _metadataBase -> getStripeBlks(filename, requestorIP);

      if (stripe.size() > _conf -> _ecK) stripe.resize(_conf -> _ecK); // TODO I don't know why

      if (COORDINATOR_DEBUG) {
	for(int i=0; i<(int)stripe.size(); i++) {
	  cout << __func__ << " stripe: " << (stripe[i].first >> 24) << ", " << stripe[i].second << endl;
	}
      }

      if (stripe.empty() || stripe.size() < _conf -> _ecK) {
	if (COORDINATOR_DEBUG) cout << __func__ << ": repair fails, not enough helper\n";
	continue;
      }
      if (COORDINATOR_DEBUG) cout << __func__ << ": repair starts\n";

      if (_conf -> _pathSelectionEnabled) {
          cout << "checkpoint 1" << endl;
        vector<int> IPids;
        vector<pair<unsigned int, string>> temp;
        IPids.push_back(_ip2idx[requestorIP]);
        for (int i = 0; i < _ecK; i ++) {
          IPids.push_back(_ip2idx[stripe[i].first]);
        }
        for (auto it : IPids) cout << it << " ";
        cout << endl;
          cout << "checkpoint 2" << endl;

        // get the matrix
        vector<vector<double>> lWeightMat = vector<vector<double>>(_ecK + 1, vector<double>(_ecK + 1, 0));
        for (int i = 0; i < _ecK + 1; i ++) {
          for (int j = 0; j < _ecK + 1; j ++) {
            lWeightMat[i][j] = _linkWeight[IPids[i]][IPids[j]];
          }
        }

          cout << "checkpoint 3" << endl;
          cout << "lWeightMat: " << endl;
        for (int i = 0; i < _ecK + 1; i ++) {
          for (int j = 0; j < _ecK + 1; j ++) {
            cout << lWeightMat[i][j] << " ";
          }
          cout << endl;
        }

        _pathSelection -> intelligentSearch(lWeightMat, _ecK);
        vector<int> selectedPath = _pathSelection -> getPath();

        cout << "checkpoint 4 selected path:" << endl;
        for (auto it : selectedPath) cout << it << " ";
        cout << endl;

        for (int i = 0; i < _ecK; i ++) temp.push_back(stripe[selectedPath[_ecK - i] - 1]);
        swap(temp, stripe);

        cout << "checkpoint 5 blocks in order:" << endl;
        for (auto it : stripe) cout << it.first << " " << it.second << endl;
      }

      for (auto& it : _status) it.second = 1; _status[filename] = 0;
      for (auto& it : stripe) _status[it.second] = 2;
      map<string, int> coef = _metadataBase -> getCoefficient(filename, &_status);

      if (COORDINATOR_DEBUG) {
	for (auto it : coef) {
	  cout << __func__ << " coef, with status: " << it.first << ", " << it.second << endl;
	}
      }

      int rHolder = isRequestorHolder(stripe, requestorIP), ecK;

      if (COORDINATOR_DEBUG) cout << "rHolder: " << rHolder << endl;

      char* drCmd;
      //pair<unsigned int, pair<char*, size_t>> cmdStruct;
      if (rHolder!= -1) {
        drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

        /* pack idx, begin */
        int idx = searchCtx(_ip2Ctx, requestorIP, 0, _slaveCnt - 1);
        memcpy(drCmd, (char*)&idx, 4);
        drCmd = drCmd + 4;
        /* pack idx, end */

        ecK = 0x010000;
        memcpy(drCmd, (char*)&ecK, 4);
        //cout << "ecK: " << ecK << endl;
        //cout << "first four bytes: ";
        //for (int i = 0; i < 4; i ++) cout << (int)drCmd[i] << " ";
        memcpy(drCmd + 4, (char*)&requestorIP, 4);
        memcpy(drCmd + 16, (char*)&ecK, 4);
        //cout << "requestorIP: " << requestorIP << endl;
        unsigned int coefficient = coef[stripe[rHolder].second];
        coefficient = ((coefficient << 16) | requestedFileNameLen);
        memcpy(drCmd + 20, (char*)&coefficient, 4);
        //memcpy(drCmd + 20, (char*)&requestedFileNameLen, 4);
        //cout << "requestedFileNameLen: " << requestedFileNameLen << endl;
        memcpy(drCmd + 24, filename.c_str(), requestedFileNameLen);
        localFileBase = 24 + requestedFileNameLen;
        localFileNameLen = stripe[rHolder].second.length();
        memcpy(drCmd + localFileBase, (char*)&localFileNameLen, 4);
        memcpy(drCmd + localFileBase + 4, stripe[rHolder].second.c_str(), localFileNameLen);


        stripe.erase(stripe.begin() + rHolder);
        memcpy(drCmd + 8, (char*)&requestorIP, 4);
        /* restore drCmd pointer, begin */
        drCmd = drCmd - 4;
        /* restore drCmd pointer, end */

        cmds.push_back({_ip2Ctx[idx].second.first, 
            {drCmd, localFileBase + localFileNameLen + 8}});
        ecK = (stripe.size() | 0x0100);
      } else {
        ecK = stripe.size();
      }


      for (int i = 0; i < (ecK & 0xff); i ++) {
        if (COORDINATOR_DEBUG) cout << "i: " << i << "rHolder: " 
          << rHolder << " ecK: " << ecK << endl;
        drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

        /* pack idx, begin */
        int idx = searchCtx(_ip2Ctx, stripe[i].first, 0, _slaveCnt - 1);
        memcpy(drCmd, (char*)&idx, 4);
        drCmd = drCmd + 4;
        /* pack idx, end */
	
	//add by Zuoru: debug
	cout << " block name: " << stripe[i].second << 
		" ip: " << stripe[i].first <<
		" idx: " << idx << endl; 
	//
        memcpy(drCmd, (char*)&ecK, 4);
        memcpy(drCmd + 4, (char*)&requestorIP, 4);
        memcpy(drCmd + 8, (char*)&(i == 0 ? stripe[ecK - 1].first : stripe[i - 1].first), 4);
        memcpy(drCmd + 12, (char*)&(i == (ecK & 0xff) - 1 ? stripe[0].first : stripe[i + 1].first), 4);
        memcpy(drCmd + 16, (char*)&i, 4);
        unsigned int coefficient = coef[stripe[i].second];
        coefficient = ((coefficient << 16) | requestedFileNameLen);
        memcpy(drCmd + 20, (char*)&coefficient, 4);
        //memcpy(drCmd + 20, (char*)&requestedFileNameLen, 4);
        memcpy(drCmd + 24, filename.c_str(), requestedFileNameLen);
        localFileBase = 24 + requestedFileNameLen;
        localFileNameLen = stripe[i].second.length();
        memcpy(drCmd + localFileBase, (char*)&localFileNameLen, 4);
        memcpy(drCmd + localFileBase + 4, stripe[i].second.c_str(), localFileNameLen);

        /* restore drCmd pointer, begin */
        drCmd = drCmd - 4;
        /* restore drCmd pointer, end */

        cmds.push_back({_ip2Ctx[idx].second.first, 
            {drCmd, localFileBase + localFileNameLen + 8}});
	printf("%s: cmds.push_back %d\n", __func__, idx);
        //cmds.push_back({ip2Str(stripe[i].first), 
        //    {drCmd, localFileBase + localFileNameLen + 4}});
        if (COORDINATOR_DEBUG) cout << "rHolder: " << rHolder << endl;

      }
      puts("checkpoint2");

      // pipeline commands
      redisAppendCommand(rContext, "MULTI");
      for (auto& it : cmds) {
        redisAppendCommand(rContext, "RPUSH %s %b", 
            it.first.c_str(), 
            it.second.first, it.second.second);
      }
      redisAppendCommand(rContext, "EXEC");

      // execute commands
      redisGetReply(rContext, (void **)&rReply);
      freeReplyObject(rReply);
      for (auto& it : cmds) {
        redisGetReply(rContext, (void **)&rReply);
        freeReplyObject(rReply);
        free(it.second.first);
      }
      redisGetReply(rContext, (void **)&rReply);
      freeReplyObject(rReply);
      cmds.clear();

      if (_conf -> _ECPipePolicy.find("basic") != string::npos) {
        rReply = (redisReply*)redisCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), &(stripe.back().first), 4);
        cout << __func__ << ": send to _selfCtx [" << filename.c_str() << "] ." << (stripe.back().first >> 24) << endl;
//        freeReplyObject(rReply);
      } else {
        // only for single failure
        char* drCmd;
        int drCmdLen;
        setupCyclReply(requestorIP, stripe, &drCmd, &drCmdLen);

        redisAppendCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), drCmd, drCmdLen);
        redisGetReply(_selfCtx, (void**)&rReply);
      }

      gettimeofday(&tv2, NULL);
      string logMsg = "start at " + to_string((tv1.tv_sec * 1000000 + tv1.tv_usec) * 1.0 / 1000000);
      logMsg += " end at " + to_string((tv2.tv_sec * 1000000 + tv2.tv_usec) * 1.0 / 1000000) + "\n";
      cout << logMsg;
    }
  }
  // should never end ...
}

void PipeCoordinator::doProcess() {
  // starts the threads

  for (int i = 0; i < _distributorThreadNum; i ++) {
    _distThrds[i] = thread([=]{this -> cmdDistributor(i, _distributorThreadNum);});
  }
  for (int i = 0; i < _handlerThreadNum; i ++) {
    _handlerThrds[i] = thread([=]{this -> requestHandler();});
  }
  

  // should not reach here
  for (int i = 0; i < _distributorThreadNum; i ++) {
    _distThrds[i].join();
  }
  for (int i = 0; i < _handlerThreadNum; i ++) {
    _handlerThrds[i].join();
  }
}

PipeCoordinator::PipeCoordinator(Config* conf) : Coordinator(conf) {
}

