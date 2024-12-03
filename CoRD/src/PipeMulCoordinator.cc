#include "PipeMulCoordinator.hh"

void PipeMulCoordinator::requestHandler() {
  struct timeval timeout = {1, 500000}; 
  redisContext* rContext = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
  if (rContext == NULL || rContext -> err) {
    if (rContext) {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } else {
      cerr << "Connection error: can't allocate redis context" << endl;
      redisFree(rContext);
    }
    return;
  }

  redisReply* rReply;
  unsigned int requestorIP;
  char* reqStr;
  int reqLen;
  size_t requestedFileNameLen, localFileBase, localFileNameLen;
  vector<pair<string, pair<char*, size_t>>> cmds;
  vector<pair<string, int>> lostFiles;

  struct timeval tv1, tv2;
  char* drCmdHistory;
  int drCmdHistoryLen;

  struct timeval tv_start;
  gettimeofday(&tv_start, NULL);

  map<string, unsigned int> lastHelperStore; // filename -> ip

  while (true) {
    if (COORDINATOR_DEBUG) cout << "waiting for requests ..." << endl;
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
      printf("LOG: %s dr_requests RECEIVED, length = %d, time = %s\n", __func__, (int)(rReply -> element[1] -> len), getTime(tv_start, tv1).c_str());
      reqStr = rReply -> element[1] -> str;
      reqLen = rReply -> element[1] -> len;
      memcpy((char*)&requestorIP, reqStr, 4);
      string filename(reqStr + 4);
      
      int index = parseFRRequest(reqStr, reqLen, requestorIP, filename, lostFiles);

      // For HDFS3
      if (index) {
        if (_conf -> _fileSysType == "HDFS3") {
          filename = lostFiles[index].first;
          if (_conf -> _DRPolicy == "ecpipe" && _conf -> _ECPipePolicy.find("cyclic") != string::npos) {
            if (COORDINATOR_DEBUG) printf("%s: cyclic reply to %s\n", __func__, filename.c_str());
            rReply = (redisReply *)redisCommand(_selfCtx, "rpush %s %b", filename.c_str(), drCmdHistory, drCmdHistoryLen);
          }
          else {
            if (COORDINATOR_DEBUG) printf("%s: dummy reply to %s\n", __func__, filename.c_str());
            if (!lastHelperStore.count(filename) || lastHelperStore[filename] == 0) {
              printf("ERROR: No %s in lastHelperStore\n", filename.c_str());
              exit(1);
            }

            rReply = (redisReply *)redisCommand(_selfCtx, "rpush %s %b", filename.c_str(), &(lastHelperStore[filename]), 4);
            lastHelperStore[filename] = 0;
          }

        } else 
        printf("%s: warning: not HDFS3. No response.\n", __func__);
        continue;
      }

      filename = lostFiles[0].first;
      if (COORDINATOR_DEBUG) printf("%s: filename = %s\n", __func__, filename.c_str());

      _status[filename] = 0;
      freeReplyObject(rReply);
      requestedFileNameLen = filename.length();

      if (COORDINATOR_DEBUG) cout << "request recv'd: ip: " << requestorIP
        << " requested file name: " << filename << endl;

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

     
      // first and second.first may not be paired!
      map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> stripes = 
  (PIPE_MUL_OPT)
        ? _metadataBase -> getStripeBlksMul(filename, requestorIP, &_status, 0) // PIPE_MUL_OPT)
        : _metadataBase -> getStripeBlksMul(filename, requestorIP);

      for (auto& it : stripes) if (it.second.first.size() == 0) {
        printf("%s: ERROR: empty file in stripes, IP .%d\n", __func__, it.first >> 24);
        exit(1);
      }

      map<unsigned int, int> numOfJobs;

      for (auto& it : lostFiles) {
        for (auto it2 : stripes) {
          if (it2.second.first == it.first) // the same file name
          {
            lastHelperStore[it.first] = it2.second.second.back().first;
            break;
          }
        }
      }
      /*
        for (auto& it : lostFiles)
        lastHelperStore[it.first] = (stripes[requestorIP].second.back().first);
        */

      int flag = outputStripeMul(stripes, numOfJobs);
      
      map<unsigned int, int> indOfJobs = map<unsigned int, int>(numOfJobs.begin(), numOfJobs.end());
      int* ips, ecK = _conf -> _ecK;
      
      ips = (int*)calloc(_conf -> _ecK, sizeof(int));

      if (flag == 0) {
        cout << __func__ << ": repair fails, not enough helper\n";
        continue;
      }
      else if (COORDINATOR_DEBUG) cout << __func__ << ": repair starts\n";


/*
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
      } */

      for (auto it : stripes) {
        if (COORDINATOR_DEBUG) cout << "Starts for: ." << (it.first >> 24) << ", " << it.second.first << "\n";

        vector<pair<unsigned int, string>>& stripe = it.second.second;

        for (auto& it : _status) if (it.second) it.second = 1;
        for (int i = 0; i < (int)stripe.size(); i++) {
          _status[stripe[i].second] = 2;
        }

        if (COORDINATOR_DEBUG) {
          cout << __func__ << " _status: ";
            for(auto& it : _status) {
              cout << "(" << it.first << ", " << it.second << ") ";
            }
          cout << endl;
        }

        unsigned int failedIP = it.first;
        string lostFilename(it.second.first);
        unsigned int lostFilelength = lostFilename.length();

        if (COORDINATOR_DEBUG) 
          printf("failedIP = .%d, lostFilename = %s, lostFilelength = %d\n", failedIP >> 24, lostFilename.c_str(), lostFilelength);

        map<string, int> coef = 
            _metadataBase -> getCoefficient(it.second.first, &_status);

        bool first = true;
        if (COORDINATOR_DEBUG) {
          cout << "coef:\n";
          for (auto& it : coef) printf("(%s,%d) ", it.first.c_str(), it.second);
          cout << "\n";
        }

        int rHolder = isRequestorHolder(stripe, it.first), ecK;

        if (COORDINATOR_DEBUG) cout << "rHolder: " << rHolder << endl;

        char* drCmd;
        //pair<unsigned int, pair<char*, size_t>> cmdStruct;
        
        /*
        if (rHolder!= -1) { // TODO
          drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

          // pack idx, begin
          int idx = searchCtx(_ip2Ctx, requestorIP, 0, _slaveCnt - 1);
          memcpy(drCmd, (char*)&idx, 4);
          drCmd = drCmd + 4;
          // pack idx, end

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
          // restore drCmd pointer, begin
          drCmd = drCmd - 4;
          // restore drCmd pointer, end 

          cmds.push_back({_ip2Ctx[idx].second.first, 
              {drCmd, localFileBase + localFileNameLen + 8}});
          ecK = (stripe.size() | 0x0100);
        } else {
          ecK = stripe.size();
        }
        */
        ecK = stripe.size();

        // commands for helpers
        for (int i = 0; i < (ecK & 0xff); i ++) {
          drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

          /* pack idx, begin */
          int idx = searchCtx(_ip2Ctx, stripe[i].first, 0, _slaveCnt - 1);
          memcpy(drCmd, (char*)&idx, 4);
          drCmd = drCmd + 4;
          /* pack idx, end */

          memcpy(drCmd, (char*)&ecK, 4);
                memcpy(drCmd + 4, (char*)&requestorIP, 4);
          memcpy(drCmd + 8, (char*)&(i == 0 ? stripe[ecK - 1].first : stripe[i - 1].first), 4);
          memcpy(drCmd + 12, (char*)&(i == (ecK & 0xff) - 1 ? stripe[0].first : stripe[i + 1].first), 4);
          memcpy(drCmd + 16, (char*)&i, 4);
          unsigned int coefficient = coef[stripe[i].second];
          coefficient = ((coefficient << 16) | lostFilelength);
          memcpy(drCmd + 20, (char*)&coefficient, 4);
          memcpy(drCmd + 24, lostFilename.c_str(), lostFilelength);
          localFileNameLen = stripe[i].second.length();
          memcpy(drCmd + 24 + lostFilelength, (char*)&localFileNameLen, 4);
          memcpy(drCmd + 24 + lostFilelength + 4, stripe[i].second.c_str(), localFileNameLen);
          memcpy(drCmd + 24 + lostFilelength + 4 + localFileNameLen, (char*)&(indOfJobs[stripe[i].first]), 4);
                indOfJobs[stripe[i].first]--;
          memcpy(drCmd + 24 + lostFilelength + 4 + localFileNameLen + 4, (char*)&(numOfJobs[stripe[i].first]), 4);

          if (COORDINATOR_DEBUG) {
            printf("cmd to .%d: \n", stripe[i].first >> 24);
            printf("  0: ecK = %d\n", ecK);
            printf("  4: requestorIP/failedIP = %d\n", (_conf -> _fileSysType == "HDFS3") ? (requestorIP >> 24) : (failedIP >> 24));
            printf("  8: last IP = %d\n", (i == 0 ? stripe[ecK - 1].first : stripe[i - 1].first) >> 24);
            printf("  12: next IP = %d\n", (i == (ecK & 0xff) - 1 ? stripe[0].first : stripe[i + 1].first) >> 24);
            printf("  16: id = %d\n", i);
            printf("  20: coef = 0x%x\n", coefficient);
            printf("  24: %s\n", lostFilename.c_str());
            printf("  24+l, 28+l: %d, %s\n", (int)localFileNameLen, stripe[i].second.c_str());
            printf("  28+l+l2: (indOfJobs = %d)\n", indOfJobs[stripe[i].first]+1);
            printf("  32+l+l2: (numOfJobs = %d)\n", numOfJobs[stripe[i].first]);
          }
          

          /* restore drCmd pointer, begin */
          drCmd = drCmd - 4;
          /* restore drCmd pointer, end */

          cmds.push_back({_ip2Ctx[idx].second.first, 
              {drCmd, 24 + lostFilelength + localFileNameLen + 16}});
          if (COORDINATOR_DEBUG) {
            cout << "rHolder: " << rHolder << endl;
            printf("%s: cmds.push_back %d\n", __func__, idx);
          }

        }
        if (COORDINATOR_DEBUG) puts("normal commands finished");
        
        // passive requests
        if (_conf -> _fileSysType == "HDFS") {
          if (failedIP != requestorIP && lostFiles.size() > 1) { // to requestors
            if (COORDINATOR_DEBUG) puts("special commands");

            redisReply* rReply;
            drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

            int idx = searchCtx(_ip2Ctx, failedIP, 0, _slaveCnt - 1);

            /* 
             * Cmd format of "dr_passive":
             * [a(4Byte)][b0(4)][b(4Byte)][c(n Byte)]
             * a: prev ip. The ip where passiveReceiver receives data.
             * b0: req ip. The ip where requestor is.
             * b: lostFile length start pos 4
             * c: lostFile name
             * */

            int cmdbase = 0;
            /*
            if (_conf -> _ECPipePolicy == "cyclic") {
              cmdbase = stripe.size() - 1; 
              memcpy(drCmd, (char*)&(cmdbase), 4);
              cmdbase = 4;
              for (int i = 0; i < stripe.size() - 1; i ++) {
                memcpy(drCmd + cmdbase, (char*)&(stripe[i].first), 4);
                cmdbase += 4;
              }
            }
            else {*/
              memcpy(drCmd, (char*)&(requestorIP), 4);          // prevIP, or the final IP
              cmdbase += 4;
            //}
            
            memcpy(drCmd + cmdbase, (char*)&requestorIP, 4);          // requestorIP
            memcpy(drCmd + cmdbase + 4, (char*)&lostFilelength, 4);
            memcpy(drCmd + cmdbase + 8, lostFilename.c_str(), lostFilelength);

            rReply = (redisReply*)redisCommand(_ip2Ctx[idx].second.second, "rpush dr_passive %b", 
                drCmd, cmdbase + lostFilelength + 8);

            freeReplyObject(rReply);
    
            
            if (COORDINATOR_DEBUG) printf("%s: special: %d, ip = .%d, cmd length = %d\n", __func__, idx, (failedIP >> 24), cmdbase + lostFilelength + 8);
          }

        }
        if (COORDINATOR_DEBUG) puts("special commands finished");
      }

      // pipeline commands
      redisAppendCommand(rContext, "MULTI");
      for (auto& it : cmds) {
        redisAppendCommand(rContext, "rpush %s %b", 
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

      int nj = lostFiles.size();

      if (_conf -> _ECPipePolicy == "basic") {
        // Return cmd to EIS: 
        // lastHelper-0, lastHelper-1, requstorIP-1, lastHelper-2, requestorIP-2, ...
        // For HDFS3, only the first 4 bytes are useful 
        char* drCmd = (char*)malloc(sizeof(int) * (lostFiles.size() * 2 - 1));
        memcpy(drCmd, &(lastHelperStore[lostFiles[0].first]), 4);
        for (int i = 1; i < nj; i++) {
          memcpy(drCmd + i * 8 - 4, &(lastHelperStore[lostFiles[i].first]), 4);
          memcpy(drCmd + i * 8,     &(lostFiles[i].second), 4);
        }
        rReply = (redisReply*)redisCommand(_selfCtx, "rpush %s %b", filename.c_str(), drCmd, nj * 8 - 4);
      } else {
        drCmdHistoryLen = sizeof(int) * (_conf -> _ecK + 1) * lostFiles.size();
        drCmdHistory = (char*)malloc(drCmdHistoryLen);
        setupCyclReply(lostFiles, stripes, &drCmdHistory, &drCmdHistoryLen);
        
        redisAppendCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), drCmdHistory, drCmdHistoryLen);
        redisGetReply(_selfCtx, (void**)&rReply);
      }

      freeReplyObject(rReply);
      gettimeofday(&tv2, NULL);
      printf("LOG: %s::%s() requests takes totally %.6lf s\n", typeid(this).name(), __func__, ((tv2.tv_sec - tv1.tv_sec) * 1000000.0 + tv2.tv_usec - tv1.tv_usec) / 1000000.0);
      
    }
  }
  // should never end ...
}

void PipeMulCoordinator::doProcess() {
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

PipeMulCoordinator::PipeMulCoordinator(Config* conf) : Coordinator(conf) {
  _status.clear();
}
