#include "ConvCoordinator.hh"

void ConvCoordinator::requestHandler() 
{
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
  size_t requestedFileNameLen, cmdbase, localFileNameLen;
  vector<pair<string, pair<char*, size_t>>> cmds;

  struct timeval tv1, tv2;

  vector<pair<string, int>> lostFiles;

  map<string, int> returnIPs; // lostFile -> ip

  while (true) 
  {
    if (COORDINATOR_DEBUG) cout << "waiting for requests ..." << endl;
    /* Redis command: BLPOP (LISTNAME1) [LISTNAME2 ...] TIMEOUT */
    rReply = (redisReply*)redisCommand(rContext, "blpop dr_requests 100");  // wait the lost client (ECPipeInputStream.cc) send the dr_request req
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "ConvCoordinator::requestHandler() empty queue " << endl; 
      freeReplyObject(rReply);
      continue;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "ConvCoordinator::requestHandler() ERROR happens " << endl;
      freeReplyObject(rReply);
      continue;
    } else {
      gettimeofday(&tv1, NULL);
      reqStr = rReply -> element[1] -> str;
      reqLen = rReply -> element[1] -> len;
      if (reqLen >= 4) 
	      memcpy((char*)&requestorIP, reqStr, 4);
      cout << typeid(this).name() << "::" << __func__ << "() from IP ." << (requestorIP >> 24) << " starts\n";
      // cmd  : |- locIP (4bytes) -|- 0000 -|- the length of lost block name (4bytes) -|- lost_blk_name -|
      /**
       * Old Command format:
       * |<---Requestor IP (4Byte)--->|<---Lost filename (?Byte)--->|
       * New Command format:
       * |<---Requestor IP (4Byte)--->|<---Index--->|<---Lost filename 1 (4+l_1 Byte)--->|...|<---Lost filename f (4+l_f Byte)--->|
       */
      string filename(reqStr + 4);

      int index = parseFRRequest(reqStr, reqLen, requestorIP, filename, lostFiles);
      
      if (index) 
      {
        //filename = lostFiles[index].first;
        printf("%s: dummy reply to %s, req IP = .%d\n", __func__, filename.c_str(), requestorIP >> 24);
        rReply = (redisReply *)redisCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), &requestorIP, 4);
        continue;
      }

      _status[filename] = 0;
      freeReplyObject(rReply);
      requestedFileNameLen = filename.length();

      if (COORDINATOR_DEBUG) 
        cout << typeid(this).name() << "::" << __func__ << "(): request recv'd: ip: ." << (requestorIP >> 24) << endl;

      /*
      vector<pair<unsigned int, string>> stripe = 
        _metadataBase -> getStripeBlks(filename, requestorIP); */
      map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> stripe_mul = 
        (CONV_OPT) 
        ?  _metadataBase -> getStripeBlksMul(filename, requestorIP, &_status, 0)
        :  _metadataBase -> getStripeBlksMul(filename, requestorIP);

      map<unsigned int, int> numOfJobs;

      int flag = outputStripeMul(stripe_mul, numOfJobs);
      
      map<unsigned int, int> indOfJobs = map<unsigned int, int>(numOfJobs.begin(), numOfJobs.end());
      int* ips, ecK = _conf -> _ecK;
      
      ips = (int*)calloc(_conf -> _ecK, sizeof(int));

      if (flag == 0) 
      {
        cout << __func__ << ": repair fails, not enough helper\n";
        continue;
      }
      else if (COORDINATOR_DEBUG) 
        cout << __func__ << ": repair starts\n";


      int tmp;

      // C1: send commands to helpers
      for (int i = 0; i < ecK; i ++) 
      {
        /** 
         * Cmd format: helpers
         * [a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)]
         * a: ecK pos: 0 // if ((ecK & 0xff00) >> 1) == 1), requestor is a holder
         * b: requestor ip start pos: 4
         * >=0: helper label, start pos 8
         * f: lost file name of requestor, start pos 20, 24
         * g: local file name of helper, start pos ?, ? + 4
         */
      
        vector<pair<unsigned int, string>>& stripe = stripe_mul[requestorIP].second;

        if (COORDINATOR_DEBUG) 
          cout << "i: " << i << " ecK: " << ecK << endl;
        char* drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

        /* pack idx, begin */
        int idx = searchCtx(_ip2Ctx, stripe[i].first, 0, _slaveCnt - 1);
        memcpy(drCmd, (char*)&idx, 4);
        drCmd = drCmd + 4;
        /* pack idx, end */

	      tmp = 1;

        memcpy(drCmd, (char*)&ecK, 4);
        memcpy(drCmd + 4, (char*)&requestorIP, 4);
	      memcpy(drCmd + 8, (char*)&tmp, 4);
        memcpy(drCmd + 12, (char*)&(i == ecK - 1 ? stripe[0].first : stripe[i + 1].first), 4); // useless now, for conventional
        memcpy(drCmd + 16, (char*)&i, 4);  // useless now, for conventional
        memcpy(drCmd + 20, (char*)&requestedFileNameLen, 4);
        memcpy(drCmd + 24, filename.c_str(), requestedFileNameLen);
        localFileNameLen = stripe[i].second.length();
        memcpy(drCmd + 24 + requestedFileNameLen, (char*)&localFileNameLen, 4);
        memcpy(drCmd + 24 + requestedFileNameLen + 4, stripe[i].second.c_str(), localFileNameLen);

        ips[i] = stripe[i].first;

        /* restore drCmd pointer, begin */
        drCmd = drCmd - 4;
        /* restore drCmd pointer, end */

        if (COORDINATOR_DEBUG)
          cout << "Helper: " << i << endl
            << "  0: ecK = " << ecK << endl
            << "  4: reqIP = ." << (requestorIP >> 24) << endl
            << "  8: tmp = " << tmp << endl
            << "  12: helperIP = ." << ((i == ecK - 1 ? stripe[0].first : stripe[i+1].first) >> 24) << endl
            << "  16: id = " << i << endl
            << "  20: reqFileNamelen = " << requestedFileNameLen << endl
            << "  24: filename = " << filename.c_str() << endl
            << "  24+l: localFileNameLen = " << localFileNameLen << endl
            << "  28+l: localFile = " << stripe[i].second << endl;

        // zhouhai: _ip2Ctx[idx].second.first = disQ:0
        cmds.push_back({_ip2Ctx[idx].second.first, {drCmd, 32 + requestedFileNameLen + localFileNameLen}});
      }
        
      // C2: send active recovery command
      {
        /* 
          * Cmd format of "dr_cmds" for active requestor:
          * [a(4)][b(4)][0xffffffff][m(4)][0(4)][c(4K)][d(4)][e(4)][f1(4+4k)]...[fm(4+4k)]
          * a: ecK
          * b: requestor IP, start pos 4
          * -1: requestor label, start pos 8
          * m: number of failures, start pos 12
          * c: helper ips, start pos 20
          * d, e: lost file name [l], start pos (20 + 4k), (24 + 4k)
          * f1: passive requestor IP 1, start pos (24 + 4k + l)
          *     coef1, ..., coefk, start pos (28 + 4k + l)
          * ...
          * fm: passive requestor IP m, start pos (24 + 4k + l + (m-1)(4K+4)) -> requestor
          *     ... 
          * */
        vector<pair<unsigned int, string>>& stripe = stripe_mul[requestorIP].second;

        char* drCmd;
        
        drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);
        
        /* pack idx, begin */
        int idx = searchCtx(_ip2Ctx, requestorIP, 0, _slaveCnt - 1);
	      tmp = stripe_mul.size();

        unsigned int m1 = 0xffffffff;
        memcpy(drCmd, (char*)&idx, 4);
        drCmd = drCmd + 4;
        /* pack idx, end */
        
        memcpy(drCmd, (char*)&ecK, 4);
        memcpy(drCmd + 4, (char*)&requestorIP, 4);
        memcpy(drCmd + 8, (char*)&m1, 4);
        memcpy(drCmd + 12, (char*)&tmp, 4);
        memcpy(drCmd + 20, (char*)ips, ecK * 4);
        memcpy(drCmd + 20 + 4 * ecK, (char*)&requestedFileNameLen, 4);
        memcpy(drCmd + 24 + 4 * ecK, filename.c_str(), requestedFileNameLen);
        cmdbase = 24 + 4 * ecK + requestedFileNameLen;

        if (COORDINATOR_DEBUG) 
        {
          cout << "Requestor: " << endl
            << "  0: ecK = " << ecK << endl
            << "  4: reqIP = ." << (requestorIP >> 24) << endl
            << "  8: m1(-1) = " << m1 << endl
            << "  12: tmp(fails) = " << tmp << endl
            << "  20: ips = ";
          for (int i = 0; i < ecK; i++) 
            cout << "." << (ips[i] >> 24) << " ";
          cout << endl << "  20+4k: requestedfileNameLen = " << requestedFileNameLen << endl << "  24+4k: requested File = " << filename << endl;
        }

        set<pair<unsigned int, string>> allBlks = _metadataBase -> getAllBlks(filename);
        for (int i = 0; i < ecK; i++) 
        {
          for (auto& it : allBlks) if (it.first == ips[i]) 
          {
            _status[it.second] = 2;
            break;
          }
        }

        for (auto& it : lostFiles) 
        {
          for (auto& it2 : stripe_mul) if (it2.second.first == it.first) 
          {
            addReqInfo(drCmd, (int*)(&cmdbase), it.second, it2.second);
            break;
          }
        }
        
        /* restore drCmd pointer, begin */
        drCmd = drCmd - 4;
        /* restore drCmd pointer, end */

        cmds.push_back({_ip2Ctx[idx].second.first, {drCmd, cmdbase + 4}});

      }

      // C3: send (f-1) passive recovery commands
      if (_conf -> _fileSysType == "HDFS")
        for (auto& it : stripe_mul) 
        {
          redisReply* rReply;

          cout << "Starts for: ." << (it.first >> 24) << ", " << it.second.first << "\n";

          if (it.first == requestorIP) 
          {
            printf("Skip\n"); 
            continue;
          }

          vector<pair<unsigned int, string>>& stripe = it.second.second;

          unsigned int failedIP = it.first;
          string lostFilename(it.second.first);
          unsigned int lostFilelength = lostFilename.length();

          map<string, int> coef = _metadataBase -> getCoefficient(lostFilename, &_status);

          char* drCmd;
          ecK = stripe.size();

          drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);
          int idx = searchCtx(_ip2Ctx, failedIP, 0, _slaveCnt - 1);

          memcpy(drCmd, (char*)&requestorIP, 4); // Last helper, receive (at passive's perspective)
          memcpy(drCmd + 4, (char*)&requestorIP, 4);  // requestor IP, send (at passive's perspective)
          memcpy(drCmd + 8, (char*)&lostFilelength, 4);
          memcpy(drCmd + 12, lostFilename.c_str(), lostFilelength);

          rReply = (redisReply*)redisCommand(_ip2Ctx[idx].second.second, "rpush dr_passive %b", drCmd, lostFilelength + 12);

          freeReplyObject(rReply);

          printf("%s: passive id: %d, ip = .%d\n", __func__, idx, (failedIP >> 24));
        }
      // Redis Multi 命令用于标记一个事务块的开始。事务块内的多条命令会按照先后顺序被放进一个队列当中，最后由 EXEC 命令原子性(atomic)地执行。
      // commands
      redisAppendCommand(rContext, "MULTI");
      for (auto& it : cmds) 
      {
        // 将cmd压入 disQ:0 中，并由coor的cmdDistributor线程去分发任务
        redisAppendCommand(rContext, "RPUSH %s %b", it.first.c_str(), it.second.first, it.second.second);
      }
      redisAppendCommand(rContext, "EXEC");

      // execute commands
      redisGetReply(rContext, (void **)&rReply);
      freeReplyObject(rReply);
      for (auto& it : cmds) 
      {
        redisGetReply(rContext, (void **)&rReply);
        freeReplyObject(rReply);
        free(it.second.first);
      }
      redisGetReply(rContext, (void **)&rReply);
      freeReplyObject(rReply);

      string retFilename = lostFiles[0].first; 
      printf("sending to _selfCtx, filename is %s\n", retFilename.c_str());
      rReply = (redisReply *)redisCommand(_selfCtx, "RPUSH %s %b", retFilename.c_str(), &requestorIP, 4);
      freeReplyObject(rReply);
      cmds.clear();
    }
  }
  // should never end ...
}


void ConvCoordinator::addReqInfo(char* drCmd, int* cmdbase, unsigned int failedIP, pair<string, vector<pair<unsigned int, string>>>& info) 
{
  string lostFilename(info.first);
  int len = lostFilename.length();
  int ecK = _conf -> _ecK;

  printf("LOG: %s: lostFilename = %s\n", __func__, lostFilename.c_str());
  map<string, int> coef = _metadataBase -> getCoefficient(lostFilename, &_status);

  printf("coef:\n");
  for (auto& it : coef) {
    cout << "(" << it.first << ", " << it.second << ") ";
  }
  printf("\n");

  cout << "  " << (*cmdbase) << ": ";
  memcpy(drCmd + *cmdbase, (char*)&(failedIP), 4);
  memcpy(drCmd + *cmdbase + 4, &(len), 4);
  memcpy(drCmd + *cmdbase + 8, lostFilename.c_str(), len);

  cout << "." << (failedIP >> 24) << ", " << len << ", " << lostFilename << endl;

  *cmdbase += 8 + lostFilename.length();
	cout << "  " << *cmdbase << ": ";

  for (int i = 0; i < ecK; i++) {
    memcpy(drCmd + *cmdbase + 4 * i, (char*)&(coef[info.second[i].second]), 4);
	  cout << coef[info.second[i].second] << ", ";
  }
  *cmdbase += 4 * ecK;
  
  cout << endl;
}
