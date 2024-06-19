#include "ECPipeInputStream.hh"


/*
ECPipeInputStream::ECPipeInputStream(Config* conf, redisContext* cCtx, unsigned int locIP, const string& filename) : 
    _isCCtxSelfInit(false),
    _coordinatorCtx(cCtx),
    _lostFileName(filename),
    _packetCnt(conf->_packetCnt),
    _packetSize(conf->_packetSize),
    _processedPacket(0),
    _completePacketIndex(0),
    _DRPolicy(conf->_DRPolicy),
    _ECPipePolicy(conf->_ECPipePolicy),
    _locIP(locIP),
    _shutdownThread(false),
    _contents(NULL)
{
  sendRequest(_lostFileName);
  InitRedis(_selfCtx, 0x0100007F, 6379);
  for (auto it : _conf -> _helpersIPs) {
    redisContext *temp;
    InitRedis(temp, it, 6379);
    _slaveCtx.push_back({it, temp});
  }
}

ECPipeInputStream::ECPipeInputStream( size_t packetCnt,
                                  size_t packetSize,
                                  string DRPolicy,
                                  string ECPipePolicy,
                                  unsigned int cIP,
                                  unsigned int locIP) :
    _isCCtxSelfInit(true),
    _packetCnt(packetCnt),
    _packetSize(packetSize),
    _processedPacket(0),
    _completePacketIndex(0),
    _DRPolicy(DRPolicy),
    _ECPipePolicy(ECPipePolicy),
    _locIP(locIP),
    _shutdownThread(false),
    _contents(NULL)
{
  InitRedis(_coordinatorCtx, cIP, 6379);
  InitRedis(_selfCtx, 0x0100007F, 6379);
  for (auto it : _conf -> _helpersIPs) {
    redisContext *temp;
    InitRedis(temp, it, 6379);
    _slaveCtx.push_back({it, temp});
  }
}
 */

ECPipeInputStream::ECPipeInputStream( Config* conf,
                                  size_t packetCnt,
                                  size_t packetSize,
                                  string DRPolicy,
                                  string ECPipePolicy,
                                  unsigned int cIP,
                                  unsigned int locIP,
                                  const vector<string>& filenames) :
    _conf(conf),
    _isCCtxSelfInit(true),
    _packetCnt(packetCnt),
    _packetSize(packetSize),
    _processedPacket(0),
    _completePacketIndex(0),
    _DRPolicy(DRPolicy),
    _ECPipePolicy(ECPipePolicy),
    _locIP(locIP),
    _shutdownThread(false),
    _contents(NULL)
{
  _lostFiles.clear();
  // Multi-failure recovery only works for conventional and basic pipeline
  if (_conf -> _DRPolicy == "conv" || (_conf -> _DRPolicy == "ecpipe" && (_conf -> _ECPipePolicy == "basic" || _conf -> _ECPipePolicy == "cyclic")))
    for (auto& it : filenames) 
    {
      _lostFiles.push_back({it, 0});
    }
  else
    _lostFiles.push_back({filenames[0], 0});
  _nj = _lostFiles.size();
  InitRedis(_coordinatorCtx, cIP, 6379);
  InitRedis(_selfCtx, 0x0100007F, 6379);
  for (auto it : _conf -> _helpersIPs) 
  {
    redisContext *temp;
    InitRedis(temp, it, 6379);
    _slaveCtx.push_back({it, temp});
  }
  sendRequest();
}

// return the redisContext* of node ip using binary divid way
redisContext* ECPipeInputStream::findCtx(unsigned int ip){
  int sid = 0, eid = _slaveCtx.size() - 1, mid;
  while (sid < eid) {
    mid = (sid + eid) / 2;
    if (_slaveCtx[mid].first == ip) { 
      return _slaveCtx[mid].second;
    } else if (_slaveCtx[mid].first < ip) {
      sid = mid + 1;
    } else {
      eid = mid - 1;
    }
  }
  return _slaveCtx[sid].first == ip ? _slaveCtx[sid].second : NULL;
}

void ECPipeInputStream::sendRequest()
{
  if (EIS_DEBUG)
    printf("%s::%s() starts\n", typeid(this).name(), __func__);
  assert(_coordinatorCtx && _selfCtx);

  _contents = new char**[_packetCnt];

  struct timeval tv1, tv2, tv1_1;
  gettimeofday(&tv1, NULL);
  if (!sendToCoordinator()) 
  {
    // TODO: error handling
    ;
  };
  _readerThread = thread([=]{dataCollector();});  // write data  dataCollector()-->pipeCollector()-->pipeForwardWorker()

  /** 
   * TODO: this is a test only mode, should not block constructor
   */
  _readerThread.join();
  redisReply* rReply;
  if (_conf -> _DRPolicy == "conv")
  for (int i = 1; i < _lostFiles.size(); i++) {
    rReply = (redisReply*)redisCommand(_selfCtx, "blpop fr_end_%s 100", _lostFiles[i].first.c_str());
    int reqIP;
    memcpy(&reqIP, rReply -> element[1] -> str, 4);
    char* filename = rReply -> element[1] -> str + 4;
    printf("recovered %s on IP .%d\n", filename, reqIP >> 24);
    freeReplyObject(rReply);
  }
  gettimeofday(&tv2, NULL);
  
  cout << "overall time: " << ((tv2.tv_sec - tv1.tv_sec) * 1000000.0 + tv2.tv_usec - tv1.tv_usec) / 1000000.0 << endl;
}

string ECPipeInputStream::ip2Str(unsigned int ip) const {
  string retVal;
  retVal += to_string(ip & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 8) & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 16) & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 24) & 0xff);
  return retVal;
}

void ECPipeInputStream::InitRedis(redisContext* &ctx, unsigned int ip, unsigned short port){
  if (EIS_DEBUG) cout << "Initializing IP: " << ip2Str(ip) << endl;
  struct timeval timeout = {1, 500000};
  char ipStr[INET_ADDRSTRLEN];

  
  ctx = redisConnectWithTimeout(inet_ntop(AF_INET, &ip, ipStr, INET_ADDRSTRLEN), port, timeout);
  if (ctx == NULL || ctx -> err) {
    if (ctx) {
      cerr << "Connection error: redis host: " << ipStr << ":" << port << ", error msg: " << ctx -> errstr << endl;
      redisFree(ctx);
      ctx = 0;
    } else {
      cerr << "Connection error: can't allocate redis context at IP: " << ipStr << endl;
    }
  }
}

ECPipeInputStream::~ECPipeInputStream(){
  if(_readerThread.joinable()){
    _shutdownThread = true;
    _readerThread.join();
  }
  if(_contents) {
    for (int i = 0; i < _packetCnt; i++) delete[] _contents[i];
    delete[] _contents;
  }
  if(_isCCtxSelfInit && _coordinatorCtx){
    redisFree(_coordinatorCtx);
    _coordinatorCtx = 0;
  }
  if(_selfCtx){
    redisFree(_selfCtx);
    _selfCtx = 0;
  }
}

bool ECPipeInputStream::sendToCoordinator() 
{
  if (EIS_DEBUG) 
    printf("%s::%s() starts\n", typeid(this).name(), __func__);
  
  char cmd[COMMAND_MAX_LENGTH];
  redisReply* rReply;

  unsigned int fnlen = 0;
  int replyLen;
  char* replyStr;
  // cmd  : |- locIP (4bytes) -|- 0000 -|- the length of lost block name (4bytes) -|- lost_blk_name -|
  memcpy(cmd, (char*)&_locIP, 4);
  memcpy(cmd + 4, &fnlen, 4);
  if (EIS_DEBUG) 
  {
    printf("%s: 0, %d\n", __func__, _locIP);
    printf("%s: 4, %d\n", __func__, fnlen);
  }
  int cmdbase = 8;
  for (int i = 0; i < _lostFiles.size(); i++) 
  {
    fnlen = _lostFiles[i].first.length();  // the length of lost block name
    memcpy(cmd + cmdbase, &fnlen, 4);
    memcpy(cmd + cmdbase + 4, _lostFiles[i].first.c_str(), fnlen);
    if (EIS_DEBUG) 
      printf("%s: at %d, %s\n", __func__, cmdbase, _lostFiles[i].first.c_str());
    cmdbase += 4 + fnlen;
  }
  redisAppendCommand(_coordinatorCtx, "RPUSH dr_requests %b", cmd, cmdbase);

  if ( _conf -> _DRPolicy == "conv" || _conf -> _DRPolicy == "ppr" || 
      (_conf -> _DRPolicy == "ecpipe" && _conf -> _ECPipePolicy.find("basic") != string::npos) ||
      _conf -> _DRPolicy == "diy") 
  {
    unsigned int fetchIP, reqIP;
    if (EIS_DEBUG) 
      printf("%s: creating _pipePullCtxs on lost files, %s first \n", __func__, _lostFiles[0].first.c_str());
    redisAppendCommand(_coordinatorCtx, "BLPOP %s 0", _lostFiles[0].first.c_str());  // 超时参数设为 0 表示阻塞时间可以无限期延长

    redisGetReply(_coordinatorCtx, (void**)&rReply);  // send "dr_requests"
    freeReplyObject(rReply);

    redisGetReply(_coordinatorCtx, (void**)&rReply);  // receive response from "dr_requests"
    replyLen = rReply -> element[1] -> len;
    replyStr = rReply -> element[1] -> str;
    if (EIS_DEBUG) printf("get reply: len = %d\n", replyLen);
    
    if (_conf -> _DRPolicy == "conv" || _conf -> _DRPolicy == "ppr") _nj = 1;
    //if (_conf -> _DRPolicy == "ppr") _nj = 1;

    if (replyLen != 8 * _nj - 4) {
      printf("%s: ERROR: not enough ips!\n", __func__);
      exit(1);
    }
    
    _pipePullCtx = (redisContext**)malloc(sizeof(redisContext*) * _nj);

    memcpy((char*)&fetchIP, replyStr, 4);  // Get fetch IP 0
    _fetchIPs.push_back(fetchIP);
    _lostFiles[0].second = _locIP;
    _pipePullCtx[0] = findCtx(fetchIP);

    for (int j = 1; j < _nj; j++) {  // Parse command: Get other fetch IPs from the command
      memcpy((char*)&fetchIP, replyStr + j * 8 - 4, 4);
      memcpy((char*)&reqIP, replyStr + j * 8, 4);
      _pipePullCtx[j] = findCtx(fetchIP);
      _lostFiles[j].second = reqIP;
      if (_pipePullCtx[j] == NULL) cout << "ctx not found" << endl;
    }

    if (EIS_DEBUG) {  // display debug information
      printf("_nj = %d, _fetchIPs.size = %d, _lostFiles.size = %d\n", 
        _nj, (int)_fetchIPs.size(), (int)_lostFiles.size());
      printf("%s: received, ip =", __func__);
      for (int j = 0; j < _nj; j++) {
        printf(" (.%d,", _fetchIPs[j] >> 24);
        printf(".%d)", _lostFiles[j].second >> 24);
      }
      printf("\n");
    }
    freeReplyObject(rReply);
  }
  else if (_conf -> _DRPolicy == "ecpipe" && _conf -> _ECPipePolicy.find("cyclic") != string::npos) {
    unsigned int ip;
    unsigned int cmdbase = 0;
    char* reqStr;
    int reqLen;

    redisContext** ctxes;
    redisAppendCommand(_coordinatorCtx, "BLPOP %s 0", _lostFiles[0].first.c_str());

    if (EIS_DEBUG) printf("%s: getting reply from coordinator...\n", __func__);

    redisGetReply(_coordinatorCtx, (void**)&rReply);
    freeReplyObject(rReply);

    redisGetReply(_coordinatorCtx, (void**)&rReply); // return fetch IPs, req IPs
    reqStr = rReply -> element[1] -> str;
    reqLen = rReply -> element[1] -> len;

    for (int j = 0; j < _nj; j ++) {
      int numOfFetchIPs;  // For most cases, this value should be k-1
      if (cmdbase < reqLen) {
        memcpy((char*)&numOfFetchIPs, reqStr + cmdbase, 4);
        cmdbase += 4;
      }
      else {
        printf("%s: ERROR: too large cmdbase = %d\n", __func__, cmdbase);
        exit(1);
      }
      if (numOfFetchIPs == 0 || numOfFetchIPs > _conf -> _ecN) {
        printf("%s: ERROR: numOfFetchIPs wrong (%d)\n", __func__, numOfFetchIPs);
        exit(1);
      }
      ctxes = (redisContext**)malloc(sizeof(redisContext**) * numOfFetchIPs);
      if (EIS_DEBUG) printf(" fetch IPs: ");
      for (int k = 0; k < numOfFetchIPs; k++) {
        memcpy((char*)&ip, reqStr + cmdbase, 4);
        cmdbase += 4;
        ctxes[k] = findCtx(ip);
        if (EIS_DEBUG) printf(" .%d", ip >> 24);
      }

      _cyclPullCtx.push_back(ctxes);

      if (cmdbase < reqLen) {
        memcpy((char*)&ip, reqStr + cmdbase, 4);  // reqIP
        cmdbase += 4;
        _lostFiles[j].second = ip;
        if (EIS_DEBUG) printf("\nreqIP of %s: .%d\n", _lostFiles[j].first.c_str(), ip >> 24);
      }
      else {
        printf("%s: ERROR: too large cmdbase = %d\n", __func__, cmdbase);
        exit(1);
      }

    }
    freeReplyObject(rReply);
  }
  if (EIS_DEBUG) printf("%s::%s() ends\n", typeid(this).name(), __func__);
  return true;
}

void ECPipeInputStream::dataCollector() {
  if (_DRPolicy == "ecpipe" && _ECPipePolicy.find("cyclic") != string::npos) {
    cyclCollector();
  } else {
    pipeCollector();
  }
}

void ECPipeInputStream::cyclReceiver(redisContext* rc, int cnts, int j, struct timeval tv) {
  redisReply* rReply;
  int pktId;
  struct timeval tv2;

  bool flag = false;
  int newIndex, baseIndex;
  int idt, jt;

  for (int i = 0; i < cnts; i++) { 
    redisGetReply(rc, (void**)&rReply);
    pktId = *(int*)(rReply -> element[1] -> str + _packetSize);
    _contents[pktId][j] = new char[_packetSize];
    memcpy(_contents[pktId][j], rReply -> element[1] -> str, _packetSize);

    gettimeofday(&tv2, NULL);
    if (EIS_DEBUG && (i < 2 || cnts - i < 2)) printf("%s: received %d, pktId %d, job %d, time = %s\n", __func__, i, pktId, j, getTime(tv, tv2).c_str());

    freeReplyObject(rReply);

    _indexCompleted[pktId][j] = 1;
    newIndex = pktId * _nj + j;
    baseIndex = _completePacketIndex;

    // critical section
    indexMtx.lock();

    flag = true;
    while (baseIndex < newIndex) {
      if (_indexCompleted[baseIndex / _nj][baseIndex % _nj]) { 
        baseIndex++;
      }
      else {
        flag = false;
        break;
      }
    }

    if (flag) {
      _completePacketIndex = newIndex + 1;
      _reader2SenderCondVar.notify_one();
    }

    indexMtx.unlock();
  }
}

void ECPipeInputStream::cyclCollector() {
  redisReply* rReply;
  int ecK = _conf -> _ecK;
  struct timeval tv1, tv2, tv3;

  _completePacketIndex = 0;

  thread forwardWorker;
  forwardWorker = thread([=]{pipeForwardWorker();}); 

  gettimeofday(&tv1, NULL);
  for (int i = 0; i < _packetCnt; i ++) {
    for (int j = 0; j < _nj; j++) {
      redisAppendCommand(_cyclPullCtx[j][i % (ecK - 1)], "BLPOP %s 0", _lostFiles[j].first.c_str());
      // TODO: here (ecK - 1) should be numOfFetchIPs
    }
  }

  int pktId;
  _indexCompleted = new int*[_packetCnt]; 
  thread cyclReceivers[(ecK - 1) * _nj];

  for (int i = 0; i < _packetCnt; i ++) {
    _contents[i] = new char*[_nj];
    _indexCompleted[i] = new int[_nj];
    for (int j = 0; j < _nj; j++) _indexCompleted[i][j] = 0;
    /*
    for (int j = 0; j < _nj; j++) {
      // TODO: here (ecK - 1) should be numOfFetchIPs
      redisGetReply(_cyclPullCtx[j][i % (ecK - 1)], (void**)&rReply);

      pktId = *(int*)(rReply -> element[1] -> str + _packetSize);

      _contents[pktId][j] = new char[_packetSize]; //(char*)malloc(sizeof(char) * _packetSize);
      memcpy(_contents[pktId][j], rReply -> element[1] -> str, _packetSize);

      //if (EIS_DEBUG && pktOutput(i, _packetCnt)) 
        gettimeofday(&tv2, NULL);
        printf("%s: received %d, pktId %d, job %d, filename = %s, first byte = [%x] or [%x], time = %s\n", __func__, i, pktId, j, _lostFiles[j].first.c_str(), *(_contents[i][j]), *(rReply -> element[1] -> str), getTime(tv1, tv2).c_str());


      freeReplyObject(rReply);
    }
    */
  }

  int div = _packetCnt / (ecK - 1);
  int mod = _packetCnt % (ecK - 1);

  for (int i = 0; i < ecK - 1; i++) {
    for (int j = 0; j < _nj; j++) {
      cyclReceivers[i * _nj + j] = thread([=]{
          cyclReceiver(_cyclPullCtx[j][i], div + (i < mod), j, tv1);
          });
    }
  }

  for (int i = 0; i < (ecK - 1) * _nj; i++) cyclReceivers[i].join();

  _completePacketIndex = _packetCnt;
  _reader2SenderCondVar.notify_one();

  forwardWorker.join();

  free(_indexCompleted);

  gettimeofday(&tv2, NULL);
  cout << "fetch time: " << ((tv2.tv_sec - tv1.tv_sec) * 1000000.0 + tv2.tv_usec - tv1.tv_usec) / 1000000.0 << endl;
}

void ECPipeInputStream::pipeCollector() 
{
  redisReply* rReply;
  _completePacketIndex = 0;

  struct timeval tv1, tv2, temp;
  thread forwardWorker;

  forwardWorker = thread([=]{pipeForwardWorker();}); 

  gettimeofday(&tv1, NULL);
  for (int i = 0; i < _packetCnt; i ++) 
  {
    for (int j = 0; j < _nj; j++) 
    {
      if (EIS_DEBUG && pktOutput(i, _packetCnt)) 
	      printf("%s: appending %d, job %d, filename = %s\n", __func__, i, j, _lostFiles[j].first.c_str());
      redisAppendCommand(_pipePullCtx[j], "BLPOP %s 0", _lostFiles[j].first.c_str());
    }
  }

  for (int i = 0; i < _packetCnt; i ++) 
  {
    _contents[i] = new char*[_nj];
    for (int j = 0; j < _nj; j++) 
    {
      _contents[i][j] = new char[_packetSize]; // (char*)calloc(sizeof(char), _packetSize);

      redisGetReply(_pipePullCtx[j], (void**)&rReply);
      if (EIS_DEBUG && pktOutput(i, _packetCnt)) 
      {
        gettimeofday(&tv2, NULL);
        printf("%s: packet %d %s, job %d, time %s\n", __func__, i, _lostFiles[j].first.c_str(), j, getTime(tv1, tv2).c_str()); 
      }
      memcpy(_contents[i][j], rReply -> element[1] -> str, _packetSize);

      _completePacketIndex ++;
      _reader2SenderCondVar.notify_one();

      freeReplyObject(rReply);
    }
  }

  forwardWorker.join();
  gettimeofday(&tv2, NULL);

  cout << "read time: " << (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0 << endl;
}

void ECPipeInputStream::sendMetadata(const string &info){
  freeReplyObject(
    redisCommand(_coordinatorCtx,
      "RPUSH dr_meta %b", info.c_str(), info.length())
  );
}

bool ECPipeInputStream::canRecover(const string &blkname){
  redisReply* rReply = (redisReply*)redisCommand(_coordinatorCtx,
      "BLPOP dr_meta_ret:%s 5", blkname.c_str());
  bool ret = rReply->type != REDIS_REPLY_NIL &&
             rReply->type != REDIS_REPLY_ERROR &&
             !!strcmp(rReply->element[1]->str, "0");
  freeReplyObject(rReply);
  return ret;
}

void ECPipeInputStream::output2File(const string& output) {
  ofstream ofs(output);
  for(_processedPacket=0; _processedPacket < _packetCnt; _processedPacket++){
    {
      unique_lock<mutex> lck(_reader2SenderMtx);
      _reader2SenderCondVar.wait(lck, [this]{return _processedPacket < _completePacketIndex;});

      // no need lock thread when reading
    }

    ofs.write(_contents[_processedPacket][0], _packetSize);
    delete _contents[_processedPacket][0];
    _contents[_processedPacket][0]=0;
  }
  ofs.close();
}
/*
void ECPipeInputStream::getNextPacket(char* dest){
  {
    unique_lock<mutex> lck(_reader2SenderMtx);
    _reader2SenderCondVar.wait(lck, [this]{return _processedPacket < _completePacketIndex;});

    // no need lock thread when reading
  }

  if(_processedPacket < _packetCnt){
      memcpy(dest, _contents[_processedPacket], _packetSize);
      delete _contents[_processedPacket];
      _contents[_processedPacket] = 0;
  }else memset(dest, 0, _packetSize);
  _processedPacket++;
}*/

void ECPipeInputStream::pipeForwardWorker() 
{
  ofstream ofs("testfileOut");
  redisReply* rReply;
  redisContext** requestCtxes = new redisContext*[_nj];
  for (int j = 0; j < _nj; j++) 
    InitRedis(requestCtxes[j], _locIP, 6379);
  
  if (EIS_DEBUG) 
    cout << __func__ << " starts\n";

  _processedPacket = 0;

  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);

  for (int i = 0; i < _packetCnt; i ++)
  {
    for (int j = 0; j < _nj; j++) 
    {
      {
        unique_lock<mutex> lck(_reader2SenderMtx);
        _reader2SenderCondVar.wait(lck, [this]{return _processedPacket < _completePacketIndex;});
      }
      
      _processedPacket++;

      if (!j) 
      {
        ofs.write(_contents[i][0], _packetSize);
        delete _contents[i][0];
        _contents[i][0]=0;
      }
      else 
      {
	      rReply = (redisReply*)redisCommand(requestCtxes[j], "rpush %s %b", _lostFiles[j].first.c_str(), _contents[i][j], _packetSize);
	      freeReplyObject(rReply);
      }
      gettimeofday(&tv2, NULL);
      if (EIS_DEBUG && pktOutput(i, _packetCnt)) 
	      printf("%s: packet %d, job %d, time %.6lf s\n", __func__, i, j, (tv2.tv_sec - tv1.tv_sec + (tv2.tv_usec - tv1.tv_usec)*1.0/1000000.0)*1.0);
    }
  }
  ofs.close();
}
