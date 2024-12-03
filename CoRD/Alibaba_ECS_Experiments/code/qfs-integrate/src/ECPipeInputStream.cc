#include "ECPipeInputStream.hh"

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
  std::cout << "ECPipeInputStream start" << std::endl;
  InitRedis(_coordinatorCtx, cIP, 6379);
  InitRedis(_selfCtx, 0x0100007F, 6379);
  std::cout << "ECPipeInputStream constructor" << std::endl;
}


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

void ECPipeInputStream::sendRequest(const string& lostFile){
  assert(_coordinatorCtx && _selfCtx);

  _lostFileName = lostFile;

  _contents = new char*[_packetCnt];

  struct timeval tv1, tv2, tv1_1;
  gettimeofday(&tv1, NULL);

  if (!sendToCoordinator()) {
    // TODO: error handling
    ;
  };
  _readerThread = thread([=]{dataCollector();});

  /** 
   * TODO: this is a test only mode, should not block constructor
   */
  _readerThread.join();
  gettimeofday(&tv2, NULL);
  
  //cout << "ECPipeInputStream: start at " << tv1.tv_sec << "." << tv1.tv_usec 
  //  << " requestor sent at " << tv1_1.tv_sec << "." << tv1_1.tv_usec
  //  << " end at " << tv2.tv_sec << "." << tv2.tv_usec << endl;
  cout << "overall time: " << ((tv2.tv_sec - tv1.tv_sec) * 1000000.0 + tv2.tv_usec - tv1.tv_usec) / 1000000.0 << endl;

  //string outFilename("testfileOut");
  //output2File(outFilename);
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
  //cout << "Initializing IP: " << ip2Str(ip) << endl;
  struct timeval timeout = {1, 500000};
  char ipStr[INET_ADDRSTRLEN];

  /**
   * test-only begin
   */
  //if (ip == 0x0100007F) {
  //  cout << "initializing selfCtx" << endl;
  //  ctx = redisConnectWithTimeout("10.10.10.30", port, timeout);
  //  if (ctx == NULL || ctx -> err) {
  //    if (ctx) {
  //      cerr << "Connection error: redis host: " << ipStr << ":" << port << ", error msg: " << ctx -> errstr << endl;
  //      redisFree(ctx);
  //      ctx = 0;
  //    } else {
  //      cerr << "Connection error: can't allocate redis context at IP: " << ipStr << endl;
  //    }
  //    ;
  //  }
  //  return;
  //}
  /**
   * test-only end
   */
  
  ctx = redisConnectWithTimeout(inet_ntop(AF_INET, &ip, ipStr, INET_ADDRSTRLEN), port, timeout);
  if (ctx == NULL || ctx -> err) {
    if (ctx) {
      cerr << "Connection error: redis host: " << ipStr << ":" << port << ", error msg: " << ctx -> errstr << endl;
      redisFree(ctx);
      ctx = 0;
    } else {
      cerr << "Connection error: can't allocate redis context at IP: " << ipStr << endl;
    }
//<<<<<<< HEAD
//    _contents = (redisReply**)malloc(sizeof(redisReply*) * _conf -> _packetCnt);
//    _limit = 0;
//
//    struct timeval tv1, tv2, tv1_1;
//    gettimeofday(&tv1, NULL);
//    if (!sendToCoordinator()) {
//      // TODO: error handling
//      ;
//    };
//    gettimeofday(&tv1_1, NULL);
//    _readerThread = thread([=]{dataCollector();});
//
//    /** 
//     * TODO: this is a test only mode, should not block constructor
//     */
//=======
  }
}
ECPipeInputStream::~ECPipeInputStream(){
  if(_readerThread.joinable()){
    _shutdownThread = true;
//>>>>>>> d324b4f9b2049796b670a25fa302f5998262a7c8
    _readerThread.join();
  }
  if(_contents) delete[] _contents;
  if(_isCCtxSelfInit && _coordinatorCtx){
    redisFree(_coordinatorCtx);
    _coordinatorCtx = 0;
  }
  if(_selfCtx){
    redisFree(_selfCtx);
    _selfCtx = 0;
  }
}
bool ECPipeInputStream::sendToCoordinator() {
  char cmd[256];
  redisReply* rReply;
  unsigned int fnlen = _lostFileName.length();
  memcpy(cmd, (char*)&_locIP, 4);
  memcpy(cmd + 4, _lostFileName.c_str(), fnlen);
  //freeReplyObject(
  //  redisCommand(_coordinatorCtx,
  //    "RPUSH dr_requests %b", cmd, fnlen + 4)
  //);
  redisAppendCommand(_coordinatorCtx, "RPUSH dr_requests %b", cmd, fnlen + 4);
  if ( _DRPolicy == "conv" || 
       _DRPolicy == "ppr" ||
       ( _DRPolicy == "ecpipe" && _ECPipePolicy == "basic")) {
    unsigned int ip;
    redisAppendCommand(_coordinatorCtx, "BLPOP %s 0", _lostFileName.c_str());

    redisGetReply(_coordinatorCtx, (void**)&rReply);
    freeReplyObject(rReply);

    redisGetReply(_coordinatorCtx, (void**)&rReply);
    memcpy((char*)&ip, rReply -> element[1] -> str, 4);
    InitRedis(_pipePullCtx, ip, 6379);
//    _pipePullCtx = findCtx(ip);
    if (_pipePullCtx == NULL) cout << "ctx not found" << endl;
    freeReplyObject(rReply);
  } else if (_DRPolicy == "ecpipe" && _ECPipePolicy == "cyclic") {
    unsigned int ip;
    for (int i = 0; i < _conf -> _ecK - 1; i ++) {
      redisAppendCommand(_coordinatorCtx, "BLPOP %s 0", _lostFileName.c_str());
    }

    redisGetReply(_coordinatorCtx, (void**)&rReply);
    freeReplyObject(rReply);

    for (int i = 0; i < _conf -> _ecK - 1; i ++) {
      redisGetReply(_coordinatorCtx, (void**)&rReply);
      memcpy((char*)&ip, rReply -> element[1] -> str, 4);
      _cyclPullCtx.push_back(findCtx(ip));
      //if (_pipePullCtx == NULL) cout << "ctx not found" << endl;
      freeReplyObject(rReply);
    }
  }
  return true;
}

void ECPipeInputStream::dataCollector() {
  if (_DRPolicy == "ecpipe" && _ECPipePolicy == "cyclic") {
    cyclCollector();
  } else {
    pipeCollector();
  }
}

void ECPipeInputStream::cyclCollector() {
  redisReply* rReply;
  const char* filename = _lostFileName.c_str();
  int ecK = _conf -> _ecK;
  struct timeval tv1, tv2, tv3;
  vector<redisReply*> toFree;
  for (int i = 0; i < _packetCnt; i ++) {
    redisAppendCommand(_cyclPullCtx[i % (ecK - 1)], "BLPOP %s:%d 0", filename, i);
  }
  gettimeofday(&tv1, NULL);
  for (int i = 0; i < _packetCnt; i ++) {
    redisGetReply(_cyclPullCtx[i % (ecK - 1)], (void**)&rReply);
    //_contents[i] = (char*)malloc(sizeof(char) * _packetSize);
    //cout << "getting packet " << i << endl;
    //memcpy(_contents[i], rReply -> element[1] -> str, _packetSize);

    //_completePacketIndex ++;

    //freeReplyObject(rReply);
    toFree.push_back(rReply);
  }
  gettimeofday(&tv2, NULL);
  cout << "fetch time: " << ((tv2.tv_sec - tv1.tv_sec) * 1000000.0 + tv2.tv_usec - tv1.tv_usec) / 1000000.0 << endl;
  for (auto it : toFree) freeReplyObject(it);
  //while(_completePacketIndex < _packetCnt && !_shutdownThread) {
  //  rReply = (redisReply*)redisCommand(_selfCtx, "BLPOP %s:%d 10", filename, _completePacketIndex);

  //  if(rReply->type == REDIS_REPLY_NIL){
  //    cerr << "CyclCollector pop data timeout" << endl;
  //    // always retry now
  //    // if nil return, it is probably because of low recovery speed
  //    //   this should be verify later
  //    //memset(_contents + _completePacketIndex * _packetSize, 0, _packetSize);
  //  }else if(rReply->type == REDIS_REPLY_ERROR){
  //    cerr << "CyclCollector pop data return error" << endl;
  //    // if error occur, it is probably because of the file size
  //    //   this should be verify later

  //    // lock thread when reading
  //    lock_guard<mutex> lck(_reader2SenderMtx);

  //    _contents[_completePacketIndex] = new char[_packetSize];
  //    memset(_contents[_completePacketIndex], 0, _packetSize);

  //    _completePacketIndex ++;
  //  }else{
  //    // lock thread when reading
  //    lock_guard<mutex> lck(_reader2SenderMtx);

  //    _contents[_completePacketIndex] = new char[_packetSize];
  //    memcpy(_contents[_completePacketIndex], rReply->element[1] -> str, _packetSize);

  //    _completePacketIndex ++;
  //  }
  //  // done read, tell reader restart
  //  _reader2SenderCondVar.notify_all();

  //  freeReplyObject(rReply);
  //  // TODO: add error handling
  //}
}

void ECPipeInputStream::pipeCollector() {
  redisReply* rReply;
  const char* filename = _lostFileName.c_str();
  _completePacketIndex = 0;

  struct timeval tv1, tv2, temp;

  gettimeofday(&tv1, NULL);
  //for (int i = 0; i < _packetCnt; i ++) {
  //  //gettimeofday(&temp, NULL);
  //  //cout << "reading slice " << i << " at time " << temp.tv_sec << "s" << temp.tv_usec << "us" << endl;
  //  rReply = (redisReply*)redisCommand(_selfCtx, "BLPOP %s 10", filename);
  //  freeReplyObject(rReply);
  //  //cout << "read slice " << i << endl;
  //}
  for (int i = 0; i < _packetCnt; i ++) {
    //redisAppendCommand(_selfCtx, "BLPOP %s 0", filename);
    redisAppendCommand(_pipePullCtx, "BLPOP %s 0", filename);
  }

  for (int i = 0; i < _packetCnt; i ++) {
    _contents[i] = (char*)calloc(sizeof(char), _packetSize);
    redisGetReply(_pipePullCtx, (void**)&rReply);
    memcpy(_contents[i], rReply -> element[1] -> str, _packetSize);

    _completePacketIndex ++;

    freeReplyObject(rReply);
  }
  _reader2SenderCondVar.notify_all();
  gettimeofday(&tv2, NULL);

  cout << "read time: " << (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0 << endl;

  //while(_completePacketIndex < _packetCnt && !_shutdownThread) {
  //  rReply = (redisReply*)redisCommand(_selfCtx, "BLPOP %s 10", filename);

  //  if(rReply->type == REDIS_REPLY_NIL){
  //    cerr << "PipeCollector pop data timeout" << endl;
  //    // always retry now
  //    // if nil return, it is probably because of low recovery speed
  //    //   this should be verify later

  //    //memset(_contents + _completePacketIndex * _packetSize, 0, _packetSize);
  //  }else if(rReply->type == REDIS_REPLY_ERROR){
  //    cerr << "PipeCollector pop data return error" << endl;
  //    // if error occur, it is probably because of the file size
  //    //   this should be verify later

  //    // lock thread when reading
  //    lock_guard<mutex> lck(_reader2SenderMtx);

  //    _contents[_completePacketIndex] = new char[_packetSize];
  //    memset(_contents[_completePacketIndex], 0, _packetSize);

  //    _completePacketIndex ++;
  //  }else{
  //    // lock thread when reading
  //    lock_guard<mutex> lck(_reader2SenderMtx);

  //    _contents[_completePacketIndex] = new char[_packetSize];
  //    memcpy(_contents[_completePacketIndex], rReply->element[1] -> str, _packetSize);

  //    _completePacketIndex ++;
  //  }
  //  // done read, tell restart reader
  //  _reader2SenderCondVar.notify_all();

  //  freeReplyObject(rReply);
  //  // TODO: add error handling
  //}
}

// info structure
// "<recover chunk filename>:<index 0 filename>,<index 0 server ipStr>:<index 1 filename>,<index 1 server ipStr>:...:<index m filename>,<index m server ipStr>"

void ECPipeInputStream::output2File(const string& output) {
  ofstream ofs(output);
  for(_processedPacket=0; _processedPacket < _packetCnt; _processedPacket++){
    {
      unique_lock<mutex> lck(_reader2SenderMtx);
      _reader2SenderCondVar.wait(lck, [this]{return _processedPacket < _completePacketIndex;});

      // no need lock thread when reading
    }

    ofs.write(_contents[_processedPacket], _packetSize);
    delete _contents[_processedPacket];
    _contents[_processedPacket]=0;
  }
  ofs.close();
}

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
}

bool ECPipeInputStream::isReady() {
  return _coordinatorCtx && _selfCtx;
}

void ECPipeInputStream::sendMetadata(const string &info) {
  freeReplyObject(
    redisCommand(_coordinatorCtx,
	"RPUSH dr_meta %b", info.c_str(), info.length()));
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
