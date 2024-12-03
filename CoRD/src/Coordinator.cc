#include "Coordinator.hh"

Coordinator::Coordinator(Config* conf) 
{
  _conf = conf;
  init();

  _handlerThrds = vector<thread>(_handlerThreadNum);
  _distThrds = vector<thread>(_distributorThreadNum);
}


int Coordinator::isRequestorHolder(vector<pair<unsigned int, string>>& stripe, unsigned int requestorIP) const {
  int sid = 0, eid = stripe.size() - 1, mid;
  while (sid < eid) {
    mid = (sid + eid) / 2;
    if (stripe[mid].first == requestorIP) return mid;
    else if (stripe[mid].first > requestorIP) eid = mid - 1;
    else sid = mid + 1;
  }
  return stripe[sid].first == requestorIP ? sid : -1;
}

/*
init the encoding matrix, read the metadata in dir standalone-test, and recv the block report from each ECHelper.cc
*/
void Coordinator::init() 
{
  _handlerThreadNum = _conf -> _coCmdReqHandlerThreadNum;
  _distributorThreadNum = _conf -> _coCmdDistThreadNum;
  _ecK = _conf -> _ecK;
  _ecN = _conf -> _ecN;
  _ecM = _ecN - _ecK;
  _slaveCnt = _conf -> _helpersIPs.size();
  
  if (COORDINATOR_DEBUG) 
    cout << "# of slaves: " << _slaveCnt << endl;
  
  /**
   * Queue of cmd distributor: "disQ:{i}", i is the index of the distributor
   */
  vector<int> rightBounder;

  if (_slaveCnt % _distributorThreadNum == 0) 
    rightBounder.push_back(_slaveCnt / _distributorThreadNum);
  else 
    rightBounder.push_back(_slaveCnt / _distributorThreadNum + 1);

  int remaining = _slaveCnt % _distributorThreadNum, idx, rBounderId;
  for (int i = 1; i < _distributorThreadNum; i ++) { 
    if (i >= remaining) rightBounder.push_back(rightBounder.back() + _slaveCnt / _distributorThreadNum);
    else rightBounder.push_back(rightBounder.back() + _slaveCnt / _distributorThreadNum + 1);
  }
  
  puts("start to read the parameter.");

  idx = 0; 
  rBounderId = 0;
  string prefix("disQ:");
  for (auto& it : _conf -> _helpersIPs) 
  {
    if (++ idx > rightBounder[rBounderId]) 
      rBounderId ++;
    _ip2Ctx.push_back({it, {prefix + to_string(rBounderId), initCtx(it)}});
    cout << prefix + to_string(rBounderId) << endl;
  }
  _selfCtx = initCtx(_conf -> _localIP);

  

  _rsConfigFile = _conf -> _rsConfigFile;  // read encode matrix info

  // add by Zuoru:
  printf("%s\n", _rsConfigFile.c_str());

  ifstream ifs(_rsConfigFile);
  _rsEncMat = (int*)malloc(sizeof(int) * _ecM * _ecK);
  for (int i = 0; i < _ecM; i ++) 
    for (int j = 0; j < _ecK; j ++) 
      ifs >> _rsEncMat[_ecK * i + j];
    
  ifs.close();

  puts("initializing rs utility");
  _rsUtil = new RSUtil(_conf, _rsEncMat);
  puts("finish the parameter reading");

  

  if (_conf -> _pathSelectionEnabled) 
  {
    // init matrix
    _linkWeight = vector<vector<double>>(_slaveCnt, vector<double>(_slaveCnt, 0));

    // read in data
    ifstream ifsLink(_conf -> _linkWeightConfigFile);
    for (int i = 0; i < _slaveCnt; i ++) {
      for (int j = 0; j < _slaveCnt; j ++) {
        ifsLink >> _linkWeight[i][j];
      }
    }
    ifsLink.close();

    cout << "_linkWeight: " << endl;
    for (int i = 0; i < _slaveCnt; i ++) {
      for (int j = 0; j < _slaveCnt; j ++) {
          cout << _linkWeight[i][j] << " ";
      }
      cout << endl;
    }

    // init _ip2idx
    for (int i = 0; i < _slaveCnt; i ++) {
      _ip2idx[_conf -> _helpersIPs[i]] = i;
    }
    cout << "_ip2idx: " << endl;
    for (int i = 0; i < _slaveCnt; i ++) {
      cout << _conf -> _helpersIPs[i] << " " << _ip2idx[_conf -> _helpersIPs[i]] << endl;
    }

    // init path selection
    _pathSelection = new PathSelection();
  }
  
  puts("Start to create the MetadataBase!");
  // start metadatabase
  if (_conf -> _fileSysType == "HDFS") 
  {
    
    if (COORDINATOR_DEBUG) cout << "creating metadata base for HDFS " << endl;
    _metadataBase = new HDFS_MetadataBase(_conf, _rsUtil);
    
    
  }
  else if(_conf -> _fileSysType == "QFS")
  {
    if (COORDINATOR_DEBUG) cout << "creating metadata base for QFS " << endl;
    _metadataBase = new QFS_MetadataBase(_conf, _rsUtil);
  }
  else if(_conf -> _fileSysType == "HDFS3")
  {
    // add by Zuoru: Hadoop3
    if (COORDINATOR_DEBUG) cout << "creating metadata base for HDFS3 " << endl;
    _metadataBase = new HDFS3_MetadataBase(_conf, _rsUtil);
    ///
  }

  
  cout << typeid(this).name() << "::" << __func__ << "() ends\n";
  //sort(_ip2Ctx.begin(), _ip2Ctx.end());

  
}

redisContext* Coordinator::initCtx(unsigned int redisIP) 
{
  struct timeval timeout = { 1, 500000 }; // 1.5 seconds
  cout << "initCtx: connect to " << ip2Str(redisIP) << endl;
  redisContext* rContext = redisConnectWithTimeout(ip2Str(redisIP).c_str(), 6379, timeout);
  if (rContext == NULL || rContext -> err) 
  {
    if (rContext) 
    {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } 
    else 
    {
      cerr << "Connection error: can't allocate redis context at IP: " << redisIP << endl;
    }
  }
  return rContext;
}

void Coordinator::doProcess() 
{
  
  // starts the threads
  for (int i = 0; i < _distributorThreadNum; i ++) 
  {
    _distThrds[i] = thread([=]{this -> cmdDistributor(i, _distributorThreadNum);});
  }
  for (int i = 0; i < _handlerThreadNum; i ++) 
  {
    _handlerThrds[i] = thread([=]{this -> requestHandler();});
  }

  // should not reach here
  for (int i = 0; i < _distributorThreadNum; i ++) 
  {
    _distThrds[i].join();
  }
  for (int i = 0; i < _handlerThreadNum; i ++) 
  {
    _handlerThrds[i].join();
  }
}

/**
 * Since the _ip2Ctx is sorted, we just do a binary search,
 * which should be faster than unordered_map with a modest number of slaves
 *
 * TODO: We current assume that IP must be able to be found.  Should add error
 * handling afterwards
 */
int Coordinator::searchCtx(vector<pair<unsigned int, pair<string, redisContext*>>>& arr, unsigned int target, size_t sId, size_t eId) 
{
  int mid;
  while (sId < eId) 
  {
    mid = (sId + eId) / 2;
    if (arr[mid].first < target) sId = mid + 1;
    else if (arr[mid].first > target) eId = mid - 1;
    else return mid;
  }
  return sId;
}

void Coordinator::cmdDistributor(int idx, int total) 
{
  
  //return;
  //size_t sId, eId, i, j;
  int i, j, currIdx;
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  string disQKey("disQ:");
  disQKey += to_string(idx);


  char* cmdBase;
  bool reqBlkHolder; // requestor itself is a block holder
  redisReply* rReply, *rReply2;

  // local filename of operator and requested block
  char locFN[FILENAME_MAX_LENGTH], dstFN[FILENAME_MAX_LENGTH]; 

  //if (COORDINATOR_DEBUG) cout << " cmdDistributor idx: " << idx 
  //  << " starting id: " << sId << " ending id: " << eId 
  //  << " charged ips: " << cIPstr << endl;

  // TODO: trying to switch communication to conditional variable
  redisContext* locCtx = redisConnectWithTimeout("127.0.0.1", 6379, timeout), *opCtx;
  if (locCtx == NULL || locCtx -> err) {
    if (locCtx) {
      cerr << "Connection error: " << locCtx -> errstr << endl;
    } else {
      cerr << "Connection error: can't allocate redis context" << endl;
    }
    redisFree(locCtx);
    return;
  }

  

  while (1) 
  {
    /* Redis command: BLPOP (LISTNAME1) [LISTNAME2 ...] TIMEOUT */
    rReply = (redisReply*)redisCommand(locCtx, "BLPOP %s 100", disQKey.c_str());
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "Coordinator::CmdDistributor() empty queue " << endl;
      freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "Coordinator::CmdDistributor() ERROR happens " << endl;
      freeReplyObject(rReply);
    } else {
      memcpy((char*)&currIdx, rReply -> element[1] -> str, 4);  //the id of each helper/requestor 
      opCtx = _ip2Ctx[currIdx].second.second;  // the redisContext for each helper/requestor

      // TODO: try use asynchronous style
      if (COORDINATOR_DEBUG)
	      printf("%s: currIdx = %d, IP = .%d\n", __func__, currIdx, _ip2Ctx[currIdx].first >> 24);
      // distribute the repair cmd for each helper/requestor
	    rReply2 = (redisReply*)redisCommand(opCtx, "RPUSH dr_cmds %b", rReply -> element[1] -> str + 4, rReply -> element[1] -> len - 4);  

      

      freeReplyObject(rReply2);
      freeReplyObject(rReply);
    }
  }
}


string Coordinator::ip2Str(unsigned int ip) const {
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

void Coordinator::requestHandler() {
  // now overrided by inherited classes
  // TODO: add ECPipe and PPR flag in Config.hh
  //requestHandlerECPipe();
  ;
}

int Coordinator::outputStripeMul(const map<unsigned int, pair<string, vector<pair<unsigned int, string>>>>& stripe_mul, map<unsigned int, int>& numOfJobs) 
{
  if (COORDINATOR_DEBUG) 
  {
    printf("%s::%s() starts\n", typeid(this).name(), __func__);
    printf("size = %d\n", (int)stripe_mul.size());
  }

  int flag = 1;
  for(auto& it : stripe_mul) 
  {
    const vector<pair<unsigned int, string>>& stripe = it.second.second;
    if (stripe.empty() || stripe.size() < _conf -> _ecK) 
      flag = 0;
    if (COORDINATOR_DEBUG)
      cout << __func__ << " stripe_mul: ." << (it.first >> 24) << ", " << it.second.first << ":" << endl;
    else
      cout << "LOG: " << __func__ << " stripe_mul: ." << (it.first >> 24) << ", " << it.second.first << ":" ;
    for (auto& it : stripe) 
    {
      if (COORDINATOR_DEBUG) 
        cout << __func__ << "   ." << (it.first >> 24) << ", " << it.second << endl;
      else
        cout << " ." << (it.first >>  24);
      if (numOfJobs.count(it.first)) 
        numOfJobs[it.first]++;
      else 
        numOfJobs[it.first] = 1;
    }

    if (!COORDINATOR_DEBUG) 
      printf("\n");
    
  }
  return flag;
}

// return the index of lostfile
int Coordinator::parseFRRequest(const char* reqStr, int reqLen, unsigned int requestorIP, string& reqFile,  vector<pair<string, int>>& lostFiles) 
{
  unsigned int index = 0;
  string version = "1.1";
  if (reqLen < 8) {
    if (COORDINATOR_DEBUG) printf("%s:request is too short!\n", __func__);
    exit(1);
  }

  memcpy((char*)&index, reqStr + 4, 4);

  // TODO: fix this
  if (index > _conf -> _ecK) {
    version = "1.0";
    index = 0;
  }
  
  if (index) 
    if (COORDINATOR_DEBUG) printf("%s: index = %d\n", __func__, index);
  

  lostFiles.clear();
  _status.clear();

  set<pair<unsigned int, string>> allBlks; // _metadataBase -> getAllBlks(lostFile);
  allBlks.clear();
  
  int cmdbase = 8, len, ip;
  int i = 0;

  reqFile = "";

  if (version == "1.1") 
  {
    while (cmdbase < reqLen) 
    {
      memcpy((char*)&len, reqStr + cmdbase, 4);
      string lostFile(reqStr + cmdbase + 4, len);
      ip = _metadataBase -> getIPfromBlk(lostFile);
      cmdbase += 4 + len;
      lostFiles.push_back({lostFile, ip}); 

      if (index) {
        if (i == index) reqFile = lostFile;
      }
      else 
        if (ip == requestorIP) reqFile = lostFile;
      //if (i == index) reqFile = lostFile;

      if (allBlks.empty()) 
      {
        allBlks = _metadataBase -> getAllBlks(lostFile);
        for (auto& it : allBlks) _status[it.second] = 1;
      }

      _status[lostFile] = 0;

      printf("LOG: %s: index = %d, len = %d, ", __func__, index, len);
      printf("base = %d, string = %s, IP = .%d\n", cmdbase - 4, lostFile.c_str(), ip >> 24);

      if (COORDINATOR_DEBUG) 
      {
        cout << "_status:\n";
        for (auto& it : _status) 
          printf("(%s,.%d,%d) ", it.first.c_str(), _metadataBase -> getIPfromBlk(it.first) >> 24, it.second);
        cout << endl;
      }

      i++;
    }
  }
  else 
  {
    string lostFile(reqStr + 4);
    allBlks = _metadataBase -> getAllBlks(lostFile);
    for (auto& it : allBlks) _status[it.second] = 1;
    ip = _metadataBase -> getIPfromBlk(lostFile);
    lostFiles.push_back({lostFile, ip});
    reqFile = lostFile;
    _status[lostFile] = 0;
    printf("LOG: %s: ver 1.0, index = %d, ", __func__, index);
    printf("string = %s, IP = .%d\n", lostFile.c_str(), ip >> 24);
  }

  // TODO: as degraded read
  if (reqFile == "") reqFile = lostFiles[0].first;

  if (COORDINATOR_DEBUG) {
    printf("Lostfiles: ");
    for (auto& it : lostFiles) 
      printf("(.%s, %d) ", it.first.c_str(), it.second >> 24);
    printf("\n");
  }

  return index;
}


int Coordinator::setupCyclReply(vector<pair<string, int>>& lostFiles, 
    map<unsigned int, pair<string, vector<pair<unsigned int, string>>>>& stripes, 
    char** drCmd, int* drCmdLen) 
{
  *drCmdLen = sizeof(int) * (_conf -> _ecK + 1) * lostFiles.size();
  *drCmd = (char*)malloc(*drCmdLen);
  int cmdbase = 0;
  int numOfFIP; 

  if (lostFiles.size() == 0) {
    printf("ERROR: %s: lostFiles is empty\n", __func__);
    exit(1);
  }

  if (stripes.size() == 0) {
    printf("ERROR: %s: stripes is empty\n", __func__);
    exit(1);
  } 

  for (int i = 0; i < lostFiles.size(); i++) {
    vector<pair<unsigned int, string>>& stripe = stripes[lostFiles[i].second].second; 
    numOfFIP = stripe.size() - 1;
    memcpy(*drCmd + cmdbase, &numOfFIP, 4);
    cmdbase += 4;

    if (numOfFIP != stripe.size() - 1) {
      if (COORDINATOR_DEBUG) printf("%s: numOfFIP is wrong: %d\n", __func__, numOfFIP);
      exit(1);
    }

    for (int j = 0; j < numOfFIP; j ++) {
      memcpy(*drCmd + cmdbase, &(stripe[j].first), 4);
      cmdbase += 4;
    }
    memcpy(*drCmd + cmdbase, &(lostFiles[i].second), 4);
    cmdbase += 4;
  }

  return 1;
  
}

int Coordinator::setupCyclReply(unsigned int reqIP, 
    vector<pair<unsigned int, string>>& stripe, 
    char** drCmd, int* drCmdLen) 
{
  *drCmdLen = sizeof(int) * (_conf -> _ecK + 1);
  *drCmd = (char*)malloc(*drCmdLen);
  int cmdbase = 0;
  int numOfFIP = _conf -> _ecK - 1;

  if (numOfFIP != stripe.size() - 1) {
    if (COORDINATOR_DEBUG) printf("%s: numOfFIP is wrong: %d\n", __func__, numOfFIP);
    exit(1);
  }
  
  memcpy(*drCmd + cmdbase, &numOfFIP, 4);
  cmdbase += 4;
  for (int j = 0; j < numOfFIP; j ++) {
    memcpy(*drCmd + cmdbase, &(stripe[j].first), 4);
    cmdbase += 4;
  }
  memcpy(*drCmd + cmdbase, &(reqIP), 4);
  cmdbase += 4;

  return 1;
}
