#include "DRWorker.hh"

using namespace std;

DRWorker::DRWorker(Config* conf) : _conf(conf) 
{ 
  _packetCnt = _conf -> _packetCnt; 
  _packetSize = _conf -> _packetSize; 
  init(_conf -> _coordinatorIP, _conf -> _localIP, _conf -> _helpersIPs);
}

void DRWorker::cleanup() 
{
  fill_n(_diskFlag, _packetCnt, false);
  _waitingToSend = 0;
  for (int i = 0; i < _packetCnt; i ++) memset(_diskPkts[i], 0, _packetSize);
  cout << "DRWorker() cleanup finished" << endl;
}

void DRWorker::init(unsigned int cIP, unsigned int sIP, vector<unsigned int>& slaveIP) 
{
  _localIP = sIP;
  cout << "_packetCnt " << _packetCnt << endl;
  _diskPkts = (char**)malloc(_packetCnt * sizeof(char*));
  _toSend = (char**)malloc(_packetCnt * sizeof(char*));
  //_networkPkts = vector<char*>(_packetCnt, NULL);
  _diskFlag = (bool*)calloc(_packetCnt, sizeof(bool));
  //_networkFlag = vector<bool>(_packetCnt, false);
  _diskMtx = vector<mutex>(_packetCnt);
  //_networkMtx = vector<mutex>(_packetCnt);
  _waitingToSend = 0;

  for (int i = 0; i < _packetCnt; i ++) 
    _diskPkts[i] = (char*)calloc(sizeof(char), _packetSize);


  // TODO: create contexts to other agenets, self and coordinator
  _coordinatorCtx = initCtx(cIP);
  _selfCtx = initCtx(0);
  for (auto &it : slaveIP) {
    _slavesCtx.push_back({it, initCtx(it)});
  }
  sort(_slavesCtx.begin(), _slavesCtx.end());
}

string DRWorker::ip2Str(unsigned int ip) 
{
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

redisContext* DRWorker::initCtx(unsigned int redisIP) {
  cout << "initing cotex to " << ip2Str(redisIP) << endl;
  struct timeval timeout = { 1, 500000 }; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout(ip2Str(redisIP).c_str(), 6379, timeout);
  if (rContext == NULL || rContext -> err) {
    if (rContext) {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } else {
      cerr << "Connection error: can't allocate redis context at IP: " << redisIP << endl;
    }
  }
  return rContext;
}

// return the redisContext* of node ip using binary divid way
redisContext* DRWorker::findCtx(unsigned int ip) {
  int sid = 0, eid = _slavesCtx.size() - 1, mid;
  while (sid < eid) {
    mid = (sid + eid) / 2;
    if (_slavesCtx[mid].first == ip) { 
      return _slavesCtx[mid].second;
    } else if (_slavesCtx[mid].first < ip) {
      sid = mid + 1;
    } else {
      eid = mid - 1;
    }
  }
  return _slavesCtx[sid].first == ip ? _slavesCtx[sid].second : NULL;
}

int DRWorker::openLocalFile(string localBlkName) {

  int fd;

  string fullName = _conf -> _blkDir + '/' + localBlkName;
  fd = open(fullName.c_str(), O_RDONLY);

  // add by Zuoru: debug
  cout << "Test by Zuoru: fd: " << fd << endl;
  int subIndex = 0;
  while (fd == -1) {
    fullName = _conf -> _blkDir + '/' + "subdir" + to_string(subIndex) + '/' + localBlkName;
    cout << "the current fullName is: " << fullName << endl;
    fd = open(fullName.c_str(), O_RDONLY);
    if (fd != -1) {
       cout << "Find the file in the dir: " << fullName << endl;
       break;
    }
    subIndex ++;
    if (subIndex > 10) {
       cout << "ERROR: Can not find the file in given dir!" << endl;
       exit(1);
    }    
  }

  return fd;
}

// read the size of packetsize from file descriptor fd to buffer content at a given offset base
void DRWorker::readPacketFromDisk(int fd, char* content, int packetSize, int base) 
{
  // pread, pwrite - read from or write to a file descriptor at a given offset  
  // #include <unistd.h> 
  // ssize_t pread(int fd, void *buf, size_t count, off_t offset);
  // ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset); 
  int readLen = 0, readl;
  while (readLen < packetSize) 
  {
    if ((readl = pread(fd, content + readLen, packetSize - readLen, base + readLen)) < 0) 
    {
      cerr << "ERROR During disk read" << endl;
      exit(1);
    } 
    else 
    {
      readLen += readl;
    }
  }
}

/*
 * Fetch from the helper IP itself.
 * 1. It receives "dr_passive" command, containing [prevIP][reqIP-IP of EIS][lostBlkName]
 * 2. It receives the packets from [prevIP], and will inform [reqIP] when the work is finished
 */
void DRWorker::passiveReceiver(bool cycl) 
{
  redisReply* rReply;
  redisContext* locCtx = initCtx(_localIP);
  printf("%s()\n", __func__);
  if (locCtx == NULL) {
    printf("%s: initCtx(0) failed\n", __func__);
    return;
  }

  char* cmd;
  string lostBlkName;
  unsigned int lostBlkNameLen;
  unsigned int prevIP, reqIP;
  unsigned int fetchIPs;

  redisContext* prevCtx;
  vector<redisContext*> prevCtxes;
  int cmdbase;


  while (true) 
  {
    rReply = (redisReply*)redisCommand(locCtx, "blpop dr_passive 100");
    if (rReply -> type == REDIS_REPLY_NIL) 
    {
      if (DR_WORKER_DEBUG) 
        cout << typeid(this).name() << "::" << __func__ << "() empty list \n"; 
    } 
    else if (rReply -> type == REDIS_REPLY_ERROR) 
    {
      if (DR_WORKER_DEBUG) 
        cout << typeid(this).name() << "::" << __func__ << "() error happens \n"; 
      exit(1);
    } 
    else 
    {
      if (DR_WORKER_DEBUG)
        cout << __func__ << "(): ." << (_localIP >> 24) <<  " received\n";

      string sendKey;
      cmd = rReply -> element[1] -> str;
      cmdbase = 0;

      memcpy((char*)&prevIP, cmd, 4);
      if (DR_WORKER_DEBUG)
        cout << __func__ << "(): from ." << (prevIP >> 24) << endl;
      //prevCtx = (prevIP == _localIP) ? initCtx(prevIP) : findCtx(prevIP);
      prevCtx = (prevIP == _localIP) ? locCtx : findCtx(prevIP);
      cmdbase += 4;

      memcpy((char*)&reqIP, cmd + cmdbase, 4);  
      redisContext* reqCtx = (reqIP == _localIP) ? locCtx : findCtx(reqIP);
      //redisContext* reqCtx = (reqIP == _localIP) ? initCtx(prevIP) : findCtx(reqIP);
      cmdbase += 4;
      memcpy((char*)&lostBlkNameLen, cmd + cmdbase, 4); 
      cmdbase += 4;
      lostBlkName = string(cmd + cmdbase, lostBlkNameLen);

      if (DR_WORKER_DEBUG)
        printf("%s: requestor IP .%d, with filename = %s\n", __func__, (reqIP >> 24), lostBlkName.c_str());

      /* multiple failure receiver. Passive recovery
       * TODO: Use functions or threads
       * TODO: Mutex
       * */
      {
        
        /* Copied from ECPipeInputStream::pipeCollector() */
        redisReply* rReply;
        const char* filename = lostBlkName.c_str();
        int pktId = 0;
	unsigned int k1;

        struct timeval tv1, tv2, temp;

        gettimeofday(&tv1, NULL);
        for (int i = 0; i < _packetCnt; i ++) {
          if (DR_WORKER_DEBUG && pktOutput(i, _packetCnt)) 
            printf("%s: appending %d, filename = %s\n", __func__, i, lostBlkName.c_str());
          redisAppendCommand(prevCtx, "blpop %s 100", filename);
        }

	// xiaolu modify start June 3 2019
        // string output("testfilePassiveMulOutput");
        string output("testfileOut");
        // xiaolu modify end

        ofstream ofs(output);

        for (pktId = 0; pktId < _packetCnt; pktId ++) {
          if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) 
	    printf("%s: getting %d ... ", __func__, pktId);
          redisGetReply(prevCtx, (void**)&rReply);

          if (DR_WORKER_DEBUG && pktOutput(pktId, _packetCnt)) { 
	    printf("received %d, [%x], at %s\n", pktId, *(int*)(rReply -> element[1] -> str), 
                getTime(tv1, tv2).c_str()); 
            redisReply* rReply2;
          }

          ofs.write(rReply -> element[1] -> str, _packetSize);

          freeReplyObject(rReply);
        }
        gettimeofday(&tv2, NULL);

        cout << "read time: " << getTime(tv1, tv2) << endl; 
        
        ofs.close();
	
	char cmd[COMMAND_MAX_LENGTH];
	printf("%s: writing fr_end .%d\n", __func__, reqIP >> 24);
	memcpy(cmd, &(_localIP), 4);
	memcpy(cmd + 4, filename, lostBlkName.length());
        rReply = (redisReply*)redisCommand(reqCtx, "rpush fr_end_%s %b", filename, cmd, lostBlkName.length() + 4);
        freeReplyObject(rReply);
      }

      freeReplyObject(rReply);

    }
  }
}


