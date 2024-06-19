#ifndef _ECPIPE_INPUT_STREAM_HH_
#define _ECPIPE_INPUT_STREAM_HH_

#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <cassert>

#include <hiredis/hiredis.h>
#include <string.h>
#include <sys/time.h>

#include <stdio.h>
#include <stdlib.h>

#include <arpa/inet.h>

#include "Config.hh"

using namespace std;

class ECPipeInputStream {
  private:
    bool _isCCtxSelfInit;
    redisContext* _coordinatorCtx;
    redisContext* _selfCtx;
    vector<pair<unsigned int, redisContext*>> _slaveCtx;

    Config* _conf;
    string _lostFileName;

    size_t _packetCnt;
    size_t _packetSize;
    size_t _processedPacket;
    size_t _completePacketIndex;
    string _DRPolicy;
    string _ECPipePolicy;

    unsigned int _locIP;
    thread _readerThread;
    bool _shutdownThread;
    char** _contents;

    redisContext* _pipePullCtx;
    vector<redisContext*> _cyclPullCtx;
    
    mutex _reader2SenderMtx;
    condition_variable _reader2SenderCondVar;

    void InitRedis(redisContext* &ctx, unsigned int ip, unsigned short port);
    bool sendToCoordinator();
    redisContext* findCtx(unsigned int ip);
    string ip2Str(unsigned int) const;

    void pipeCollector(); 
    void cyclCollector(); 
    void dataCollector(); 

  public :
    ECPipeInputStream(Config* conf, redisContext* cCtx, unsigned int locIP, const string& filename);
    ECPipeInputStream(size_t packetCnt, size_t packetSize, string DRPolicy, string ECPipePolicy, unsigned int cIP, unsigned int locIP);
    ECPipeInputStream(Config*, size_t packetCnt, size_t packetSize, string DRPolicy, string ECPipePolicy, unsigned int cIP, unsigned int locIP, const string& filename);
    ~ECPipeInputStream();
    void sendRequest(const string& lostFile);
    void getNextPacket(char* dest);
    // for test only
    void output2File(const string& filename);
    void close();
    
    // for qfs
    bool isReady();
    void sendMetadata(const string &info);
    bool canRecover(const string& blkname);
};

#endif //_ECPIPE_INPUT_STREAM_HH_
