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
#include "DROpt.hh"
#include "Util/gettime.h"

#define EIS_DEBUG 0 

using namespace std;

class ECPipeInputStream {
  private:
    bool _isCCtxSelfInit;
    redisContext* _coordinatorCtx;
    redisContext* _selfCtx;
    vector<pair<unsigned int, redisContext*>> _slaveCtx;

    Config* _conf;

    // added by jhli
    // string _lostFileName;
    vector<pair<string, unsigned int>> _lostFiles; // The second element
    vector<unsigned int> _fetchIPs;
    int _nj; 

    size_t _packetCnt;
    size_t _packetSize;
    size_t _processedPacket;
    size_t _completePacketIndex;
    string _DRPolicy;
    string _ECPipePolicy;

    unsigned int _locIP;
    thread _readerThread;
    bool _shutdownThread;
    char*** _contents;

    // modified by jhli
    //redisContext* _pipePullCtx;
    //vector<redisContext*> _cyclPullCtx;
    redisContext** _pipePullCtx;
    vector<redisContext**> _cyclPullCtx;
    
    mutex _reader2SenderMtx;
    condition_variable _reader2SenderCondVar;

    void InitRedis(redisContext* &ctx, unsigned int ip, unsigned short port);
    bool sendToCoordinator();
    redisContext* findCtx(unsigned int ip);
    string ip2Str(unsigned int) const;

    void pipeCollector(); 
    void cyclCollector(); 
    void dataCollector(); 

    // add by jhli
    int** _indexCompleted;
    mutex indexMtx;
    void pipeForwardWorker();
    void cyclReceiver(redisContext*, int, int, struct timeval);

  public :
    // ECPipeInputStream(Config* conf, redisContext* cCtx, unsigned int locIP, const string& filename);
    // ECPipeInputStream(size_t packetCnt, size_t packetSize, string DRPolicy, string ECPipePolicy, unsigned int cIP, unsigned int locIP);
    // ECPipeInputStream(Config*, size_t packetCnt, size_t packetSize, string DRPolicy, string ECPipePolicy, unsigned int cIP, unsigned int locIP, const string& filename);
    ~ECPipeInputStream();
    void sendMetadata(const string &info);
    bool canRecover(const string& blkname);
    // void getNextPacket(char* dest);
    // for test only
    void output2File(const string& filename);
    void close();

    // added by jhli
    ECPipeInputStream(Config*, size_t packetCnt, size_t packetSize, string DRPolicy, string ECPipePolicy, unsigned int cIP, unsigned int locIP, const vector<string>& filenames);
    // void sendRequest(const vector<string>& lostFiles);
    void sendRequest();
};

#endif //_ECPIPE_INPUT_STREAM_HH_
