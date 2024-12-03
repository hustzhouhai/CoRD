#ifndef _DR_WORDER_HH_
#define _DR_WORDER_HH_

#include <algorithm>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <unordered_map>
#include <fstream>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "Computation.hh"
#include "Config.hh"
#include "RSUtil.hh"
#include "Util/hiredis.h"
#include "Util/gettime.h"

#include "DROpt.hh"

#define DR_WORKER_DEBUG true 
#define PACKET_TRANSFER_TIMEOUT 1000 // 1000 seconds

#define fr(i,m,n) for(int i = m; i <= n; i++)

using namespace std;

/**
 * Toy example of the sub-packet processing scheme (Pipeline)
 * k = 4, 6 packets in total
 * packet 0: 0 -> 1 -> 2 -> 3 -> r
 * packet 1:      0 -> 1 -> 2 -> 3 -> r
 * packet 2:           0 -> 1 -> 2 -> 3 -> r
 * packet 3:                0 -> 1 -> 2 -> 3 -> r
 * packet 4:                     0 -> 1 -> 2 -> 3 -> r
 * packet 5:                          0 -> 1 -> 2 -> 3 -> r
 *           | t1 | t2 | t3 | t4 | t5 | t6 | t7 | t8 | t9 |
 *
 * Toy example of the sub-packet processing scheme (Cyclic)
 *
 * packet 0: 1 -> 2 -> 3 -> 0
 * packet 1: 2 -> 3 -> 0 -> 1
 * packet 2: 3 -> 0 -> 1 -> 2
 *           | t1 | t2 | t3 |
 *
 * packet 3:                1 -> 2 -> 3 -> 0
 * packet 4:                2 -> 3 -> 0 -> 1
 * packet 5:                3 -> 0 -> 1 -> 2
 *                          | t4 | t5 | t6 |
 *
 * t4: node 0 send packet 0 to requestor
 * t5: node 1 send packet 1 to requestor
 * t6: node 2 send packet 2 to requestor
 * t7: node 0 send packet 3 to requestor
 * t8: node 1 send packet 4 to requestor
 * t9: node 2 send packet 5 to requestor
 *
 * Work pattern of DRWorker
 *   1. wait for cmds
 *   2. start read thread to read from disk
 *   3. read data from redis (should be sent from preceder) computed with data
 *   from disk, send to redis in successor
 */

class DRWorker 
{
  protected:
    int _packetCnt;
    size_t _packetSize;
    int _id;
    int _ecK;
    unsigned int _localIP;

    // we assume it is 1 in current implementation
    int _coefficient;
    Config* _conf;

    //string _fileName;

    vector<mutex> _diskMtx;
    condition_variable _diskCv;
    bool* _diskFlag;
    char** _diskPkts;

    // communication between the main and sender thread
    char** _toSend;
    int _waitingToSend;
    int _waitingToPull;
    mutex _mainSenderMtx;
    mutex _mainPullerMtx;
    condition_variable _mainSenderCondVar;
    condition_variable _mainPullerCV;
    
    vector<pair<unsigned int, redisContext*>> _slavesCtx;
    redisContext* _coordinatorCtx;
    redisContext* _selfCtx;

    // threads
    //void recvWorker(int sd);

    redisContext* initCtx(unsigned int);
    string ip2Str(unsigned int);
    void init(unsigned int, unsigned int, vector<unsigned int>&);

    // helper function
    redisContext* findCtx(unsigned int ip);
    int openLocalFile(string localBlkName); 
    void readPacketFromDisk(int fd, char* content, int packetSize, int base);

    // reset the protected variables after using
    void cleanup(); 
    string timeval2str(struct timeval& tv) {return to_string(tv.tv_sec) + "s" + to_string(tv.tv_usec) + "us";};

    virtual void passiveReceiver(bool cycl = false);
    // returns a flag, repairable or not

  public:
    virtual void doProcess() {;};

    /**
     * Use simple basic policy
     */
    //void doProcess2();

    DRWorker(Config* conf);
    // for test only
    DRWorker(int pCnt, int pktSize, int id, int ce, int ecK, string& fileName, int inSd, int outSd);

    DRWorker(int pCnt, int id, int ce, int ecK);
    DRWorker(int id);
};

#endif //_DR_WORDER_HH_


