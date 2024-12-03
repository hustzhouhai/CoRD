#ifndef _DR_AGENT_HH_
#define _DR_AGENT_HH_

#include <iostream>
#include <pthread>

using namespace std;

/**
 * Manage DRWorker threads
 */
class ECHelper {
    int _packetCnt;
    int _packetSize;

    int _threadNum;

    // TODO: replace with ZeroMQ
    
    // Currently, we keep persistant connections
    vector<string> _slaveIPs;
    string _coordinatorIP;
  public:
    ECHelper();
};

#endif //_DR_AGENT_HH_
