#ifndef _PPR_PULL_DR_WORKER_HH_
#define _PPR_PULL_DR_WORKER_HH_

#include <map>

#include "DRWorker.hh"

class PPRPullDRWorker : public DRWorker {
    int* _indicesWaiting;
    void readWorker(const string&);
    void puller(const string&, unsigned int* holderIps, const string& lostBlkName);
    void readThread(const string&, const pair<unsigned int, redisContext*>& ctxPair, int num, int totNum, int isRequestor);

    int _waitingToPull;
    mutex _toPullerMtx;
    condition_variable _toPullerCV;

    mutex _readThreadMtx; 

    mutex _toSenderMtx;
    condition_variable _toSenderCV;

    vector<unsigned int> getChildrenIndices(int, unsigned int);
    unsigned int getID(int k) const;
    unsigned int PPRnextIP(int id, unsigned int  ecK) const;

  public : 
    void doProcess();
    PPRPullDRWorker(Config* conf) : DRWorker(conf){;};
};

#endif //_PPR_PULL_DR_WORKER_HH_

