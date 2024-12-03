#ifndef _PIPE_DRWORKER_HH_ 
#define _PIPE_DRWORKER_HH_ 

#include "DRWorker.hh"

class PipeDRWorker : public DRWorker {
    void checkWorker();

    void readWorker(const string&);
    void sendWorker(const char*, redisContext* rc1);

    void pullRequestorCompletion(string&, redisContext*);
  public :
    void doProcess();
    PipeDRWorker(Config* conf) : DRWorker(conf) {;};
};

#endif //_PIPE_DRWORKER_HH_ 
