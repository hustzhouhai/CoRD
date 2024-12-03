#ifndef _PIPE_MUL_DRWORKER_HH_ 
#define _PIPE_MUL_DRWORKER_HH_ 

#include "DRWorker.hh"

#define MAX_ECK 30

class PipeMulDRWorker : public DRWorker {
    struct PipeMulJob {
      PipeMulJob(int ecK, unsigned int reqIP, unsigned int prevIP, unsigned int nextIP, int id, 
        int coefficient, string lostBlkName, string localBlkName)
        : ecK(ecK), reqIP(reqIP), prevIP(prevIP), nextIP(nextIP), id(id), coefficient(coefficient),
          lostBlkName(lostBlkName), localBlkName(localBlkName) {
            if (ecK == 0x010000 || ((ecK & 0x100) == 0 && id == ecK - 1)) 
              sendKey = lostBlkName;
            else sendKey = "tmp:" + lostBlkName;
          }
      unsigned int reqIP, prevIP, nextIP;
      int ecK, id, coefficient;
      string lostBlkName;
      string localBlkName;
      string sendKey;
    };

    char** _sendMulPkt;

    unsigned int waitingToSend[MAX_ECK];
    mutex senderMtx[MAX_ECK];
    condition_variable senderCondVar[MAX_ECK];

    void readWorker(const string&);
    void sendWorker(const char*, redisContext* rc1);
    void pullRequestorCompletion(string&, redisContext*);

    void readMulWorker(const vector<PipeMulJob>& jobs);
    void sendMulWorker(const vector<PipeMulJob>& jobs);
    void pullRequestorMulCompletion(const vector<PipeMulJob>& jobs);




  public :
    void doProcess();
    PipeMulDRWorker(Config* conf) : DRWorker(conf) {;};
};

#endif //_PIPE_MUL_DRWORKER_HH_ 
