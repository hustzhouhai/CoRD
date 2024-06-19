#ifndef _CONV_DRWORKER_HH_ 
#define _CONV_DRWORKER_HH_ 

#include "DRWorker.hh"

class ConvDRWorker : public DRWorker 
{
    struct ConvMulJob 
    {
      ConvMulJob(int ecK, unsigned int nextIP, int* coefficients, string lostBlkName) //, string localBlkName)
                : ecK(ecK), nextIP(nextIP), lostBlkName(lostBlkName)/*, localBlkName(localBlkName) */ 
      {
        for (int i = 0; i < ecK; i++) 
        {
          coefs.push_back(coefficients[i]); 
        }
      }
      
      unsigned int nextIP;
      int ecK;
      vector<unsigned int> coefs;
      string lostBlkName;
      string localBlkName;
    };

    unsigned int _requestor; // 0: helper; 1: requestor;
    unsigned int* _ips;
    unsigned int* _waitingToPullerNum;
    mutex _waitingToPullerMtx;

    void readThread(redisContext* rc, int id, int totNum, const string lostBlkName);

    char*** _diskPkts3; 
    
    void readWorker(const string&);
    void sendWorker(const char*, redisContext* rc1);

    void fetchMulWorker(const vector<ConvMulJob>& jobs) ;
    void requestorMulCompletion(const vector<ConvMulJob>& jobs);
    void sendMulWorker(const vector<ConvMulJob>& jobs);

    void requestorCompletion(string&, unsigned int[]);
    void nonRequestorCompletion(string&, redisContext*);


    
  public :
    void doProcess();
    ConvDRWorker(Config* conf) : DRWorker(conf) {;};
};

#endif //_CONV_DRWORKER_HH_ 
