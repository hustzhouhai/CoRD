#ifndef _CYCL_MUL_FR_WORKER_HH_
#define _CYCL_MUL_FR_WORKER_HH_

#include <queue>

#include "DRWorker.hh"

using namespace std;

class CyclMulFRWorker : public DRWorker {
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

    int _readCnt;
    mutex _toPullerMtx;
    condition_variable _toPullerCV;

    int _toSendCnt;

    bool _toSendIdsUsing;
    mutex _toSendIdsUsingMtx;
    condition_variable _toSendIdsUsingCV;

    int* _toSendIds;
    mutex _toSenderMtx;
    condition_variable _toSenderCV;
    //queue<int> _toSendIds[50]; // <jobID, pktID>

    char *** _diskPktsMul;

    vector<vector<int>> _pkt2sli;
    vector<map<int, int>> _sli2pkt;

    // new interface
    void readerMul(const vector<PipeMulJob>& jobs);
    void senderMul(const vector<PipeMulJob>& jobs);
    void pullerMul(const vector<PipeMulJob>& jobs);

  public : 
    void doProcess();
    CyclMulFRWorker(Config* conf) : DRWorker(conf){;};
};

#endif 
