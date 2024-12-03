#ifndef _CONV_COORDINATOR_HH_
#define _CONV_COORDINATOR_HH_

#include "Coordinator.hh"

using namespace std;

/**
 * Coordinator implementation for ECPipe
 */
class ConvCoordinator : public Coordinator {
    // override
    void requestHandler();
    void addReqInfo(char* drCmd, int* cmdbase, unsigned int failedIP, pair<string, vector<pair<unsigned int, string>>>& info);
  public:
    // just init redis contexts
    ConvCoordinator(Config* c) : Coordinator(c){;};
};

#endif //_CONV_COORDINATOR_HH_

