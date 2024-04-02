#ifndef _CYCL_COORDINATOR_HH_
#define _CYCL_COORDINATOR_HH_

#include "Coordinator.hh"

using namespace std;

/**
 * Coordinator implementation for Cyclic ECPipe (extended version)
 */
class CyclCoordinator : public Coordinator {
    
    int*** _preComputedMat; 
    // override
    void requestHandler() {;};
  public:
    // init redis contexts and pre-compute the recovery matrix
    CyclCoordinator(Config* c);
};

#endif //_CYCL_COORDINATOR_HH_

