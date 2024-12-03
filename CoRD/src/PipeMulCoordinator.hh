#ifndef _PIPE_MUL_COORDINATOR_HH_
#define _PIPE_MUL_COORDINATOR_HH_

#include "Coordinator.hh"

/* 
 * PIPE_MUL_OPT == 0: Single failure
 * PIPE_MUL_OPT > 0: Multiple failure
 *
 */

using namespace std;

class PipeMulCoordinator : public Coordinator {
    // override
    void requestHandler();

  public:
    // just init redis contexts
    PipeMulCoordinator(Config* c);
    void doProcess();
};

#endif //_PIPE_MUL_COORDINATOR_HH_

