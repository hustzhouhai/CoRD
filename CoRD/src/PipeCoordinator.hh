#ifndef _PIPE_COORDINATOR_HH_
#define _PIPE_COORDINATOR_HH_

#include "Coordinator.hh"

using namespace std;

/**
 * Coordinator implementation for Pipe
 */
class PipeCoordinator : public Coordinator {

    void requestHandler();

    map<string, int> _status;
    map<int, string> _md5;
  public:
    // just init redis contexts
    PipeCoordinator(Config* c);
    void doProcess();
};

#endif //_PIPE_COORDINATOR_HH_

