#ifndef _PPR_COORDINATOR_HH_
#define _PPR_COORDINATOR_HH_

#include "Coordinator.hh"

using namespace std;

/**
 * ONE COORDINATOR to rule them ALL!!!
 */
class PPRCoordinator : public Coordinator {
    unsigned int PPRnextIP(int, unsigned int) const;
    void requestHandler();

  public:

    PPRCoordinator(Config* c) : Coordinator(c) {;};
};

#endif //_PPR_COORDINATOR_HH_

