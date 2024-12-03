#ifndef _RS_UTIL_HH_
#define _RS_UTIL_HH_

#include <string.h>

#include "Config.hh"

extern "C" {
#include "Util/galois.h"
#include <gf_complete.h>
#include "Util/jerasure.h"
}

using namespace std;

class RSUtil {
    int _ecK;
    int _ecN;
    int _ecM;
    int** _coefficient;
    int* _encMat;
    gf_t _gf;

  public:
    RSUtil(Config* conf, int* encMat);
    int* getCoefficient(int idx) {return _coefficient[idx];};
    static void multiply(char* buf, int mulby, int size);

    // status should be of size _ecN
    int* getCoefficient_multi(int idx, int* status);

    /* for test */
    void multiply(char* buf, char* addto, int mulby, int size);
    void test();
};

#endif //_RS_UTIL_HH_

