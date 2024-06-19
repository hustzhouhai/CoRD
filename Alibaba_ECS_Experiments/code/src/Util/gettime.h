#ifndef _GETTIME_HH_
#define _GETTIME_HH_

#include<string>
#include<stdlib.h>

using namespace std;

string getTime(struct timeval tv1, struct timeval tv2);

bool pktOutput(int pktId, int packetCnt, int lbound = -1, int rbound = -1, int intv = -1);

#endif
