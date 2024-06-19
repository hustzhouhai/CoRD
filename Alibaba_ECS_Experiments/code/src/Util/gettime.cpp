
#include "gettime.h"

string getTime(struct timeval tv1, struct timeval tv2) {
  char s[40];
  sprintf(s, "%.6lf s", ((tv2.tv_sec - tv1.tv_sec)*1000000.0 + tv2.tv_usec - tv1.tv_usec) / 1000000.0);
  return string(s);
}

bool pktOutput(int pktId, int packetCnt, int lbound, int rbound, int intv) {
  if (lbound < 0) lbound = 0;
  if (rbound < 0) rbound = packetCnt - 1;
  if (intv <= 0) intv = 4;
  return pktId <= lbound || pktId >= rbound || pktId % (packetCnt / intv) == 0;
}
