#include <iostream>

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#include "../Computation.hh"

using namespace std;

int main(int argc, char** argv) {
  struct timeval tv1, tv2;
  char* buf1;
  char* buf2;
  gettimeofday(&tv1, NULL);
  for (int i = 0; i < 64; i ++) {
    buf1 = (char*)malloc(1048576);
    buf2 = (char*)malloc(1048576);
    Computation::XORBuffers(buf1, buf2, 1048576);
    free(buf1);
    free(buf2);
  }
  gettimeofday(&tv2, NULL);
  cout << tv1.tv_sec << " s " << tv1.tv_usec << "us" << endl;
  cout << tv2.tv_sec << " s " << tv2.tv_usec << "us" << endl;
  return 0;
}

