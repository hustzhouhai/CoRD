#include "RSUtil.hh"

#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>

RSUtil::RSUtil(Config* conf, int* encMat) : _encMat(encMat) {
  _ecN = conf -> _ecN;
  _ecK = conf -> _ecK;
  _ecM = _ecN - _ecK;

     
  _coefficient = (int**)calloc(sizeof(int*), _ecN);

  int* completeEncMat = (int*)calloc(_ecN * _ecK, sizeof(int));
  int* eMat = (int*)calloc(_ecK * _ecK, sizeof(int));
  int* dMat = (int*)calloc(_ecK * _ecK, sizeof(int));
  memcpy((char*)completeEncMat, (char*)_encMat, _ecK * _ecM * sizeof(int));
  

  for (int i = 0; i < _ecK; i ++) {
    completeEncMat[(_ecM + i) * _ecK + i] = 1;
  }

  printf("%s: completeEncMat:\n", __func__);
  for (int i = 0; i < _ecN; i ++) {
    for (int j = 0; j < _ecK; j ++) printf("%4d ", completeEncMat[i * _ecK + j]);
    printf("\n");
  }

  for (int i = 0; i < _ecN; i ++) {
    _coefficient[i] = (int*)calloc(sizeof(int), _ecK);
  }
  

  for (int i = 0; i < _ecN; i ++) {
    // copy rows
    for (int j = 0; j < min(i, _ecK); j ++) {
      memcpy((char*)eMat + j * _ecK * sizeof(int), 
          (char*)completeEncMat + j * _ecK * sizeof(int),
          _ecK * sizeof(int));
    }
    for (int j = i + 1; j <= _ecK; j ++) {
      memcpy((char*)eMat + (j - 1) * _ecK * sizeof(int), 
          (char*)completeEncMat + j * _ecK * sizeof(int),
          _ecK * sizeof(int));
    }
    jerasure_invert_matrix(eMat, dMat, _ecK, 8);
    
      _coefficient[i] = jerasure_matrix_multiply(
          completeEncMat + i * _ecK, dMat, 1, _ecK, _ecK, _ecK, 8);
  }
  free(completeEncMat);
  free(eMat);
  free(dMat);
}

void RSUtil::multiply(char* blk, int mulby, int size) {
  //cout << "RSUtil::multiply() " << mulby << endl;
  galois_w08_region_multiply(blk, mulby, size, blk, 0);
}

/**
 * For test only
 */
void RSUtil::multiply(char* blk, char* addto, int mulby, int size) {
  galois_w08_region_multiply(blk, mulby, size, addto, 1);
}

void RSUtil::test() {
  string fileprefix("blkDir/blk_");
  char** data = (char**)calloc(5, sizeof(char*));
  int blkSize = 1048576;
  for (int i = 0; i < 5; i ++) data[i] = (char*)calloc(blkSize, sizeof(char));
  for (int i = 3; i <= 5; i ++) {
    string filename = fileprefix + to_string(i);
    int fd = open(filename.c_str(), O_RDONLY);
    int retVal = pread(fd, data[i - 1], blkSize, 0);
    close(fd);
  }

  puts("before encode");
  // encode
  struct timeval tv1, tv2;
  for (int i = 0; i < 2; i ++) {
    for (int j = 2; j < 5; j ++) {
      gettimeofday(&tv1, NULL);
      multiply(data[j], data[i], _encMat[i * 3 + j - 2], blkSize);
      gettimeofday(&tv2, NULL);
      cout << (tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_usec - tv1.tv_usec << endl;
    }
    string filename = fileprefix + to_string(i + 1);
    int fd = open(filename.c_str(), O_WRONLY);
    int retVal = pwrite(fd, data[i], blkSize, 0);
    close(fd);
  }

  // decode
  char* recovered = (char*)calloc(sizeof(char), blkSize);
  for (int i = 0; i < 5; i ++) {
    memset(recovered, 0, blkSize);
    for (int j = 0; j < min(i, 3); j ++) {
      cout << "coefficient: " << _coefficient[i][j] << endl;
      multiply(data[j], recovered, _coefficient[i][j], blkSize);
    }
    for (int j = i + 1; j <= 3; j ++) {
      cout << "coefficient: " << _coefficient[i][j - 1] << endl;
      multiply(data[j], recovered, _coefficient[i][j - 1], blkSize);
    }
    string filename = "blkDir/recovered_" + to_string(i + 1);
    cout << " filename: " << filename << endl;
    int fd = open(filename.c_str(), O_WRONLY);
    int retVal = pwrite(fd, recovered, blkSize, 0);
    close(fd);
  }
  free(recovered);
}


int* RSUtil::getCoefficient_multi(int idx, int* status) {
  int cnt = 0;


  for (int i=0; i<_ecN; i++) {
    if (status[i] && idx == i) {
      return nullptr;
    }
    if (status[i]) cnt++;
  }
  if (cnt != _ecK) {
    return nullptr;
  }

  cnt = 0;

  int* coefficient = (int*)calloc(_ecK, sizeof(int));

  int* completeEncMat = (int*)calloc(_ecN * _ecK, sizeof(int));
  int* eMat = (int*)calloc(_ecK * _ecK, sizeof(int));
  int* dMat = (int*)calloc(_ecK * _ecK, sizeof(int));
  memcpy((char*)completeEncMat, (char*)_encMat, _ecK * _ecM * sizeof(int));

  for (int i = 0; i < _ecK; i ++) {
    completeEncMat[(_ecM + i) * _ecK + i] = 1;
  }

  // select rows

  for (int j = 0; j < _ecN; j ++) {
    if (status[j]) {
      memcpy((char*)eMat + cnt * _ecK * sizeof(int), 
          (char*)completeEncMat + j * _ecK * sizeof(int),
          _ecK * sizeof(int));

      cnt++;
    }
  }

  jerasure_invert_matrix(eMat, dMat, _ecK, 8);

  coefficient = jerasure_matrix_multiply(
      completeEncMat + idx * _ecK, dMat, 1, _ecK, _ecK, _ecK, 8);

  // select rows end

  free(completeEncMat);
  free(eMat);
  free(dMat);

  return coefficient;
}
