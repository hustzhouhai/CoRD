#include "CyclCoordinator.hh"

#include "Util/jerasure.h"

CyclCoordinator::CyclCoordinator(Config* c) : Coordinator(c) {
  // init the matrix
  int* completeEncMat = (int*)calloc(_ecN * _ecK, sizeof(int));
  int* decMat = (int*)calloc(_ecK * _ecK, sizeof(int));
  int* encMat = (int*)calloc(_ecK * _ecK, sizeof(int));
  _preComputedMat = (int***)calloc(sizeof(int**), _ecN);
  int i, j, round;

  memcpy((char*)completeEncMat, (char*)_rsEncMat, _ecK * _ecM * sizeof(int));

  for (int i = 0; i < _ecK; i ++) {
    completeEncMat[(_ecM + i) * _ecK + i] = 1;
  }

  int survivor[_ecN - 1];
  int idx[_ecK];
  int coef[_ecK];
  for (int i = 0; i < _ecN - 1; i ++) survivor[i] = i;
  int survivorId = 3;
  int start = survivorId;
  int failed = _ecN - 1;

  struct timeval tv1, tv2;
  gettimeofday(&tv1, NULL);
  fill_n(coef, _ecK, -1);
  for (round = 0; round < _ecK; round ++) {
    for (i = 0; i < _ecK; i ++) {
      idx[i] = survivor[(start + i) % (_ecN - 1)];
      memcpy((char*)encMat + i * _ecK * sizeof(int), 
          (char*)completeEncMat + idx[i] * _ecK * sizeof(int),
          _ecK * sizeof(int));
    }

    jerasure_invert_matrix(encMat, decMat, _ecK, 8);
    coef[start] = decMat[(failed - _ecM) * _ecK + round];

    start = (start - 2 + _ecN) % (_ecN - 1);
  }
  gettimeofday(&tv2, NULL);
  cout << tv1.tv_sec << "s " << tv1.tv_usec << "us" << endl;
  cout << tv2.tv_sec << "s " << tv2.tv_usec << "us" << endl;

  for (i = 0; i < _ecN - 1; i ++) cout << coef[i] << " ";
  cout << endl;

  // packet start node
  int packetCnt = 9;
  int startSurvivor[packetCnt]; 
  startSurvivor[0]= _ecN - _ecK;
  for (i = 1; i < _ecK - 1; i ++) {
    startSurvivor[i] = startSurvivor[i - 1] + 1;
  }
  for (; i < packetCnt; i ++) {
    startSurvivor[i] = (startSurvivor[i + 1 - _ecK] + _ecK) % (_ecN - 1);
  }
  for (i = 0; i < packetCnt; i ++) cout << startSurvivor[i] << " ";
  cout << endl;

  int count[_ecN - 1];
  fill_n(count, _ecN - 1, 0);
  for (i = 0; i < packetCnt; i ++) {
    for (j = 0; j < _ecK; j ++) {
      count[(startSurvivor[i] + j) % (_ecN - 1)] ++;
    }
  }
  for (i = 0; i < _ecN - 1; i ++) cout << count[i] << " ";
  cout << endl;

  vector<vector<unsigned int>> readSeq = vector<vector<unsigned int>>(_ecN - 1);
  vector<vector<unsigned int>> sendSeq = vector<vector<unsigned int>>(_ecN - 1);

  for (int groupStart = 0; groupStart < (packetCnt / (_ecK - 1) + 1) * (_ecK - 1); groupStart += (_ecK - 1)) {
    int groupPktCnt = min(_ecK - 1, packetCnt - groupStart);
    for (i = 0; i < _ecK; i ++) {
      if (groupStart != 0 && i != _ecK - 1 && groupStart + 1 - _ecK + i < packetCnt) {
        cout << "send to req: node " << (startSurvivor[groupStart - _ecK + 1] + _ecK - 1 + i) % (_ecN - 1)
          << " pkt id: " << groupStart + 1 - _ecK + i << endl;
        sendSeq[(startSurvivor[groupStart - _ecK + 1] + _ecK - 1 + i) % (_ecN - 1)].push_back(groupStart + 1 - _ecK + i);
      }
      for (j = 0; j < groupPktCnt; j ++) {
        if (groupStart + j < packetCnt) {
          readSeq[(startSurvivor[groupStart + j] + i) % (_ecN - 1)].push_back(groupStart + j);
          if (i != _ecK - 1) {
            sendSeq[(startSurvivor[groupStart + j] + i) % (_ecN - 1)].push_back(groupStart + j);
          }
        }
      }
    }
  }

  for (int i = 0; i < _ecN - 1; i ++) {
    cout << "node " << i << ":" << endl;
    for (auto it : sendSeq[i]) cout << " " << it;
    cout << endl;
  }

  exit(0);
}

