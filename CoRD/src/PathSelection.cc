#include "PathSelection.hh"

#include <iostream>
#include <map>
#include <set>

#define MAX_VALUE 1000000

double PathSelection::bruteForce(vector<vector<double>>& mat) {
  double retVal = 100;
  int ecK = mat.size();
  vector<int> order;
  for (int i = 0; i < ecK; i ++) order.push_back(i);
  do {
    //if (order.back() < order[0]) continue;
    double temp = 0;
    for (int i = 0; i < ecK - 1; i ++) temp += mat[order[i]][order[i + 1]];
    temp += mat[order[ecK - 1]][order[0]];
    if (temp < retVal) {
      retVal = temp;
    }
  } while (next_permutation(order.begin(), order.end()));
  return retVal;
}

double PathSelection::growing(vector<vector<double>>& mat) {
  double retVal = 0;
  int ecK = mat.size();
  vector<double> distance =  vector<double>(ecK);
  vector<int> parent = vector<int>(ecK);
  for (int i = 1; i < ecK; i ++) distance[i] = mat[0][i];
  for (int i = 1; i < ecK; i ++) parent[i] = 0;
  distance[0] = -1;
  int head = 0, tail = 0;

  for (int i = 1; i < ecK; i ++) {
    int marker = 0;
    double minDis = 100;
    for (int j = 1; j < ecK; j ++) {
      if (distance[j] > 0 && distance[j] < minDis) {
        marker = j;
        minDis = distance[j];
      }
    }
    retVal += minDis;

    // extend path
    if (head == parent[marker]) head = marker;
    else tail = marker;
    //cout << "marker: " << marker << " parent: " << parent[marker] << " head: " << head << " tail: " << tail << " " << retVal << endl;
    distance[marker] = -1;

    for (int j = 1; j < ecK; j ++) {
      if (distance[j] > 0) {
        if (mat[head][j] > mat[tail][j]) {
          distance[j] = mat[tail][j];
          parent[j] = tail;
        } else {
          distance[j] = mat[head][j];
          parent[j] = head;
        }
      }
    }
  }
  return retVal;
}

double PathSelection::connecting(vector<vector<double>>& mat) {
  double retVal = 0;
  int ecK = mat.size();
  multimap<double, pair<int, int>> edges;
  vector<int> state(ecK, 0);
  vector<int> nextEnd(ecK);
  for (int i = 0; i < ecK; i ++) {
    nextEnd[i] = i;
    for (int j = i + 1; j < ecK; j ++) {
      edges.insert({mat[i][j], {i, j}});
    }
  }
  int edgeCnt = 0, aEnd, bEnd;
  for (auto it : edges) {
    if (state[it.second.first] < 2 && state[it.second.second] < 2 &&
        nextEnd[it.second.first] != it.second.second) {
      aEnd = nextEnd[it.second.first];
      bEnd = nextEnd[it.second.second];
      nextEnd[aEnd] = bEnd;
      nextEnd[bEnd] = aEnd;
      retVal += it.first;
      state[it.second.first] ++;
      state[it.second.second] ++;
      if (++ edgeCnt == ecK - 1) break;
    }
  }
  //for (auto it : state) cout << it << " ";
  //cout << endl;
  return retVal;
}

double PathSelection::lHop(vector<vector<double>>& mat, int start, int hop, bool mode) {
  double retVal = 0;
  int ecK = mat.size(), head, tail;

  vector<int> candi, temp;
  head = tail = start;
  for (int i = 0; i < start; i ++) candi.push_back(i);
  for (int i = start + 1; i < ecK; i ++) candi.push_back(i);

  for (int i = 1; i < ecK + hop - 1; i += hop) {
    //cout << "i = " << i << endl;
    double minCost = 100;
    sort(candi.begin(), candi.end());
    int hCnt; 
    if (candi.size() % hop != 0) {
      hCnt = candi.size() % hop;
    } else hCnt = min(hop, (int)candi.size());
    bool toHead;
    do {
      double fromHead = mat[head][candi[0]], fromTail = mat[tail][candi[0]];
      for (int j = 1; j < hCnt; j ++) {
        fromHead += mat[candi[j - 1]][candi[j]];
        fromTail += mat[candi[j - 1]][candi[j]];
      }
      if (hCnt == candi.size()) {
        fromHead += mat[candi[hCnt - 1]][tail];
        fromTail += mat[candi[hCnt - 1]][head];
      }

      //if (min(fromTail, fromHead) < minCost) cout << "min val: " << min(fromHead, fromTail) << endl;

      if (fromHead <= fromTail && fromHead < minCost) {
        minCost = fromHead;
        temp.clear();
        temp.insert(temp.begin(), candi.begin(), candi.begin() + hCnt);
        toHead = true;
      } else if (fromTail < fromHead && fromTail < minCost) {
        minCost = fromTail;
        temp.clear();
        temp.insert(temp.begin(), candi.begin(), candi.begin() + hCnt);
        toHead = false;
      }
    } while (nextP(candi, hop));
    retVal += minCost;
    //cout << "retVal = " << retVal << endl;

    if (toHead) head = temp.back();
    else tail = temp.back();
    sort(candi.begin(), candi.end());
    for (auto it : temp) candi.erase(lower_bound(candi.begin(), candi.end(), it));
  }
  //cout << " start: " << start << " retVal" << retVal << endl;

  return retVal;
}
    
double PathSelection::initVal(vector<vector<double>>& mat, int k) {
  double retVal, minEdge = MAX_VALUE, tempMin;
  int endPoint1 = -1, endPoint2 = -1, ecN = mat.size(), pathLen = 2, i, j, newNode;
  bool conn2one;
  vector<bool> used = vector<bool>(ecN, false);

  endPoint1 = 0;
  for (int i = 1; i < ecN - 1; i ++) {
    if (minEdge > mat[0][i]) {
      minEdge = mat[0][i];
      endPoint2 = i;
    }
  }
  _path.push_back(0);
  _path.push_back(endPoint2);

  used[endPoint1] = used[endPoint2] = true;
  retVal = minEdge;

  while (++ pathLen <= k + 1) {
    tempMin = MAX_VALUE;
    for (i = 0; i < ecN; i ++) {
      if (!used[i] && (mat[endPoint2][i] < tempMin)) {
        tempMin = min(tempMin, mat[endPoint2][i]);
        newNode = i;
      }
    }
    retVal = max(retVal, tempMin);
    endPoint2 = newNode;
    _path.push_back(newNode);
    used[newNode] = true;
  }
  return retVal;
}

double PathSelection::dumbSearch(vector<vector<double>>& mat, int k) {
  double retVal = 0;
  for (int i = 0; i < k; i ++) {
    retVal = max(retVal, mat[i][i + 1]);
  }
  return retVal;
}

double PathSelection::intelligentSearch(vector<vector<double>>& mat, int k) {
  vector<int> selected, remaining;
  selected.push_back(0);
  for (int i = 1; i < mat.size(); i ++) remaining.push_back(i);
  _globalMax = initVal(mat, k);
  _path.clear();

  vector<bool> used(mat.size(), false);
  used[0] = true;
  ISHelper(mat, selected, remaining, used, k, 0, 1);

  if (PATH_SELECTION_DEBUG) {
    cout << "path len: " << _path.size() << endl;
    cout << "minmax cost: " << _globalMax << endl << "path: ";
    for (int i = k; i >= 0; i --) {
      cout << _path[i] << " ";
    }

    cout << endl << "weight: " << endl;
    for (int i = k - 1; i >= 0; i --) {
      cout << mat[_path[i]][_path[i + 1]] << " ";
    }
    cout << endl;
  }
  return _globalMax;
}

void PathSelection::ISHelper(vector<vector<double>>& mat, 
    vector<int>& selected, vector<int>& remaining, 
    vector<bool>& used,
    int k, double currMax, int currLen) {
  if (selected.size() == k + 1) {
    _globalMax = min(_globalMax, currMax);
    _path = selected;
    cout << "_globalMax = " << _globalMax << endl;
    for (auto it : selected) cout << it << " ";
    cout << endl;
    return;
  }
  int back, poped;
  for (int i = 0; i < used.size(); i ++) {
    if (used[i] || mat[(back = selected.back())][i] >= _globalMax) continue; 
    selected.push_back(i);
    used[i] = true;

    ISHelper(mat, selected, remaining, used, k, max(currMax, mat[back][i]), currLen + 1);

    selected.pop_back();
    used[i] = false;
  }
}

bool PathSelection::nextP(vector<int>& candi, int k) {
  reverse(candi.begin() + k, candi.end());
  return next_permutation(candi.begin(), candi.end());
}

