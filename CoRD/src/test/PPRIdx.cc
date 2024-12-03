#include <iostream>
#include <vector>

using namespace std;

unsigned int PPRnextIP(int id, unsigned int ecK) {
  id ++;
  return id + (id & (- id)) - 1;
}

unsigned int getID(int k) {
  k ++;
  while ((k & (k - 1)) != 0) k += (k & (-k));
  return -- k;
}

vector<unsigned int> getChildrenIndices(int idx, unsigned int ecK) {
  vector<unsigned int> retVal;
  idx ++;
  int lastOne = 0;
  while ((idx >> lastOne) % 2 == 0) lastOne ++;

  int id = ((idx >> lastOne) - 1 << lastOne);
  for (-- lastOne; lastOne >= 0; lastOne --) {
    id += (1 << lastOne);
    if (id <= ecK) retVal.push_back(id - 1); 
    else {
      vector<unsigned int> temp = getChildrenIndices(id - 1, ecK);
      retVal.insert(retVal.end(), temp.begin(), temp.end());
    }
  }
  return retVal;
}

/**
 * Test functions of the PPR indexing scheme
 */
int main(int argc, char** argv) {
  if (argc < 2) {
    cout << "Usage: " << argv[0] << " [ecK]" << endl;
    return 0;
  }

  int ecK = stoi(argv[1]);
  for (int i = 0; i < ecK; i ++) {
    cout << "parent of id " << i << " is " << PPRnextIP(i, ecK) << endl;
  }
  cout << endl;

  for (int i = 0; i <= ecK; i ++) {
    cout << "children of id " << i << ":";
    if (i == ecK) i = getID(i);
    vector<unsigned int> retVal = getChildrenIndices(i, ecK);
    for (auto it : retVal) cout << " " << it;
    cout << endl;
  }
  return 0;
}

