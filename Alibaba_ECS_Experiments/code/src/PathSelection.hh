#ifndef _PATH_SELECTION_HH_
#define _PATH_SELECTION_HH_

#include <algorithm>
#include <deque>
#include <vector>

#define PATH_SELECTION_DEBUG true

using namespace std;

class PathSelection {
    double _globalMax;
    vector<int> _path;
    bool nextP(vector<int>& candi, int k);
  public:
    PathSelection() {};
    double bruteForce(vector<vector<double>>& mat);
    double growing(vector<vector<double>>& mat);
    double connecting(vector<vector<double>>& mat);

    double lHop(vector<vector<double>>& mat, int l, int start, bool mode);

    double initVal(vector<vector<double>>& mat, int k);
    double intelligentSearch(vector<vector<double>>& mat, int k);
    void ISHelper(vector<vector<double>>& mat, vector<int>& selected, vector<int>& remaining, vector<bool>& used, int k, double currMax, int currLen);

    double dumbSearch(vector<vector<double>>& mat, int k);

    vector<int> getPath() {return _path;};
};

#endif //_PATH_SELECTION_HH_
