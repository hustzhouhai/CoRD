#ifndef _METADATA_BASE_HH_
#define _METADATA_BASE_HH_

#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <streambuf>
#include <vector>
#include <unordered_map>

#include <dirent.h>

#include "Config.hh"
#include "RSUtil.hh"
#include "Util/hiredis.h"

#define METADATA_BASE_DEBUG false
#define MAX_ORDER 0x1000000

using namespace std;

class MetadataBase {
  protected: 
    Config* _conf;
    RSUtil* _rsUtil;
    map<unsigned int, unsigned int> _order; 
  public:
    MetadataBase(Config* conf, RSUtil* rsu) :
      _conf(conf),
      _rsUtil(rsu){};
    ~MetadataBase(){};
    // <ip, blk> pair
    virtual vector<pair<unsigned int, string>> getStripeBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0) = 0;
    virtual map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> getStripeBlksMul(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0) = 0;
    
    virtual map<string, int> getCoefficient(const string& blkName, map<string, int>* status = nullptr) = 0;

    // added by jhli
    virtual unsigned int getIPfromBlk(const string& blkName) = 0;
    virtual set<pair<unsigned int, string>> getAllBlks(const string& blkName) = 0;
};

#endif

