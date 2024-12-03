#ifndef _HDFS_METADATA_BASE_HH_
#define _HDFS_METADATA_BASE_HH_
#include "MetadataBase.hh"
#include <algorithm>
using namespace std;

class HDFS_MetadataBase : public MetadataBase{
  private: 
    map<string, set<pair<unsigned int, string>>> _blk2Stripe;
    map<string, unsigned int> _blkIdInStripe;
    map<string, map<string, int>> _coefficient;

    map<string, unsigned int> _blkName2Ip;
    map<string, unsigned int> _blkName2StripeId;
    vector<map<unsigned int, string>> _Ip2BlkNames;
    vector<map<unsigned int, string>> _Id2BlkNames;

    // added by Zuoru: for rack-aware
    map<string, set<string>> _blk2All;
    bool _rackenable;
    bool _loadbalance;
  public:
    HDFS_MetadataBase(Config* conf, RSUtil* rsu);
    // <ip, blk> pair
    vector<pair<unsigned int, string>> getStripeBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0);
    map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> getStripeBlksMul(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0);

    // status == nullptr: single failure
    map<string, int> getCoefficient(const string& blkName, map<string, int>* status = nullptr);

    unsigned int getIPfromBlk(const string& blk);
    set<pair<unsigned int, string>> getAllBlks(const string& blkName);
    set<pair<unsigned int, string>> getScheduledBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status, set<pair<unsigned int, string>>& allStripeBlks); 
    set<pair<unsigned int, string>> getRandomBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status, set<pair<unsigned int, string>>& allStripeBlks); 
};

#endif


