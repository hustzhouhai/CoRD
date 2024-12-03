#ifndef _QFS_METADATA_BASE_HH_
#define _QFS_METADATA_BASE_HH_

#include <arpa/inet.h>
#include <condition_variable>
#include <thread>
#include <set>
#include <list>
#include "MetadataBase.hh"

using namespace std;

class QFS_MetadataBase : public MetadataBase{
  private:
    map<string, list<pair<unsigned int, string>>> _blk2Stripe; //list
    map<string, unsigned int> _blkIdInStripe;
    map<string, map<string, int>> _coefficient;

    // added for multiple failure. 
    map<string, unsigned int> _blkName2Ip;
    map<string, unsigned int> _blkName2StripeId;
    vector<map<unsigned int, string>> _Ip2BlkNames;
    vector<map<unsigned int, string>> _Id2BlkNames;

    condition_variable _blkLock;
    mutex _blkLock_m;
    thread _metadataThread;
    unsigned int _locIP;
    void ProcessMetadata();
  public:
    QFS_MetadataBase(Config* conf, RSUtil* rsu);
    ~QFS_MetadataBase(){};
    // <ip, blk> pair
    vector<pair<unsigned int, string>> getStripeBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0);
    map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> getStripeBlksMul(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0);
    map<string, int> getCoefficient(const string& blkName, map<string, int>* status = nullptr);

    unsigned int getIPfromBlk(const string& blkName);
    set<pair<unsigned int, string>> getAllBlks(const string& blkName);
};

#endif

