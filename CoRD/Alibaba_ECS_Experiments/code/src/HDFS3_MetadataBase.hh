#ifndef  _HDFS3_METADATA_BASE_HH_
#define _HDFS3_METADATA_BASE_HH_
#include <sys/time.h>
#include <iostream>
#include <string>
#include <array>
#include <stdio.h>
#include <sys/types.h>
#include "MetadataBase.hh"
#include <thread>

using namespace std;

class HDFS3_MetadataBase : public MetadataBase{
    private:
	map<string, set<pair<unsigned int, string>>> _blk2Stripe;
	map<string, unsigned int> _blkIdInStripe;
	map<string, map<string, int>> _coefficient;

	// added by jhli
	map<string, unsigned int> _blkName2Ip;
	//map<unsigned int, string> _Ip2BlkName;
	//map<unsigned int, string> _Id2BlkName;
        map<string, unsigned int> _blkName2StripeId;
        vector<map<unsigned int, string>> _Ip2BlkNames;
        vector<map<unsigned int, string>> _Id2BlkNames;

	size_t ecN;
	size_t ecK;

	thread _metadataThread;
	void GetMetadata();
	unsigned int str2Ip(const string &strIP);
    public:
        HDFS3_MetadataBase(Config* conf, RSUtil* rsu);
        vector<pair<unsigned int, string>> getStripeBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0);
        map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> getStripeBlksMul(const string& blkName, unsigned int requestorIP, map<string, int>* status = nullptr, int opt = 0);
        
        map<string, int> getCoefficient(const string& blkName, map<string, int>* status = nullptr);

	unsigned int getIPfromBlk(const string& blkName);
        set<pair<unsigned int, string>> getAllBlks(const string& blkName);
};
#endif 
