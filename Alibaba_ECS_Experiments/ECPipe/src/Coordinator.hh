#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <thread>
#include <unordered_map>

#include <arpa/inet.h>
#include <hiredis/hiredis.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "Config.hh"
#include "MetadataBase.hh"
#include "HDFS_MetadataBase.hh"
#include "PathSelection.hh"
#include "QFS_MetadataBase.hh"
#include "RSUtil.hh"
#include "Util/gettime.h"

// add by Zuoru: hadoop3
#include "HDFS3_MetadataBase.hh"
///

// add by jhli
#include "DROpt.hh"

#define COORDINATOR_DEBUG true 
#define FILENAME_MAX_LENGTH 256

using namespace std;

/**
 * ONE COORDINATOR to rule them ALL!!!
 */
class Coordinator {
  protected:
    Config* _conf;
    RSUtil* _rsUtil;
    size_t _handlerThreadNum;
    size_t _distributorThreadNum;
    vector<thread> _distThrds;
    vector<thread> _handlerThrds;

    int _slaveCnt;
    int _ecK;
    int _ecN;
    int _ecM;
    int _coefficient;
    int* _rsEncMat;

    string _rsConfigFile;

    MetadataBase* _metadataBase;
    
    /** 
     * mutex and conditional variables for intercommunication between handler and distributor
     */
    // <ip, <redisList of cmdDistributor, redisContext>>
    vector<pair<unsigned int, pair<string, redisContext*>>> _ip2Ctx;
    redisContext* _selfCtx;
   
    map<unsigned int, int> _ip2idx;
    vector<vector<double>> _linkWeight;
    PathSelection* _pathSelection;
 
    void init();
    int searchCtx(vector<pair<unsigned int, pair<string, redisContext*>>>&, unsigned int, size_t, size_t);
    redisContext* initCtx(unsigned int);

    void cmdDistributor(int id, int total);

    virtual void requestHandler();
    string ip2Str(unsigned int) const;

    int isRequestorHolder(vector<pair<unsigned int, string>>&, unsigned int) const;

    // added by jhli
    map<string, int> _status;
    int outputStripeMul(const map<unsigned int, pair<string, vector<pair<unsigned int, string>>>>& stripe_mul, map<unsigned int, int>& numOfJobs);
    int parseFRRequest(const char* reqStr, int reqLen, unsigned int requestorIP, string& reqFile,  vector<pair<string, int>>& lostFiles); 
    int setupCyclReply(vector<pair<string, int>>& lostFiles, 
	map<unsigned int, pair<string, vector<pair<unsigned int, string>>>>& stripes, 
	char** drCmd, int* drCmdLen);
    int setupCyclReply(unsigned int reqIP, 
	vector<pair<unsigned int, string>>& stripe, 
	char** drCmd, int* drCmdLen); 

  public:
    // just init redis contexts
    Coordinator(Config*);
    virtual void doProcess();
};

#endif //_DR_COORDINATOR_HH_

