#ifndef _CONFIG_HH_
#define _CONFIG_HH_

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "Util/tinyxml2.h"

#define COMMAND_MAX_LENGTH 1024 

using namespace tinyxml2;

class Config {
  public:
    size_t _ecK;
    size_t _ecN;
    size_t _packetSize;
    size_t _packetSkipSize = 0;
    size_t _packetCnt;

    bool _pathSelectionEnabled = false;
    std::string _linkWeightConfigFile;

    std::string _rsConfigFile;

    std::vector<unsigned int> _helpersIPs;
    unsigned int _coordinatorIP;
    unsigned int _localIP;

    /**
     * Rack-Aware related variables
     */

    std::map<unsigned int, std::string> _rackInfos;
    bool _shuffle = true;
    bool _rackAware = false;
    bool _loadbalance = false;
    bool _msscheduling = false;

    std::string _fileSysType;
    /**
     * HDFS related variables
     */
    std::string _stripeStore;
    std::string _hdfsHome;
    std::string _blkDir;

    std::string _DRPolicy;
    std::string _ECPipePolicy;

    // add by zhouhai
    std::string _UPPolicy;  // raid delta crd
    bool _flipOpt = false;
    int _log_size;  // MB
    std::string _UPWay; // random or trace
    int _blocksize;
    std::string _updDataDir;
    std::string _traceDir;
    std::string _traceType; // MSR AliCloud TenCloud


    /**
     * Thread nums
     */
    size_t _coCmdDistThreadNum = 1; // command distributor thread number in coordinator
    size_t _coCmdReqHandlerThreadNum = 1; // request handler thread number in coordinator

    size_t _agWorkerThreadNum = 1; // how many degraded read an helper can participate simultanously 一个助手可以同时参与多少降级读取

    Config(std::string confFile);
    void display();
};

#endif
