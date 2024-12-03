#ifndef _UP_NODE_HH_
#define _UP_NODE_HH_

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
#include "RSUtil.hh"
#include "Util/gettime.h"

extern "C"{
#include "Util/jerasure.h"
#include "Util/galois.h"
#include "Util/reed_sol.h"
#include "Util/cauchy.h"
}

#define NODE_DEBUG false 
#define FILENAME_MAX_LENGTH 256

using namespace std;


class UPNode 
{
    public:
        Config* _conf;
        redisContext* _selfCtx;
        redisContext* _coordinatorCtx;
        map<unsigned int, redisContext*> _ip2Ctx;
        int _ecK;
        int _ecM;
        //thread _UpdReqThr;
        //thread _CmdDisThr;
        unsigned int _locIP;



        void init();
        UPNode(Config*);
        redisContext* initCtx(unsigned int redisIP);
        string ip2Str(unsigned int) const;
        void doProcess();
        void send_data_to_collector_for_raid(char*, int);
        void recv_and_compute_and_send_for_raid(char*, int);
        void recv_and_save_for_raid(char*, int);
        void send_parity_delta_to_each_parity_for_delta(char*, int);
        void recv_parity_delta_and_aggre_for_delta(char*, int);
        void send_data_delta_to_parity_collector_for_crd(char*, int);
        void recv_and_compute_in_parity_collector_for_crd(char*, int);
        void recv_parity_delta_and_aggre_for_crd(char*, int);
        void readDataFromDisk(string localBlkName, char* content, int size, int base);
        void writeDataToDisk(string fileName, char* content, int size, int base);
        void gene_radm_buff(char* buff, int len);
        void compute_delta(char* result, char* srcA, char* srcB, int length);
        void debug();

};

#endif 
