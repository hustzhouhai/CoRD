#ifndef _UP_COORDINATOR_HH_
#define _UP_COORDINATOR_HH_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <thread>
#include <unordered_map>
#include <list>
#include <sstream>
#include <dirent.h>

#include <arpa/inet.h>
#include <hiredis/hiredis.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "Config.hh"
#include "RSUtil.hh"
#include "Util/gettime.h"
#include "UpdBlock.hh"

extern "C"{
#include "Util/jerasure.h"
#include "Util/galois.h"
#include "Util/reed_sol.h"
#include "Util/cauchy.h"
}

#define COORDINATOR_DEBUG false 
#define FILENAME_MAX_LENGTH 256

using namespace std;


class UPCoordinator 
{
    public:
        Config* _conf;
        redisContext* _selfCtx;
        map<unsigned int, redisContext*> _ip2Ctx;
        int _ecK;
        int _ecM;
        int* _rsEncMat;
        thread _UpdReqThr;
        thread _CmdDisThr;
        unsigned int _locIP;



        void init();
        UPCoordinator(Config*);
        redisContext* initCtx(unsigned int redisIP);
        string ip2Str(unsigned int) const;
        void doProcess();
        void CmdDistributor();
        void UpdateReqHandler();
        void RaidUpdate(vector<UpdBlock>&);
        void DeltaUpdate(vector<UpdBlock>&);
        void CrdUpdate(vector<UpdBlock>&);
        bool is_intersected(UpdBlock& d, vector<UpdBlock>& N);
        map<int, vector<UpdBlock> > get_upd_solu_crd(vector<UpdBlock> &upd_blk_vec);
        int get_sub_set_id(map<int, vector<UpdBlock> > upd_solu, int loc_blk_id);
        int recvAck();
        vector<string> splite_string(string str, char c);
        long long get_trace_req_num(string tracefile);
        void debug(Config* _conf);
        void trigger_update(map<int, vector<int> >&, map<int, vector<int> >&);
        void trigger_update_by_multi_doProcess(map<int, vector<int> >&, map<int, vector<int> >&, string);
        void single_doProcess();
        void multi_doProcess();
        bool is_computed(string tracefile);

};

#endif 
