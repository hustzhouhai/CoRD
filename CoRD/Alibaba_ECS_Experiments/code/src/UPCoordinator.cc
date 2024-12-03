#include "UPCoordinator.hh"

UPCoordinator::UPCoordinator(Config* conf) 
{
    _conf = conf;
    _locIP = _conf -> _localIP;
    init();
}

void UPCoordinator::init() 
{
    _ecK = _conf -> _ecK;
    _ecM = _conf -> _ecN - _ecK;

    _rsEncMat = reed_sol_vandermonde_coding_matrix(_ecK, _ecM, 8);  // m*k

    if (COORDINATOR_DEBUG)
    {
        cout << "_rsEncMat =" << endl;

        for (int i=0; i<_ecM; i++)
        {
            for (int j=0; j<_ecK; j++)
            {
                cout << _rsEncMat[i*_ecK+j] << " ";
            }
            cout << endl;
        }
    }

    for (auto& it : _conf -> _helpersIPs) 
    {
        _ip2Ctx[it] = initCtx(it);
    }
    _selfCtx = initCtx(_locIP);

    
    int count = (_conf -> _helpersIPs).size();
    while(1)
    {
        if (COORDINATOR_DEBUG)
            cout << "waitting for cmd ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop test 100");
        if (rReply -> type == REDIS_REPLY_NIL) 
        {
            cerr << __func__ << " empty queue " << endl;
            freeReplyObject(rReply);
        } 
        else if (rReply -> type == REDIS_REPLY_ERROR) 
        {
            cerr << __func__ << " ERROR happens " << endl;
            freeReplyObject(rReply);
        } 
        else 
        {
            if((int)rReply -> elements == 0) 
            {
                cerr << __func__ << " rReply->elements = 0, ERROR " << endl;
                continue;
            }
            //cout << "recv test from node!" << endl;
            count --;
            freeReplyObject(rReply);  // 如果不释放reply会导致内存泄漏
            if (count == 0)
            {
                cout << "recv " << (_conf -> _helpersIPs).size() << " test from node!" << endl;
                break;
            }
                
        }
    }
    
}

redisContext* UPCoordinator::initCtx(unsigned int redisIP) 
{
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    if (COORDINATOR_DEBUG)
        cout << "initCtx: connect to " << ip2Str(redisIP) << endl;
    redisContext* rContext = redisConnectWithTimeout(ip2Str(redisIP).c_str(), 6379, timeout);
    if (rContext == NULL || rContext -> err) 
    {
        if (rContext) 
        {
            cerr << "Connection error: " << rContext -> errstr << endl;
            redisFree(rContext);
        } 
        else 
        {
            cerr << "Connection error: can't allocate redis context at IP: " << redisIP << endl;
        }
    }
    return rContext;
}

string UPCoordinator::ip2Str(unsigned int ip) const 
{
  string retVal;
  retVal += to_string(ip & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 8) & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 16) & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 24) & 0xff);
  return retVal;
}

void UPCoordinator::doProcess() 
{
    if (_conf -> _UPPolicy != "all")  
        this->single_doProcess();   // run specific update scheme
    else        
        this->multi_doProcess();    // run three update schemes
}

void UPCoordinator::UpdateReqHandler()
{
    int blocksize = _conf -> _blocksize;
    int ecK = _conf -> _ecK;
    redisReply* rReply;
    char upd_cmd[COMMAND_MAX_LENGTH * COMMAND_MAX_LENGTH];
    int len = 4;
    int cur = 0;

    if (_conf -> _UPWay == "random")  // test for single stripe update, but later process use multi-stripe
    {
        cur = 0;
        int upd_stripe_num = 1;
        int upd_num = ecK;

        memcpy(upd_cmd + cur, (char*)&upd_stripe_num, len);
        cur += len;

        memcpy(upd_cmd + cur, (char*)&upd_num, len);
        cur += len;

        for (int i=0; i<ecK; i++)
        {
            int in_id = i;
            memcpy(upd_cmd + cur, (char*)&in_id, len);
            cur += len;

            int offset_add = rand() % blocksize;//[0, blocksize)
            int upd_size = (rand() % (blocksize - offset_add - 1)) + 1;   //[1, blocksize - offset_add)

            memcpy(upd_cmd + cur, (char*)&offset_add, len);
            cur += len;

            memcpy(upd_cmd + cur, (char*)&upd_size, len);
            cur += len;
        }

        redisAppendCommand(_selfCtx, "RPUSH upd_requests %b", upd_cmd, cur);
        redisGetReply(_selfCtx, (void**)&rReply);  // send "upd_requests" to cmdDistributor()
        freeReplyObject(rReply);

        cout << "coor send upd_requests to local!" << endl;

    }
    else if (_conf -> _UPWay == "trace")  // test for multi-stripe update for log trigger
    {
        string tracefile = _conf->_traceDir + "/" + _conf->_traceType + "/rsrch_2.csv";

        ifstream fp(tracefile);

        if (!fp)
        {
            cout << "open trace file failed : " << tracefile << endl;
            exit(-1); 
        }

        long long req_num =  400; //get_trace_req_num(tracefile);

        long long count = 0;

        string line;
        
        char c = ',';
        
        vector<string> res;
        
        long long offset, update_size, start_blk_id, end_blk_id, stripe_id, glo_blk_id, loc_blk_id;

        stringstream stream;

        int blocksize = _conf->_blocksize;

        map<int, vector<int> > stripe_id2loc_blk_id; // <stripe id, <local blk id vec> >
        map<int, vector<int> > glo_blk_id2start_end; // <global blk_id, <start_add, end_size> >
        
        while (getline(fp,line))
        {
            count ++;

            res = splite_string(line, c);

            if ((_conf->_traceType == "MSR" && res[3] == "Read") || (_conf->_traceType == "Ali" && res[1] == "R") || 
            (_conf->_traceType == "Ten" && res[3] == "0"))
                continue;

            if (_conf->_traceType == "MSR")
            {
                stream.clear();  // clear stringstream
                stream.str("");  // free memory
                stream<<res[4];
                stream>>offset;
                stream.clear();
                stream.str("");
                stream<<res[5];
                stream>>update_size;
            }
            else if (_conf->_traceType == "Ali")
            {
                stream.clear();
                stream.str("");
                stream<<res[2];
                stream>>offset;
                stream.clear();
                stream.str("");
                stream<<res[3];
                stream>>update_size;
            }
            else if (_conf->_traceType == "Ten")
            {
                stream.clear();
                stream.str("");
                stream<<res[1];
                stream>>offset;
                offset = offset * 512;
                stream.clear();
                stream.str("");
                stream<<res[2];
                stream>>update_size;
                update_size = update_size * 512;
            }
            else
            {
                cout << "error _conf->_traceType = " << _conf->_traceType << endl;
                exit(-1);
            }

            //cout << offset << " " << update_size << endl;

            offset = offset / 1024;  // Bytes --> KB
            update_size = update_size / 1024;  // Bytes --> KB
            
            if (update_size == 0)
                continue;
            
            start_blk_id = offset / blocksize;
            end_blk_id = (offset + update_size) / blocksize;

            for (int i=start_blk_id; i<=end_blk_id; i++)  // i is global block id
            {
                glo_blk_id = i;
                loc_blk_id = glo_blk_id % ecK;
                stripe_id = glo_blk_id / ecK;

                vector<int> start_end;

                if (glo_blk_id == start_blk_id && glo_blk_id != end_blk_id)
                {
                    start_end.push_back(offset % blocksize);
                    start_end.push_back(blocksize);
                }
                else if (glo_blk_id == start_blk_id && glo_blk_id == end_blk_id)
                {
                    start_end.push_back(offset % blocksize);
                    start_end.push_back((offset + update_size) % blocksize);
                }
                else if (glo_blk_id != start_blk_id && glo_blk_id != end_blk_id)
                {
                    start_end.push_back(0);
                    start_end.push_back(blocksize);
                }
                else if (glo_blk_id != start_blk_id && glo_blk_id == end_blk_id)
                {
                    start_end.push_back(0);
                    start_end.push_back((offset + update_size) % blocksize);
                }

                if (glo_blk_id2start_end.find(glo_blk_id) == glo_blk_id2start_end.end())  // the first insert block
                {
                    glo_blk_id2start_end[glo_blk_id] = start_end;

                    if (stripe_id2loc_blk_id.find(stripe_id) == stripe_id2loc_blk_id.end())
                    {
                        vector<int> v;
                        v.push_back(loc_blk_id);
                        stripe_id2loc_blk_id[stripe_id] = v;
                    }
                    else if (stripe_id2loc_blk_id.find(stripe_id) != stripe_id2loc_blk_id.end() && 
                    find(stripe_id2loc_blk_id[stripe_id].begin(), stripe_id2loc_blk_id[stripe_id].end(), loc_blk_id) == stripe_id2loc_blk_id[stripe_id].end())
                    {
                        stripe_id2loc_blk_id[stripe_id].push_back(loc_blk_id);
                    }
                }
                else  // the global block has been existed
                {
                    glo_blk_id2start_end[glo_blk_id][0] = min(start_end[0], glo_blk_id2start_end[glo_blk_id][0]);
                    glo_blk_id2start_end[glo_blk_id][1] = min(start_end[1], glo_blk_id2start_end[glo_blk_id][1]);
                }
            }

            // check is trigger the update process
            double cur_log_size = glo_blk_id2start_end.size() * blocksize * 1.00 / 1024;  // MB

            if (cur_log_size >= _conf->_log_size || count + 1 >= req_num)
            {
                cur = 0;
                int upd_stripe_num = stripe_id2loc_blk_id.size();
                if (upd_stripe_num == 0)
                    break;
                int upd_num;

                memcpy(upd_cmd + cur, (char*)&upd_stripe_num, len);
                cur += len;

                for (auto it = stripe_id2loc_blk_id.begin(); it != stripe_id2loc_blk_id.end(); it++)
                {
                    stripe_id = it->first;
                    upd_num = (it->second).size();

                    memcpy(upd_cmd + cur, (char*)&upd_num, len);
                    cur += len;

                    for (int i=0; i<upd_num; i++)
                    {
                        loc_blk_id = (it->second)[i];
                        glo_blk_id = stripe_id * ecK + loc_blk_id;

                        memcpy(upd_cmd + cur, (char*)&loc_blk_id, len);
                        cur += len;

                        int offset_add = glo_blk_id2start_end[glo_blk_id][0];//[0, blocksize)
                        int upd_size = glo_blk_id2start_end[glo_blk_id][1] - glo_blk_id2start_end[glo_blk_id][0];   //[1, blocksize]

                        memcpy(upd_cmd + cur, (char*)&offset_add, len);
                        cur += len;

                        memcpy(upd_cmd + cur, (char*)&upd_size, len);
                        cur += len;
                    }
                }

                redisAppendCommand(_selfCtx, "RPUSH upd_requests %b", upd_cmd, cur);
                redisGetReply(_selfCtx, (void**)&rReply);  // send "upd_requests" to cmdDistributor()
                freeReplyObject(rReply);

                if (COORDINATOR_DEBUG)
                    cout << "coor send upd_requests to local!" << endl;

                stripe_id2loc_blk_id.clear();
                glo_blk_id2start_end.clear();

            }

            if (count + 1 >= req_num)
                break;
            
        }

        // merge multi-address-part within a data block 

        // record the flip stripe and flip the address for crd_flip

        // _flipOpt = true or false

        // when the log is full, notify CmdDistributor() to update and go on repaly trace until log full and previous update finish
    }
    else
    {
        cout << "error _conf -> _UPWay = " << _conf -> _UPWay << endl;
        exit(-1);
    }
}

void UPCoordinator::CmdDistributor()
{
    struct timeval timeout = {1, 500000}; // 1.5 seconds
    redisContext* locCtx = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
    if (locCtx == NULL || locCtx -> err) 
    {
        if (locCtx) 
        {
            cerr << "Connection error: " << locCtx -> errstr << endl;
        } 
        else 
        {
            cerr << "Connection error: can't allocate redis context" << endl;
        }
        redisFree(locCtx);
        return;
    }

    int ip;
    int upd_num, upd_stripe_num, total_upd_stripe_num;
    int cur, len;
    char upd_cmd[COMMAND_MAX_LENGTH];
    double total_running_time = 0;

    len = 4;

    total_upd_stripe_num = 0;

    while (1) 
    {
        if (COORDINATOR_DEBUG)
            cout << "waitting for update request ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "BLPOP upd_requests 100");
        if (rReply -> type == REDIS_REPLY_NIL) 
        {
            cerr << "UPCoordinator::CmdDistributor() empty queue " << endl;
            freeReplyObject(rReply);
        } 
        else if (rReply -> type == REDIS_REPLY_ERROR) 
        {
            cerr << "UPCoordinator::CmdDistributor() ERROR happens " << endl;
            freeReplyObject(rReply);
        } 
        else 
        {
            if((int)rReply->elements == 0) 
            {
                cerr << __func__ << " rReply->elements = 0, ERROR " << endl;
                continue;
            }
            if (COORDINATOR_DEBUG)
                cout << "coor recv upd_requests from local!" << endl;

            memcpy((char*)&upd_stripe_num, rReply -> element[1] -> str, 4); 

            cout << "need to update " << upd_stripe_num << " stripes" << endl;

            cur = 4;

            total_upd_stripe_num += upd_stripe_num;

            struct timeval tv1, tv2;

            gettimeofday(&tv1, NULL);

            while (upd_stripe_num > 0)
            {
                memcpy((char*)&upd_num, rReply -> element[1] -> str + cur, len);  
                cur += len;

                memcpy(upd_cmd, rReply -> element[1] -> str + cur, upd_num * (3 * len));
                cur += upd_num * (3 * len);

                int in_cur = 0;

                vector<UpdBlock> upd_blk_vec;
                for (int i=0; i<upd_num; i++)
                {
                    int in_id, offset_add, upd_size;

                    memcpy((char*)&in_id, upd_cmd + in_cur, len);
                    in_cur += len;

                    memcpy((char*)&offset_add, upd_cmd + in_cur, len);
                    in_cur += len;

                    memcpy((char*)&upd_size, upd_cmd + in_cur, len);
                    in_cur += len;

                    UpdBlock updblk(in_id, (_conf -> _helpersIPs)[in_id], offset_add, upd_size);
                    if (COORDINATOR_DEBUG)
                        updblk.showblkInfo();
                    upd_blk_vec.push_back(updblk);
                }

                if (_conf -> _UPPolicy == "raid")
                {
                    this -> RaidUpdate(upd_blk_vec);
                }
                else if (_conf -> _UPPolicy == "delta")
                {
                    this -> DeltaUpdate(upd_blk_vec);
                }
                else if (_conf -> _UPPolicy == "crd")
                {
                    this -> CrdUpdate(upd_blk_vec);
                }
                else
                {
                    cout << "error _conf -> _UPPolicy = " << _conf -> _UPPolicy << endl;
                    exit(-1);
                }

                if (recvAck() == 1)
                {
                    if (COORDINATOR_DEBUG)
                        cout << "a stripe completes updates!" << endl;
                    upd_stripe_num --;
                }
            }

            freeReplyObject(rReply);

            cout << "a round of updates finish!" << endl;

            gettimeofday(&tv2, NULL);

            double running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
            total_running_time += running_time;

            double throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time * 1024 * 1.00);  // MB/s

            cout << _conf->_UPPolicy << " throughput : " << throughput << " MB/s" << endl;

        }
    }

}

// recv ecM node ack
int UPCoordinator::recvAck()
{
    int ecK = _conf->_ecK;
    int ecN = _conf->_ecN;
    int ecM = ecN - ecK;
    int count = ecM;
    while(1)
    {
        if (COORDINATOR_DEBUG)
            cout << "waitting for ack ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop ack 100");
        if (rReply -> type == REDIS_REPLY_NIL) 
        {
            cerr << __func__ << " empty queue " << endl;
            freeReplyObject(rReply);
        } 
        else if (rReply -> type == REDIS_REPLY_ERROR) 
        {
            cerr << __func__ << " ERROR happens " << endl;
            freeReplyObject(rReply);
        } 
        else 
        {
            if((int)rReply -> elements == 0) 
            {
                cerr << __func__ << " rReply->elements = 0, ERROR " << endl;
                continue;
            }
            if (COORDINATOR_DEBUG)
                cout << "recv ack from node!" << endl;
            count --;
            freeReplyObject(rReply);
            if (count == 0)
                break;
        }
    }

    return 1;
}

void UPCoordinator::RaidUpdate(vector<UpdBlock> &upd_blk_vec)
{
    if (COORDINATOR_DEBUG)
        cout << "start " << __func__ << endl;
    int upd_num = upd_blk_vec.size();
    //cout << "upd_num = " << upd_num << endl;
    int rand_collector = rand() % upd_num; // randomly select a collector
    //cout << "rand_collector = " << rand_collector << endl;
    int collector_id = upd_blk_vec[rand_collector]._id; 
    int ecK = _conf -> _ecK;
    int ecM = _conf -> _ecN - ecK;

    char upd_cmd[COMMAND_MAX_LENGTH];
    int cur, len = 4;
    int task_type;

    redisReply* rReply;

    for (int i=0; i<upd_num; i++)
    {
        if (upd_blk_vec[i]._id == collector_id)
            continue;
        
        task_type = 1;

        cur = 0;
        memcpy(upd_cmd + cur, (char*)&task_type, len);
        cur += len;

        // skip init cmd len
        cur += len;

        memcpy(upd_cmd + cur, (char*)&(upd_blk_vec[i]._id), len);
        cur += len;

        memcpy(upd_cmd + cur, (char*)&collector_id, len);
        cur += len;

        // lastly, copy cmd len at second location
        memcpy(upd_cmd + 4, (char*)&cur, len);

        rReply = (redisReply*)redisCommand(_ip2Ctx[upd_blk_vec[i]._ip], "RPUSH upd_cmds %b", upd_cmd, cur); // send "upd_cmds" to node
        freeReplyObject(rReply);

        if (COORDINATOR_DEBUG)
            cout << "coor send upd_cmds to data node ." << to_string((upd_blk_vec[i]._ip >> 24) & 0xff) << endl;
    }

    if (COORDINATOR_DEBUG)
        cout << "collector cmd:" << endl;
    task_type = 2;
    cur = 0;

    memcpy(upd_cmd + cur, (char*)&task_type, len);
    cur += len;

    // skip init cmd len
    cur += len;

    memcpy(upd_cmd + cur, (char*)&upd_num, len);
    cur += len;

    for (int i=0; i<upd_num; i++)
    {
        int blk_id = upd_blk_vec[i]._id;
        memcpy(upd_cmd + cur, (char*)&blk_id, len);
        cur += len;
        if (COORDINATOR_DEBUG)
            cout << "updated data blk_id = " << blk_id << endl;

        if (COORDINATOR_DEBUG)
            cout << "coff = ";
        for (int j=0; j<ecM; j++)
        {
            int coff = _rsEncMat[j * ecK + blk_id];
            memcpy(upd_cmd + cur, (char*)&coff, len);
            cur += len;

            if (COORDINATOR_DEBUG)
                cout << coff << " ";
        }
        if (COORDINATOR_DEBUG)
            cout << endl;
    }

    for (int i=0; i<ecM; i++)
    {
        int blk_id = ecK + i; // the last m node are the parity node
        memcpy(upd_cmd + cur, (char*)&blk_id, len);
        cur += len;

        if (COORDINATOR_DEBUG)
            cout << "parity blk_id = " << blk_id << endl;
    }

    // lastly, copy cmd len at second location
    memcpy(upd_cmd + 4, (char*)&cur, len);

    redisAppendCommand(_ip2Ctx[upd_blk_vec[rand_collector]._ip], "RPUSH upd_cmds %b", upd_cmd, cur);
    redisGetReply(_ip2Ctx[upd_blk_vec[rand_collector]._ip], (void**)&rReply);  // send "upd_cmds" to collector
    freeReplyObject(rReply);

    if (COORDINATOR_DEBUG)
        cout << "coor send upd_cmds to data collector ." << to_string((upd_blk_vec[rand_collector]._ip >> 24) & 0xff) << endl;

    for (int i=0; i<ecM; i++)
    {
        task_type = 3;
        cur = 0;

        memcpy(upd_cmd + cur, (char*)&task_type, len);
        cur += len;

        // skip init cmd len
        cur += len;

        int blk_id = ecK + i; // the last m node are the parity node
        memcpy(upd_cmd + cur, (char*)&blk_id, len);
        cur += len;

        // lastly, copy cmd len at second location
        memcpy(upd_cmd + 4, (char*)&cur, len);

        rReply = (redisReply*)redisCommand(_ip2Ctx[_conf->_helpersIPs[blk_id]], "RPUSH upd_cmds %b", upd_cmd, cur); // send "upd_cmds" to node
        freeReplyObject(rReply);

        if (COORDINATOR_DEBUG)
            cout << "coor send upd_cmds to parity node ." << to_string((_conf->_helpersIPs[blk_id] >> 24) & 0xff) << endl;
    }

}

void UPCoordinator::DeltaUpdate(vector<UpdBlock> &upd_blk_vec)
{
    if (COORDINATOR_DEBUG)
        cout << "start " << __func__ << endl;
    
    int upd_num = upd_blk_vec.size();
    int ecK = _conf -> _ecK;
    int ecM = _conf -> _ecN - ecK;

    char upd_cmd[COMMAND_MAX_LENGTH];
    int cur, len = 4;
    int task_type;

    redisReply* rReply;

    for (int i=0; i<ecM; i++)
    {
        task_type = 5;
        cur = 0;

        memcpy(upd_cmd + cur, (char*)&task_type, len);
        cur += len;

        // skip init cmd len
        cur += len;

        memcpy(upd_cmd + cur, (char*)&upd_num, len);
        cur += len;

        int blk_id = ecK + i; // the last m node are the parity node
        memcpy(upd_cmd + cur, (char*)&blk_id, len);
        cur += len;

        // lastly, copy cmd len at second location
        memcpy(upd_cmd + 4, (char*)&cur, len);

        rReply = (redisReply*)redisCommand(_ip2Ctx[_conf->_helpersIPs[blk_id]], "RPUSH upd_cmds %b", upd_cmd, cur); // send "upd_cmds" to node
        freeReplyObject(rReply);

        if (COORDINATOR_DEBUG)
            cout << "coor send upd_cmds to parity node ." << to_string((_conf->_helpersIPs[blk_id] >> 24) & 0xff) << endl;
    }

    for (int i=0; i<upd_num; i++)
    {
        task_type = 4;

        cur = 0;
        memcpy(upd_cmd + cur, (char*)&task_type, len);
        cur += len;

        // skip init cmd len
        cur += len;

        int blk_id = upd_blk_vec[i]._id;

        if (COORDINATOR_DEBUG)
            cout << "updated data blk_id = " << blk_id << endl;

        memcpy(upd_cmd + cur, (char*)&(upd_blk_vec[i]._id), len);
        cur += len;

        int offset_add = upd_blk_vec[i]._offset_add;
        int upd_size = upd_blk_vec[i]._upd_size;

        if (COORDINATOR_DEBUG)
            cout << "updated info: " << offset_add << "  " << upd_size << endl;

        memcpy(upd_cmd + cur, (char*)&offset_add, len);
        cur += len;

        memcpy(upd_cmd + cur, (char*)&upd_size, len);
        cur += len;

        

        if (COORDINATOR_DEBUG)
            cout << "coff = ";
        for (int j=0; j<ecM; j++)
        {
            int coff = _rsEncMat[j * ecK + blk_id];
            memcpy(upd_cmd + cur, (char*)&coff, len);
            cur += len;

            if (COORDINATOR_DEBUG)
                cout << coff << " ";
        }
        if (COORDINATOR_DEBUG)
            cout << endl;
        
        for (int j=0; j<ecM; j++)
        {
            int blk_id = ecK + j; // the last m node are the parity node
            memcpy(upd_cmd + cur, (char*)&blk_id, len);
            cur += len;

            if (COORDINATOR_DEBUG)
                cout << "parity blk_id = " << blk_id << endl;
        }

        // lastly, copy cmd len at second location
        memcpy(upd_cmd + 4, (char*)&cur, len);

        rReply = (redisReply*)redisCommand(_ip2Ctx[upd_blk_vec[i]._ip], "RPUSH upd_cmds %b", upd_cmd, cur); // send "upd_cmds" to node
        freeReplyObject(rReply);

        if (COORDINATOR_DEBUG)
            cout << "coor send upd_cmds to data node ." << to_string((upd_blk_vec[i]._ip >> 24) & 0xff) << endl;
    }

    
}

void UPCoordinator::CrdUpdate(vector<UpdBlock> &upd_blk_vec)
{
    if (COORDINATOR_DEBUG)
        cout << "start " << __func__ << endl;
    int ecK = _conf -> _ecK;
    int ecM = _conf -> _ecN - ecK;
    int upd_num = upd_blk_vec.size();
    int parity_collector_id = rand() % ecM + ecK; // randomly select a parity collector
    

    char upd_cmd[COMMAND_MAX_LENGTH];
    int cur, len;
    int task_type;

    int offset_add, upd_size;

    redisReply* rReply;

    cur = 0;
    len = 4;

    for (int i=0; i<upd_num; i++)
    {    
        task_type = 6;
        cur = 0;

        memcpy(upd_cmd + cur, (char*)&task_type, len);
        cur += len;

        // skip init cmd len
        cur += len;

        memcpy(upd_cmd + cur, (char*)&(upd_blk_vec[i]._id), len);
        cur += len;

        offset_add = upd_blk_vec[i]._offset_add;
        upd_size = upd_blk_vec[i]._upd_size;

        if (COORDINATOR_DEBUG)
            cout << "updated info: " << offset_add << "  " << upd_size << endl;

        memcpy(upd_cmd + cur, (char*)&offset_add, len);
        cur += len;

        memcpy(upd_cmd + cur, (char*)&upd_size, len);
        cur += len;

        memcpy(upd_cmd + cur, (char*)&parity_collector_id, len);
        cur += len;

        // lastly, copy cmd len at second location
        memcpy(upd_cmd + 4, (char*)&cur, len);

        rReply = (redisReply*)redisCommand(_ip2Ctx[upd_blk_vec[i]._ip], "RPUSH upd_cmds %b", upd_cmd, cur); // send "upd_cmds" to node
        freeReplyObject(rReply);

        if (COORDINATOR_DEBUG)
            cout << "coor send upd_cmds to data node ." << to_string((upd_blk_vec[i]._ip >> 24) & 0xff) << endl;
    }

    map<int, vector<UpdBlock> > upd_solu = get_upd_solu_crd(upd_blk_vec);
    int sub_set_num = upd_solu.size();

    cur = 0;

    task_type = 7;
    memcpy(upd_cmd + cur, (char*)&task_type, len);
    cur += len;

    // skip init cmd len
    cur += len;

    memcpy(upd_cmd + cur, (char*)&parity_collector_id, len);
    cur += len;

    memcpy(upd_cmd + cur, (char*)&upd_num, len);
    cur += len;

    memcpy(upd_cmd + cur, (char*)&sub_set_num, len);
    cur += len;

    for (int i=0; i<upd_num; i++)
    {
        memcpy(upd_cmd + cur, (char*)&(upd_blk_vec[i]._id), len);
        cur += len;

        int sub_set_id = get_sub_set_id(upd_solu, upd_blk_vec[i]._id);
        if (sub_set_id == -1)
        {
            cout << "error sub_set_id!" << endl;
            exit(-1);
        }
        memcpy(upd_cmd + cur, (char*)&sub_set_id, len);
        cur += len;

        offset_add = upd_blk_vec[i]._offset_add;
        upd_size = upd_blk_vec[i]._upd_size;

        memcpy(upd_cmd + cur, (char*)&offset_add, len);
        cur += len;

        memcpy(upd_cmd + cur, (char*)&upd_size, len);
        cur += len;

        if (COORDINATOR_DEBUG)
        {
            cout << "updated info: " << endl
                 << "loc_blk_id = " << upd_blk_vec[i]._id << endl
                 << "sub_set_id = " << sub_set_id << endl
                 << "offset_add = " << offset_add << endl
                 << "upd_size = " << upd_size << endl;
        }

        for (int j=0; j<ecM; j++)
        {
            int coff = _rsEncMat[j * ecK + upd_blk_vec[i]._id];
            memcpy(upd_cmd + cur, (char*)&coff, len);
            cur += len;
        }
            
    }

    for (int i=0; i<ecM; i++)
    {
        if (parity_collector_id == ecK + i)
            continue;
        int blk_id = ecK + i; // the last ecM-1 node are the parity node
        memcpy(upd_cmd + cur, (char*)&blk_id, len);
        cur += len;
    }

    // lastly, copy cmd len at second location
    memcpy(upd_cmd + 4, (char*)&cur, len);

    redisAppendCommand(_ip2Ctx[_conf->_helpersIPs[parity_collector_id]], "RPUSH upd_cmds %b", upd_cmd, cur);
    redisGetReply(_ip2Ctx[_conf->_helpersIPs[parity_collector_id]], (void**)&rReply);  // send "upd_cmds" to collector
    freeReplyObject(rReply);

    if (COORDINATOR_DEBUG)
        cout << "coor send upd_cmds to parity collector." << to_string((_conf->_helpersIPs[parity_collector_id] >> 24) & 0xff) << endl;

    for (int i = 0; i<ecM; i++)
    {
        if (ecK + i == parity_collector_id)
            continue;

        cur = 0;

        task_type = 8;
        memcpy(upd_cmd + cur, (char*)&task_type, len);
        cur += len;

        // skip init cmd len
        cur += len;

        int blk_id = ecK + i;
        memcpy(upd_cmd + cur, (char*)&blk_id, len);
        cur += len;

        memcpy(upd_cmd + cur, (char*)&sub_set_num, len);
        cur += len;

        for (auto it1 = upd_solu.begin(); it1 != upd_solu.end(); it1++)
        {
            int start_add, end_add;
            int sub_set_id = it1->first;

            memcpy(upd_cmd + cur, (char*)&sub_set_id, len);
            cur += len;

            start_add = INT_MAX;
            end_add = 0;
            for (auto it2 = (it1->second).begin(); it2 != (it1->second).end(); it2++)
            {
                start_add = min(start_add, (*it2)._offset_add);
                end_add = max(end_add, (*it2)._offset_add + (*it2)._upd_size) - 1;
            }

            offset_add = start_add;
            upd_size = end_add - offset_add + 1;

            memcpy(upd_cmd + cur, (char*)&offset_add, len);
            cur += len;

            memcpy(upd_cmd + cur, (char*)&upd_size, len);
            cur += len;
        }

        // lastly, copy cmd len at second location
        memcpy(upd_cmd + 4, (char*)&cur, len);

        rReply = (redisReply*)redisCommand(_ip2Ctx[_conf->_helpersIPs[blk_id]], "RPUSH upd_cmds %b", upd_cmd, cur); // send "upd_cmds" to node
        freeReplyObject(rReply);

        if (COORDINATOR_DEBUG)
            cout << "coor send upd_cmds to non-collector parity node ." << to_string((_conf->_helpersIPs[blk_id] >> 24) & 0xff) << endl;
    }
}

bool UPCoordinator::is_intersected(UpdBlock& d, vector<UpdBlock>& N)
{
	int s1, e1, s2, e2;
	
	s1 = d._offset_add;
	e1 = s1 + d._upd_size;
	
	s2 = INT_MAX;
	e2 = 0;
	
	for (int i=0; i<N.size(); i++)
	{
		s2 = min(s2, N[i]._offset_add);
		e2 = max(e2, N[i]._offset_add + N[i]._upd_size);
	}
	
	if (e1 < s2 or e2 < s1)
		return false;
	else
		return true;
}

map<int, vector<UpdBlock> > UPCoordinator::get_upd_solu_crd(vector<UpdBlock> &upd_blk_vec)
{
	map<int, vector<UpdBlock> > upd_solu;
	list<UpdBlock> D;
	
	for (int i=0; i<upd_blk_vec.size(); i++)
		D.push_back(upd_blk_vec[i]);

	int sub_set_id = 0;
	int flag;
	
	while (D.size() != 0)
	{
		auto it = D.begin();
		UpdBlock blk = *it;
		D.pop_front();
		vector<UpdBlock> N;
		N.push_back(blk);
		
		while (true)
		{
			flag = 0;
			for (auto it = D.begin(); it != D.end(); it++)
			{
				if (is_intersected(*it, N) == true)
				{
					N.push_back(*it);
					D.erase(it);
					flag = 1;
					break;
				}
			} 
			
			if (flag == 0)
				break;
		}
		
		upd_solu[sub_set_id] = N;
		sub_set_id ++;
	}
	
	return upd_solu;
}

int UPCoordinator::get_sub_set_id(map<int, vector<UpdBlock> > upd_solu, int loc_blk_id)
{
    for (auto it1 = upd_solu.begin(); it1 != upd_solu.end(); it1++)
    {
        for (auto it2 = (it1->second).begin(); it2 != (it1->second).end(); it2++)
        {
            if ((*it2)._id == loc_blk_id)
                return it1->first;
        }
    }

    return -1;
}

vector<string> UPCoordinator::splite_string(string str, char c)
{
	int loc;
	vector<string> res;
	string s;
	
	while (str.find(c) != string::npos)
	{
		loc = str.find(c);
		s = str.substr(0, loc);
		res.push_back(s);
		str = str.substr(loc+1);
	}
	
	res.push_back(str);
	
	return res;
}

long long UPCoordinator::get_trace_req_num(string tracefile)
{
    long long req_num = 0;

    ifstream fp(tracefile);

    string line;

    if (!fp)
    {
        cout << "open trace file failed : " << tracefile << endl;
        exit(-1); 
    }

    while (getline(fp,line))
        req_num ++;
    
    return req_num;
}

void UPCoordinator::debug(Config* _conf)
{

    string tracePath = _conf->_traceDir + "/" + _conf->_traceType + "/";  // 确保当前文件夹下全是有效csv
    vector<string> traceNameVec;

    struct dirent* pDirent;
    DIR* pDir = opendir(tracePath.c_str());
    if (pDir != NULL)
    {
            while ((pDirent = readdir(pDir)) != NULL)
            {
                    string traceName = pDirent->d_name;
                    traceNameVec.push_back(traceName);
            }
    }
    traceNameVec.erase(traceNameVec.begin(), traceNameVec.begin() + 3);    //前两个存储的是当前路径和上一级路径，所以要删除

    for (int i=0; i<traceNameVec.size(); i++)
    {
        cout << traceNameVec[i] << endl;
    }
    
    
}

/*
for (auto& it : _conf -> _helpersIPs) 
{
    redisContext *opt = _ip2Ctx[it];
    redisReply* rReply = (redisReply*)redisCommand(opt, "RPUSH test 1");
    freeReplyObject(rReply);
    cout << "coor send to " << (it >> 24) << endl;
}
*/

void UPCoordinator::trigger_update(map<int, vector<int> >& stripe_id2loc_blk_id, map<int, vector<int> >& glo_blk_id2start_end) 
{
    vector<UpdBlock> upd_blk_vec;
    int stripe_id, glo_id, in_id, offset_add, upd_size, upd_num;
    for (auto it = stripe_id2loc_blk_id.begin(); it != stripe_id2loc_blk_id.end(); it++)
    {
        stripe_id = it->first;
        upd_num = (it->second).size();

        for (int i=0; i<upd_num; i++)
        {
            in_id = (it->second)[i];
            //cout << "in_id = " << in_id << endl;
            glo_id = stripe_id * _ecK + in_id;

            offset_add = glo_blk_id2start_end[glo_id][0];//[0, blocksize)
            upd_size = glo_blk_id2start_end[glo_id][1] - glo_blk_id2start_end[glo_id][0];   //[1, blocksize]

            UpdBlock updblk(in_id, (_conf -> _helpersIPs)[in_id], offset_add, upd_size);
            if (COORDINATOR_DEBUG)
                updblk.showblkInfo();
            upd_blk_vec.push_back(updblk);
        }

        if (_conf -> _UPPolicy == "raid")
        {
            this -> RaidUpdate(upd_blk_vec);
        }
        else if (_conf -> _UPPolicy == "delta")
        {
            this -> DeltaUpdate(upd_blk_vec);
        }
        else if (_conf -> _UPPolicy == "crd")
        {
            this -> CrdUpdate(upd_blk_vec);
        }
        else
        {
            cout << "error _conf -> _UPPolicy = " << _conf -> _UPPolicy << endl;
            exit(-1);
        }

        if (recvAck() == 1)
        {
            upd_blk_vec.clear();
            if (COORDINATOR_DEBUG)
                cout << "a stripe completes updates!" << endl;
        }
    }
}

void UPCoordinator::single_doProcess() 
{
    int blocksize = _conf -> _blocksize;
    int ecK = _conf -> _ecK;
    int ecM = _conf -> _ecN - ecK;

    double total_running_time = 0;
    long long total_upd_stripe_num = 0;
    double total_upd_traffic = 0;

    struct timeval tv1, tv2;

    map<int, vector<int> > stripe_id2loc_blk_id; // <stripe id, <local blk id vec> >
    map<int, vector<int> > glo_blk_id2start_end; // <global blk_id, <start_add, end_size> >

    long long offset, update_size, start_blk_id, end_blk_id, stripe_id, glo_blk_id, loc_blk_id;

    if (_conf -> _UPWay == "random")  // test for single stripe update, but later process use multi-stripe
    {
        int upd_stripe_num = 1;
        int upd_num = ecK;

        vector<int> loc_blk_id;
        for (int i=0; i<ecK; i++)
            loc_blk_id.push_back(i);
        stripe_id2loc_blk_id[0] = loc_blk_id;

        for (int i=0; i<ecK; i++)
        {
            offset = rand() % blocksize;//[0, blocksize)
            update_size = (rand() % (blocksize - offset - 1)) + 1;   //[1, blocksize - offset_add)

            vector<int> start_end;

            start_end.push_back(offset);
            start_end.push_back(offset + update_size);

            glo_blk_id2start_end[i] = start_end;
        }
        if (COORDINATOR_DEBUG)
            cout << "triger update !" << endl;
        gettimeofday(&tv1, NULL);

        trigger_update(stripe_id2loc_blk_id, glo_blk_id2start_end);

        gettimeofday(&tv2, NULL);
        if (COORDINATOR_DEBUG)
            cout << "a round of updates finish!" << endl;

        double running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
        total_running_time += running_time;
        total_upd_stripe_num += 1;

        total_upd_traffic += (upd_num + ecM - 1) * blocksize * 1.00 / (1024.0 * 1024.0);

    }
    else if (_conf -> _UPWay == "trace")  // test for multi-stripe update for log trigger
    {
        string tracefile = _conf->_traceDir + "/" + _conf->_traceType + "/wdev_2.csv";
        string line;
        stringstream stream;

        ifstream fp(tracefile);
        if (!fp)
        {
            cout << "open trace file failed : " << tracefile << endl;
            exit(-1); 
        }

        long long req_num =  100000; //get_trace_req_num(tracefile);
        long long count = 0;

        while (getline(fp,line))
        {
            vector<string> res = splite_string(line, ',');

            if ((_conf->_traceType == "MSR" && res[3] == "Read") || (_conf->_traceType == "Ali" && res[1] == "R") || 
            (_conf->_traceType == "Ten" && res[3] == "0"))
                continue;

            if (_conf->_traceType == "MSR")
            {
                stream.clear();  // clear stringstream
                stream.str("");  // free memory
                stream<<res[4];
                stream>>offset;
                stream.clear();
                stream.str("");
                stream<<res[5];
                stream>>update_size;
            }
            else if (_conf->_traceType == "Ali")
            {
                stream.clear();
                stream.str("");
                stream<<res[2];
                stream>>offset;
                stream.clear();
                stream.str("");
                stream<<res[3];
                stream>>update_size;
            }
            else if (_conf->_traceType == "Ten")
            {
                stream.clear();
                stream.str("");
                stream<<res[1];
                stream>>offset;
                offset = offset * 512;
                stream.clear();
                stream.str("");
                stream<<res[2];
                stream>>update_size;
                update_size = update_size * 512;
            }
            else
            {
                cout << "error _conf->_traceType = " << _conf->_traceType << endl;
                exit(-1);
            }

            //cout << offset << " " << update_size << endl;

            offset = offset / 1024;  // Bytes --> KB
            update_size = update_size / 1024;  // Bytes --> KB
            
            if (update_size == 0)
                continue;
            
            start_blk_id = offset / blocksize;
            end_blk_id = (offset + update_size) / blocksize;

            for (int i=start_blk_id; i<=end_blk_id; i++)  // i is global block id
            {
                glo_blk_id = i;
                loc_blk_id = glo_blk_id % ecK;
                stripe_id = glo_blk_id / ecK;

                vector<int> start_end;

                if (glo_blk_id == start_blk_id && glo_blk_id != end_blk_id)
                {
                    start_end.push_back(offset % blocksize);
                    start_end.push_back(blocksize);
                }
                else if (glo_blk_id == start_blk_id && glo_blk_id == end_blk_id)
                {
                    start_end.push_back(offset % blocksize);
                    start_end.push_back((offset + update_size) % blocksize);
                }
                else if (glo_blk_id != start_blk_id && glo_blk_id != end_blk_id)
                {
                    start_end.push_back(0);
                    start_end.push_back(blocksize);
                }
                else if (glo_blk_id != start_blk_id && glo_blk_id == end_blk_id)
                {
                    start_end.push_back(0);
                    start_end.push_back((offset + update_size) % blocksize);
                }

                if (glo_blk_id2start_end.find(glo_blk_id) == glo_blk_id2start_end.end())  // the first insert block
                {
                    glo_blk_id2start_end[glo_blk_id] = start_end;

                    if (stripe_id2loc_blk_id.find(stripe_id) == stripe_id2loc_blk_id.end())
                    {
                        vector<int> v;
                        v.push_back(loc_blk_id);
                        stripe_id2loc_blk_id[stripe_id] = v;
                    }
                    else if (stripe_id2loc_blk_id.find(stripe_id) != stripe_id2loc_blk_id.end() && 
                    find(stripe_id2loc_blk_id[stripe_id].begin(), stripe_id2loc_blk_id[stripe_id].end(), loc_blk_id) == stripe_id2loc_blk_id[stripe_id].end())
                    {
                        stripe_id2loc_blk_id[stripe_id].push_back(loc_blk_id);
                    }
                }
                else  // the global block has been existed
                {
                    glo_blk_id2start_end[glo_blk_id][0] = min(start_end[0], glo_blk_id2start_end[glo_blk_id][0]);
                    glo_blk_id2start_end[glo_blk_id][1] = min(start_end[1], glo_blk_id2start_end[glo_blk_id][1]);
                }
            }

            // check is trigger the update process
            double cur_log_size = glo_blk_id2start_end.size() * blocksize * 1.00 / 1024;  // MB

            if (cur_log_size >= _conf->_log_size || count + 1 >= req_num)
            {
                if (COORDINATOR_DEBUG)
                    cout << "triger update !" << endl;
                gettimeofday(&tv1, NULL);
                trigger_update(stripe_id2loc_blk_id, glo_blk_id2start_end);

                gettimeofday(&tv2, NULL);

                if (COORDINATOR_DEBUG)
                    cout << "a round of updates finish!" << endl;

                double running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
                total_running_time += running_time;
                total_upd_stripe_num += stripe_id2loc_blk_id.size();

                glo_blk_id2start_end.clear();
                stripe_id2loc_blk_id.clear();

            }

            count ++;
            if (count + 1 >= req_num)
                break;
            
        }
        fp.close();
    }
    else
    {
        cout << "error _conf -> _UPWay = " << _conf -> _UPWay << endl;
        exit(-1);
    }
    
    
    double throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time * 1024 * 1.00);  // MB/s

    cout << _conf->_UPPolicy << " throughput : " << throughput << " MB/s" << endl;
    
}

void UPCoordinator::trigger_update_by_multi_doProcess(map<int, vector<int> >& stripe_id2loc_blk_id, map<int, vector<int> >& glo_blk_id2start_end, string UPPolicy) 
{
    vector<UpdBlock> upd_blk_vec;
    int stripe_id, glo_id, in_id, offset_add, upd_size, upd_num;
    int blocksize = _conf -> _blocksize;
    for (auto it = stripe_id2loc_blk_id.begin(); it != stripe_id2loc_blk_id.end(); it++)
    {
        stripe_id = it->first;
        upd_num = (it->second).size();

        for (int i=0; i<upd_num; i++)
        {
            in_id = (it->second)[i];
            glo_id = stripe_id * _ecK + in_id;

            if (UPPolicy == "crd_flip" && in_id % 2 == 1)
            {
                int new_start, new_end;
                new_start = blocksize - glo_blk_id2start_end[glo_id][1];
                new_end = blocksize - glo_blk_id2start_end[glo_id][0];

                offset_add = new_start;
                upd_size = new_end - new_start;
            }
            else
            {
                offset_add = glo_blk_id2start_end[glo_id][0];//[0, blocksize)
                upd_size = glo_blk_id2start_end[glo_id][1] - glo_blk_id2start_end[glo_id][0];   //[1, blocksize]
            }
            

            UpdBlock updblk(in_id, (_conf -> _helpersIPs)[in_id], offset_add, upd_size);
            if (COORDINATOR_DEBUG)
                updblk.showblkInfo();
            upd_blk_vec.push_back(updblk);
        }

        if (UPPolicy == "raid")
        {
            this -> RaidUpdate(upd_blk_vec);
        }
        else if (UPPolicy == "delta")
        {
            this -> DeltaUpdate(upd_blk_vec);
        }
        else if (UPPolicy == "crd")
        {
            this -> CrdUpdate(upd_blk_vec);
        }
        else if (UPPolicy == "crd_flip")
        {
            this -> CrdUpdate(upd_blk_vec);
        }
        else
        {
            cout << "error UPPolicy = " << UPPolicy << endl;
            exit(-1);
        }

        if (recvAck() == 1)
        {
            upd_blk_vec.clear();
            if (COORDINATOR_DEBUG)
                cout << "a stripe completes updates!" << endl;
        }
    }
}

// 查看当前文件是否已经运行过了，返回true表明运行了
bool UPCoordinator::is_computed(string tracefile) 
{
    string filename = _conf->_traceType + "-result.csv";

    string line, current_trace_file;
    stringstream stream;

    ifstream fp(filename, ios::in);
    if (!fp)
    {
        //cout << "open file failed : " << filename << endl;
        return false;
    }
    
    while (getline(fp,line))
    {
        vector<string> res = splite_string(line, ',');
        stream.clear();  // clear stringstream
        stream.str("");  // free memory
        stream<<res[0];
        stream>>current_trace_file;
        
        if (current_trace_file == tracefile)
        {
            fp.close();
            return true;
        }
        	
	}
	
	fp.close();
	
	return false;


}

vector<string> generate_useless_trace()
{
    vector<string> useless_trace_vec;

    useless_trace_vec.push_back("VM_8063.csv");
    useless_trace_vec.push_back("VM_18393.csv");
    useless_trace_vec.push_back("VM_18599.csv");
    useless_trace_vec.push_back("VM_9324.csv");
    useless_trace_vec.push_back("VM_18324.csv");
    useless_trace_vec.push_back("VM_21977.csv");
    useless_trace_vec.push_back("VM_22275.csv");
    useless_trace_vec.push_back("VM_25077.csv");
    useless_trace_vec.push_back("VM_25141.csv");
    useless_trace_vec.push_back("VM_25076.csv");
    useless_trace_vec.push_back("VM_25035.csv");
    useless_trace_vec.push_back("VM_25006.csv");
    useless_trace_vec.push_back("VM_24043.csv");
    useless_trace_vec.push_back("VM_23530.csv");

    return useless_trace_vec;
}

// run raid delta crd
void UPCoordinator::multi_doProcess() 
{
    int blocksize = _conf -> _blocksize;
    int ecK = _conf -> _ecK;
    int ecM = _conf -> _ecN - ecK;

    double total_running_time_raid = 0;
    double total_running_time_delta = 0;
    double total_running_time_crd = 0;
    double total_running_time_crd_flip = 0;
    long long total_upd_stripe_num = 0;

    double running_time, throughput, throughput_raid, throughput_delta, throughput_crd, throughput_crd_flip;

    string tracefilePath, tracefile;

    struct timeval tv1, tv2;

    map<int, vector<int> > stripe_id2loc_blk_id; // <stripe id, <local blk id vec> >
    map<int, vector<int> > glo_blk_id2start_end; // <global blk_id, <start_add, end_size> >

    long long offset, update_size, start_blk_id, end_blk_id, stripe_id, glo_blk_id, loc_blk_id;

    if (_conf -> _UPWay == "random")  // test for single stripe update, but later process use multi-stripe
    {
        int upd_stripe_num = 1;
        int upd_num = ecK;

        vector<int> loc_blk_id;
        for (int i=0; i<ecK; i++)
            loc_blk_id.push_back(i);
        stripe_id2loc_blk_id[0] = loc_blk_id;

        for (int i=0; i<ecK; i++)
        {
            offset = rand() % blocksize;//[0, blocksize)
            update_size = (rand() % (blocksize - offset - 1)) + 1;   //[1, blocksize - offset_add)

            vector<int> start_end;

            start_end.push_back(offset);
            start_end.push_back(offset + update_size);

            glo_blk_id2start_end[i] = start_end;
        }
        if (COORDINATOR_DEBUG)
            cout << "triger update !" << endl;

        
        gettimeofday(&tv1, NULL);
        trigger_update_by_multi_doProcess(stripe_id2loc_blk_id, glo_blk_id2start_end, "raid");
        gettimeofday(&tv2, NULL);
        running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
        total_running_time_raid += running_time;

        gettimeofday(&tv1, NULL);
        trigger_update_by_multi_doProcess(stripe_id2loc_blk_id, glo_blk_id2start_end, "delta");
        gettimeofday(&tv2, NULL);
        running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
        total_running_time_delta += running_time;

        gettimeofday(&tv1, NULL);
        trigger_update_by_multi_doProcess(stripe_id2loc_blk_id, glo_blk_id2start_end, "crd");
        gettimeofday(&tv2, NULL);
        running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
        total_running_time_crd += running_time;

        if (COORDINATOR_DEBUG)
            cout << "a round of updates finish!" << endl;
        total_upd_stripe_num += 1;

        throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time_raid * 1024 * 1.00);  // MB/s
        cout << "raid throughput : " << throughput << " MB/s" << endl;

        throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time_delta * 1024 * 1.00);  // MB/s
        cout << "delta throughput : " << throughput << " MB/s" << endl;

        throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time_crd * 1024 * 1.00);  // MB/s
        cout << "crd throughput : " << throughput << " MB/s" << endl;
    }
    else if (_conf -> _UPWay == "trace")  // test for multi-stripe update for log trigger
    {
        string tracePath = _conf->_traceDir + "/" + _conf->_traceType + "/";  // 确保当前文件夹下全是有效csv
        vector<string> traceNameVec;

        struct dirent* pDirent;
        DIR* pDir = opendir(tracePath.c_str());
        if (pDir != NULL)
        {
            while ((pDirent = readdir(pDir)) != NULL)
            {
                    string traceName = pDirent->d_name;
                    if (traceName == "." or traceName == "..")
                        continue;
                    traceNameVec.push_back(traceName);

                    
            }
        }
        //traceNameVec.erase(traceNameVec.begin(), traceNameVec.begin() + 3);    //前两个存储的是当前路径和上一级路径，所以要删除

        vector<string> useless_trace_vec = generate_useless_trace();

        for (int i=0; i<traceNameVec.size(); i++)
        {
            if (is_computed(traceNameVec[i]) == true)
                continue;
            
            // zhouhai debug
            //cout << traceNameVec[i] << endl;
            //exit(0);
            
            tracefilePath = _conf->_traceDir + "/" + _conf->_traceType + "/" + traceNameVec[i];

            total_running_time_raid = 0;
            total_running_time_delta = 0;
            total_running_time_crd = 0;
            total_running_time_crd_flip = 0;
            total_upd_stripe_num = 0;

            string line;
            stringstream stream;

            ifstream fp(tracefilePath);
            if (!fp)
            {
                cout << "open trace file failed : " << tracefilePath << endl;
                exit(-1); 
            }

            long long req_num =  5000; //get_trace_req_num(tracefile);
            long long count = 0;

            while (getline(fp,line))
            {
                vector<string> res = splite_string(line, ',');

                if ((_conf->_traceType == "MSR" && res[3] == "Read") || (_conf->_traceType == "Ali" && res[1] == "R") || 
                (_conf->_traceType == "Ten" && res[3] == "0"))
                    continue;

                if (_conf->_traceType == "MSR")
                {
                    stream.clear();  // clear stringstream
                    stream.str("");  // free memory
                    stream<<res[4];
                    stream>>offset;
                    stream.clear();
                    stream.str("");
                    stream<<res[5];
                    stream>>update_size;
                }
                else if (_conf->_traceType == "Ali")
                {
                    stream.clear();
                    stream.str("");
                    stream<<res[2];
                    stream>>offset;
                    stream.clear();
                    stream.str("");
                    stream<<res[3];
                    stream>>update_size;
                }
                else if (_conf->_traceType == "Ten")
                {
                    stream.clear();
                    stream.str("");
                    stream<<res[1];
                    stream>>offset;
                    offset = offset * 512;
                    stream.clear();
                    stream.str("");
                    stream<<res[2];
                    stream>>update_size;
                    update_size = update_size * 512;
                }
                else
                {
                    cout << "error _conf->_traceType = " << _conf->_traceType << endl;
                    exit(-1);
                }

                //cout << offset << " " << update_size << endl;

                offset = offset / 1024;  // Bytes --> KB
                update_size = update_size / 1024;  // Bytes --> KB
                
                if (update_size == 0)
                    continue;
                
                start_blk_id = offset / blocksize;
                end_blk_id = (offset + update_size) / blocksize;

                for (int i=start_blk_id; i<=end_blk_id; i++)  // i is global block id
                {
                    glo_blk_id = i;
                    loc_blk_id = glo_blk_id % ecK;
                    stripe_id = glo_blk_id / ecK;

                    vector<int> start_end;

                    if (glo_blk_id == start_blk_id && glo_blk_id != end_blk_id)
                    {
                        start_end.push_back(offset % blocksize);
                        start_end.push_back(blocksize);
                    }
                    else if (glo_blk_id == start_blk_id && glo_blk_id == end_blk_id)
                    {
                        start_end.push_back(offset % blocksize);
                        start_end.push_back((offset + update_size) % blocksize);
                    }
                    else if (glo_blk_id != start_blk_id && glo_blk_id != end_blk_id)
                    {
                        start_end.push_back(0);
                        start_end.push_back(blocksize);
                    }
                    else if (glo_blk_id != start_blk_id && glo_blk_id == end_blk_id)
                    {
                        start_end.push_back(0);
                        start_end.push_back((offset + update_size) % blocksize);
                    }

                    if (glo_blk_id2start_end.find(glo_blk_id) == glo_blk_id2start_end.end())  // the first insert block
                    {
                        glo_blk_id2start_end[glo_blk_id] = start_end;

                        if (stripe_id2loc_blk_id.find(stripe_id) == stripe_id2loc_blk_id.end())
                        {
                            vector<int> v;
                            v.push_back(loc_blk_id);
                            stripe_id2loc_blk_id[stripe_id] = v;
                        }
                        else if (stripe_id2loc_blk_id.find(stripe_id) != stripe_id2loc_blk_id.end() && 
                        find(stripe_id2loc_blk_id[stripe_id].begin(), stripe_id2loc_blk_id[stripe_id].end(), loc_blk_id) == stripe_id2loc_blk_id[stripe_id].end())
                        {
                            stripe_id2loc_blk_id[stripe_id].push_back(loc_blk_id);
                        }
                    }
                    else  // the global block has been existed
                    {
                        glo_blk_id2start_end[glo_blk_id][0] = min(start_end[0], glo_blk_id2start_end[glo_blk_id][0]);
                        glo_blk_id2start_end[glo_blk_id][1] = min(start_end[1], glo_blk_id2start_end[glo_blk_id][1]);
                    }
                }

                // check is trigger the update process
                double cur_log_size = glo_blk_id2start_end.size() * blocksize * 1.00 / 1024;  // MB

                if (cur_log_size >= _conf->_log_size || count + 1 >= req_num)
                {
                    if (COORDINATOR_DEBUG)
                        cout << "triger update !" << endl;

                    gettimeofday(&tv1, NULL);
                    trigger_update_by_multi_doProcess(stripe_id2loc_blk_id, glo_blk_id2start_end, "raid");
                    gettimeofday(&tv2, NULL);
                    running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
                    total_running_time_raid += running_time;

                    gettimeofday(&tv1, NULL);
                    trigger_update_by_multi_doProcess(stripe_id2loc_blk_id, glo_blk_id2start_end, "delta");
                    gettimeofday(&tv2, NULL);
                    running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
                    total_running_time_delta += running_time;

                    gettimeofday(&tv1, NULL);
                    trigger_update_by_multi_doProcess(stripe_id2loc_blk_id, glo_blk_id2start_end, "crd");
                    gettimeofday(&tv2, NULL);
                    running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
                    total_running_time_crd += running_time;

                    gettimeofday(&tv1, NULL);
                    trigger_update_by_multi_doProcess(stripe_id2loc_blk_id, glo_blk_id2start_end, "crd_flip");
                    gettimeofday(&tv2, NULL);
                    running_time = (tv2.tv_usec - tv1.tv_usec + (tv2.tv_sec - tv1.tv_sec) * 1000000.0) / 1000000.0;
                    total_running_time_crd_flip += running_time;

                    total_upd_stripe_num += stripe_id2loc_blk_id.size();

                    if (COORDINATOR_DEBUG)
                        cout << "a round of updates finish!" << endl;

                    for (auto it = glo_blk_id2start_end.begin(); it != glo_blk_id2start_end.end(); it ++)
                        (it->second).clear();
                    glo_blk_id2start_end.clear();

                    for (auto it = stripe_id2loc_blk_id.begin(); it != stripe_id2loc_blk_id.end(); it ++)
                        (it->second).clear();
                    stripe_id2loc_blk_id.clear();

                }

                count ++;
                if (count + 1 >= req_num)
                    break;
                
            }

            fp.close();

            cout << traceNameVec[i] << endl;

            if (total_running_time_raid == 0 || total_running_time_delta == 0 || total_running_time_crd == 0 || total_running_time_crd_flip == 0)
                continue;

            throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time_raid * 1024 * 1.00);  // MB/s
            throughput_raid = throughput;
            cout << "raid throughput : " << throughput << " MB/s" << endl;

            throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time_delta * 1024 * 1.00);  // MB/s
            throughput_delta = throughput;
            cout << "delta throughput : " << throughput << " MB/s" << endl;

            throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time_crd * 1024 * 1.00);  // MB/s
            throughput_crd = throughput;
            cout << "crd throughput : " << throughput << " MB/s" << endl;

            throughput = total_upd_stripe_num * _ecM * _conf->_blocksize * 1.00 / (total_running_time_crd_flip * 1024 * 1.00);  // MB/s
            throughput_crd_flip = throughput;
            cout << "crd_flip throughput : " << throughput << " MB/s" << endl;

            // write file
            string resultfile = _conf->_traceType + "-result.csv";
	        ofstream myFile(resultfile, ios::app);

            myFile << traceNameVec[i];
            myFile << ",";
            myFile << throughput_raid;
            myFile << ",";
            myFile << throughput_delta;
            myFile << ",";
            myFile << throughput_crd;
            myFile << ",";
            myFile << throughput_crd_flip;
            myFile << "\n";

            myFile.close();
            
        }
    }
    else
    {
        cout << "error _conf -> _UPWay = " << _conf -> _UPWay << endl;
        exit(-1);
    }
    
}



