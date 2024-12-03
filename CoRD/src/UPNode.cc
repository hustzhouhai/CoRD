#include "UPNode.hh"

UPNode::UPNode(Config* conf) 
{
    _conf = conf;
    _locIP = _conf -> _localIP;
    init();
}

void UPNode::init() 
{
    _ecK = _conf -> _ecK;
    _ecM = _conf -> _ecN - _ecK;

    for (auto& it : _conf -> _helpersIPs) 
    {
        _ip2Ctx[it] = initCtx(it);
    }
    _selfCtx = initCtx(_locIP);

    _coordinatorCtx = initCtx(_conf -> _coordinatorIP);

    redisReply* rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH test 1");
    freeReplyObject(rReply);
  
}

redisContext* UPNode::initCtx(unsigned int redisIP) 
{
    struct timeval timeout = { 1, 500000 }; 
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

string UPNode::ip2Str(unsigned int ip) const 
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

void UPNode::doProcess() 
{
    int ecK = _conf -> _ecK;
    int ecM = _conf -> _ecN - ecK;

    while(1)
    {
        cout << "waitting for coor cmd ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop upd_cmds 100");
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
            cout << "node recv upd_cmds from coor!" << endl;

            int task_type, cmd_len;
            memcpy((char*)&task_type, rReply -> element[1] -> str, 4); 
            memcpy((char*)&cmd_len, rReply -> element[1] -> str + 4, 4); 

            if (NODE_DEBUG)
            {
                cout << "task_type = " << task_type << endl;
                cout << "cmd_len = " << cmd_len << endl;
            }

            char upd_cmd[COMMAND_MAX_LENGTH];
            memcpy(upd_cmd, rReply -> element[1] -> str, cmd_len);

            freeReplyObject(rReply);

            if (task_type == 1)
            {
                send_data_to_collector_for_raid(upd_cmd, cmd_len);
            }
            else if(task_type == 2)
            {
                recv_and_compute_and_send_for_raid(upd_cmd, cmd_len);
            }
            else if(task_type == 3)
            {
                recv_and_save_for_raid(upd_cmd, cmd_len);
            }
            else if(task_type == 4)
            {
                send_parity_delta_to_each_parity_for_delta(upd_cmd, cmd_len);
            }
            else if (task_type == 5)
            {
                recv_parity_delta_and_aggre_for_delta(upd_cmd, cmd_len);
            }
            else if (task_type == 6)
            {
                send_data_delta_to_parity_collector_for_crd(upd_cmd, cmd_len);
            }
            else if (task_type == 7)
            {
                recv_and_compute_in_parity_collector_for_crd(upd_cmd, cmd_len);
            }
            else if (task_type == 8)
            {
                recv_parity_delta_and_aggre_for_crd(upd_cmd, cmd_len);
            }
            else
            {
                cout << "error task_type = " << task_type << endl;
                exit(-1);
            }
        
        }
    }
}

void UPNode::send_data_to_collector_for_raid(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    // Parse cmd
    int cur, len;
    int blk_id, collector_id;

    cur = 8;
    len = 4;

    memcpy((char*)&blk_id, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&collector_id, upd_cmd + cur, len);
    cur += len;

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    if (NODE_DEBUG)
    {
        cout << "blk_id = " << blk_id << endl   
             << "collector_id = " << collector_id << endl;
    }

    // read data and send to collector
    char* send_buffer = (char*)malloc((4 + _conf->_blocksize * 1024) * sizeof(char));  // blk_id + full-blk
    memcpy(send_buffer, (char*)&blk_id, 4);

    char* old_data_buff = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));  
    char* new_data_buff = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));  // new data
    char* delta_buff = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));  // data delta

    // read old data 
    string filename = "blk_" + to_string(blk_id);
    readDataFromDisk(filename, old_data_buff, _conf->_blocksize * 1024, 0);

    // random generate new data
    gene_radm_buff(new_data_buff, _conf->_blocksize * 1024);

    // compute data delta
    compute_delta(delta_buff, old_data_buff, new_data_buff, _conf->_blocksize * 1024);
    
    // copy data delta to send_buff
    memcpy(send_buffer + 4, delta_buff, _conf->_blocksize * 1024);

    redisContext *destCon = _ip2Ctx[_conf->_helpersIPs[collector_id]];
    redisReply* rReply = (redisReply*)redisCommand(destCon, "RPUSH raidData %b", send_buffer, (4 + _conf->_blocksize * 1024) * sizeof(char));
    freeReplyObject(rReply);
    cout << "send raidData to collector" << endl;

    // write new data to local disk
    writeDataToDisk(filename, new_data_buff, _conf->_blocksize * 1024, 0);

    free(delta_buff);
    free(new_data_buff);
    free(old_data_buff);
    free(send_buffer);

}

void UPNode::recv_and_compute_and_send_for_raid(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    int ecK = _conf -> _ecK;
    int ecN = _conf -> _ecN;
    int ecM = ecN - ecK;
    int cur, len;
    int upd_num;

    // Parse cmd
    cur = 8;
    len = 4;
    memcpy((char*)&upd_num, upd_cmd + cur, len);
    cur += len;

    map<int, vector<int> > blkid2coffs;
    vector<int> parity_id_vec;

    for (int i=0; i<upd_num; i++)
    {
        int blk_id;
        memcpy((char*)&blk_id, upd_cmd + cur, len);
        cur += len;

        for (int j=0; j<ecM; j++)
        {
            int coff;
            memcpy((char*)&coff, upd_cmd + cur, len);
            cur += len;
            blkid2coffs[blk_id].push_back(coff);
        }
    }

    for (int i=0; i<ecM; i++)
    {
        int blk_id;
        memcpy((char*)&blk_id, upd_cmd + cur, len);
        cur += len;

        parity_id_vec.push_back(blk_id);
    }

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    if (NODE_DEBUG)
    {
        cout << "--- collector task info ---" << endl;
        for (auto it : blkid2coffs)
        {
            cout << "updated data blk_id = " << it.first << endl;
            cout << "coffs = ";
            for (int i=0; i<ecM; i++)
            {
                cout << (it.second)[i] << "  ";
            }
            cout << endl;
        }

        for (int i=0; i<ecM; i++)
        {
            cout << "parity blk_id = " << parity_id_vec[i] << endl;
        }
            
    }

    // recv data from other node and read local data
    char** buffer = (char**)malloc(upd_num * sizeof(char*));
    for (int i=0; i<upd_num; i++)
        buffer[i] = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));

    int loc_blk_id;
    for (int i=0; i<(_conf->_helpersIPs).size(); i++)
    {
        if (_locIP == _conf->_helpersIPs[i])
        {
            loc_blk_id = i;
            break;
        }
    }

    string filename;
    filename = "blk_" + to_string(loc_blk_id);
    int map_loc = -1;

    int count = 0;
    for (auto& it : blkid2coffs)
    {
        if (it.first == loc_blk_id)
        {
            map_loc = count;
            break;
        }
        count ++;
    }
    if (map_loc == -1)
    {
        cout << "error map_loc" << endl;
        exit(1);
    }

    readDataFromDisk(filename, buffer[map_loc], _conf->_blocksize * 1024, 0);

    int remain_data_num = upd_num - 1;
    while(1)
    {
        if (remain_data_num == 0)
            break;
        cout << "waitting for data ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop raidData 100");
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
            
            int blk_id;
            memcpy((char*)&blk_id, rReply -> element[1] -> str, 4);
            cout << "recv data block blk_id = " << blk_id << endl;
            count = 0;
            map_loc = -1;
            for (auto& it : blkid2coffs)
            {
                if (it.first == blk_id)
                {
                    map_loc = count;
                    break;
                }
                count ++;
            }
            if (map_loc == -1)
            {
                cout << "error map_loc" << endl;
                exit(1);
            }
            memcpy(buffer[map_loc], (rReply -> element[1] -> str) + 4, (_conf->_blocksize * 1024) * sizeof(char));
            freeReplyObject(rReply);
            
            remain_data_num --;
            if (remain_data_num == 0)
                break;
        }
    }

    // compute parity delta
    char** delta_buffer = (char**)malloc(ecM * sizeof(char*));
    for (int i=0; i<ecM; i++)
        delta_buffer[i] = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));

    int* encode_coff = (int*)malloc(upd_num * ecM * sizeof(int));

    count = 0;
    for (int i=0; i<ecM; i++)
    {
        for (auto& it : blkid2coffs)
        {
            encode_coff[count ++] = (it.second)[i];
        }
    }

    jerasure_matrix_encode(upd_num, ecM, 8, encode_coff, buffer, delta_buffer, _conf->_blocksize * 1024);

    //for (int i=0; i<ecM; i++)
        //writeDataToDisk(to_string(i), delta_buffer[i], _conf->_blocksize * 1024, 0);
    
    // send parity delta to parity node
    for (int i=0; i<ecM; i++)
    {
        int blk_id = parity_id_vec[i];
        redisContext *destCon = _ip2Ctx[_conf->_helpersIPs[blk_id]];
        redisReply* rReply = (redisReply*)redisCommand(destCon, "RPUSH raidData %b", delta_buffer[ecN - blk_id - 1], (_conf->_blocksize * 1024) * sizeof(char));
        freeReplyObject(rReply);
        cout << "send raidData to parity node ." << (_conf->_helpersIPs[blk_id] >> 24) << endl;
    }

    // free memory
    free(encode_coff);

    for (int i=0; i<ecM; i++)
        free(delta_buffer[i]);
    free(delta_buffer);

    for (int i=0; i<upd_num; i++)
        free(buffer[i]);
    free(buffer);
}

void UPNode::recv_and_save_for_raid(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    // Parse cmd
    int cur, len;
    int blk_id, collector_id;

    cur = 8;
    len = 4;
    memcpy((char*)&blk_id, upd_cmd + cur, len);
    cur += len;

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    char* buffer = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));

    while(1)
    {
        cout << "waitting for data from collector ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop raidData 100");
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
            
            memcpy(buffer, rReply -> element[1] -> str, _conf->_blocksize * 1024);
            cout << "recv data block blk_id = " << blk_id << endl;
            freeReplyObject(rReply);
            break;
        }
    }
    string filename = "blk_" + to_string(blk_id);
    writeDataToDisk(filename, buffer, _conf->_blocksize * 1024, 0);

    free(buffer);


    redisReply* rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH ack 1");
    freeReplyObject(rReply);
    cout << "send ack to coor" << endl;

}

void UPNode::send_parity_delta_to_each_parity_for_delta(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    // Parse cmd
    int cur, len, blk_id, offset_add, upd_size, loc_blk_id;
    vector<int> coff_vec, parity_id_vec;
    int ecK = _conf->_ecK;
    int ecN = _conf->_ecN;
    int ecM = ecN - ecK;

    cur = 8;
    len = 4;

    memcpy((char*)&loc_blk_id, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&offset_add, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&upd_size, upd_cmd + cur, len);
    cur += len;

    if (NODE_DEBUG)
    {
        cout << "loc_blk_id = " << loc_blk_id << endl   
             << "offset_add = " << offset_add << endl
             << "upd_size = " << upd_size << endl;
    }

    for (int i=0; i<ecM; i++)
    {
        int coff;
        memcpy((char*)&coff, upd_cmd + cur, len);
        cur += len;
        coff_vec.push_back(coff);
    }

    for (int i=0; i<ecM; i++)
    {
        memcpy((char*)&blk_id, upd_cmd + cur, len);
        cur += len;
        parity_id_vec.push_back(blk_id);
    }

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    char* old_data_buff = (char*)malloc((upd_size * 1024) * sizeof(char));  // old data
    char* new_data_buff = (char*)malloc((upd_size * 1024) * sizeof(char));  // new data

    char** data_delta_buff = (char**)malloc(1 * sizeof(char*));  // data delta
    data_delta_buff[0] = (char*)malloc((upd_size * 1024) * sizeof(char)); 

    char** parity_delta_buff = (char**)malloc(1 * sizeof(char*));  // parity delta
    parity_delta_buff[0] = (char*)malloc((upd_size * 1024) * sizeof(char));

    char* send_buff = (char*)malloc((8 + upd_size * 1024) * sizeof(char));  // offset_add + upd_size + upd_size-data

    // read old data 
    string filename = "blk_" + to_string(loc_blk_id);
    readDataFromDisk(filename, old_data_buff, upd_size * 1024, offset_add * 1024);

    // random generate new data
    gene_radm_buff(new_data_buff, upd_size * 1024);

    // compute data delta
    compute_delta(data_delta_buff[0], old_data_buff, new_data_buff, upd_size * 1024);

    // compute parity delta for all parity node and send
    int encode_coff[1];
    for (int i=0; i<ecM; i++)
    {
        encode_coff[0] = coff_vec[i];
        jerasure_matrix_encode(1, 1, 8, encode_coff, data_delta_buff, parity_delta_buff, upd_size * 1024);
        memcpy(send_buff, (char*)&offset_add, 4);
        memcpy(send_buff+4, (char*)&upd_size, 4);
        memcpy(send_buff+8, (char*)&(parity_delta_buff[0]), upd_size * 1024);

        blk_id = parity_id_vec[i];
        redisContext *destCon = _ip2Ctx[_conf->_helpersIPs[blk_id]];
        redisReply* rReply = (redisReply*)redisCommand(destCon, "RPUSH deltaData %b", send_buff, (8 + upd_size * 1024) * sizeof(char));
        freeReplyObject(rReply);
        cout << "send deltaData to parity node ." << (_conf->_helpersIPs[blk_id] >> 24) << endl;
    }

    writeDataToDisk(filename, new_data_buff, upd_size * 1024, offset_add * 1024);

    free(data_delta_buff[0]);
    free(data_delta_buff);
    free(new_data_buff);
    free(old_data_buff);
    free(parity_delta_buff[0]);
    free(parity_delta_buff);
    free(send_buff);

}

void UPNode::recv_parity_delta_and_aggre_for_delta(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    // Parse cmd
    int cur, len, upd_num, loc_blk_id, offset_add, upd_size;

    cur = 8;
    len = 4;

    memcpy((char*)&upd_num, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&loc_blk_id, upd_cmd + cur, len);
    cur += len;

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    char** delta_buff = (char**)malloc((upd_num+1) * sizeof(char*));  // upd_num parity delta and one local parity
    for (int i=0; i<upd_num+1; i++)
    {
        delta_buff[i] = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));
        memset(delta_buff[i], 0, _conf->_blocksize * 1024);
    }
    string filename = "blk_" + to_string(loc_blk_id);
    readDataFromDisk(filename, delta_buff[upd_num], _conf->_blocksize * 1024, 0);

    int count = 0;
    while(1)
    {
        cout << "waitting for data from updated data block ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop deltaData 100");
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
            cout << "recv delta block !" << endl;
            
            memcpy((char*)&offset_add, rReply -> element[1] -> str, 4);
            memcpy((char*)&upd_size, rReply -> element[1] -> str + 4, 4);
            memcpy(delta_buff[count] + offset_add, rReply -> element[1] -> str + 8, upd_size * 1024);
            
            freeReplyObject(rReply);

            count ++;
            if (count == upd_num)
                break;
        }
    }
    char** new_parity_buff = (char**)malloc(1 * sizeof(char*));
    new_parity_buff[0] = (char*)malloc(_conf->_blocksize * 1024 * sizeof(char));

    int* coff_matrix = (int *)malloc((upd_num+1) * sizeof(int));
    for (int i=0; i<upd_num+1; i++)
        coff_matrix[i] = 1;

    jerasure_matrix_encode(upd_num+1, 1, 8, coff_matrix, delta_buff, new_parity_buff, _conf->_blocksize * 1024);

    writeDataToDisk(filename, new_parity_buff[0], _conf->_blocksize, 0);

    free(coff_matrix);
    free(new_parity_buff[0]);
    free(new_parity_buff);
    for (int i=0; i<upd_num+1; i++)
        free(delta_buff[i]);
    free(delta_buff);

    redisReply* rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH ack 1");
    freeReplyObject(rReply);
    cout << "send ack to coor" << endl;
}

void UPNode::send_data_delta_to_parity_collector_for_crd(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    // Parse cmd
    int cur, len;
    int loc_blk_id, offset_add, upd_size, parity_collector_id;

    cur = 8;
    len = 4;

    memcpy((char*)&loc_blk_id, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&offset_add, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&upd_size, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&parity_collector_id, upd_cmd + cur, len);
    cur += len;

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    if (NODE_DEBUG)
    {
        cout << "loc_blk_id = " << loc_blk_id << endl   
             << "offset_add = " << offset_add << endl 
             << "upd_size = " << upd_size << endl 
             << "parity_collector_id = " << parity_collector_id << endl;
    }

    char* send_buffer = (char*)malloc((4 + upd_size * 1024) * sizeof(char));  // loc_blk_id + full-blk
    memcpy(send_buffer, (char*)&loc_blk_id, 4);

    char* old_data_buff = (char*)malloc((upd_size * 1024) * sizeof(char));  // old data
    char* new_data_buff = (char*)malloc((upd_size * 1024) * sizeof(char));  // new data
    char* delta_buff = (char*)malloc((upd_size * 1024) * sizeof(char));  // data delta

    // read old data 
    string filename = "blk_" + to_string(loc_blk_id);
    readDataFromDisk(filename, old_data_buff, upd_size * 1024, offset_add * 1024);

    // random generate new data
    gene_radm_buff(new_data_buff, upd_size * 1024);

    // compute data delta
    compute_delta(delta_buff, old_data_buff, new_data_buff, upd_size * 1024);
    
    // copy data delta to send_buff
    memcpy(send_buffer + 4, delta_buff, upd_size * 1024);

    redisContext *destCon = _ip2Ctx[_conf->_helpersIPs[parity_collector_id]];
    redisReply* rReply = (redisReply*)redisCommand(destCon, "RPUSH crdData %b", send_buffer, (4 + upd_size * 1024) * sizeof(char));
    freeReplyObject(rReply);
    cout << "send crdData to parity collector" << endl;

    // write new data to local disk
    writeDataToDisk(filename, new_data_buff, upd_size * 1024, offset_add * 1024);

    free(delta_buff);
    free(new_data_buff);
    free(old_data_buff);
    free(send_buffer);
}

void UPNode::recv_and_compute_in_parity_collector_for_crd(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    // Parse cmd
    int ecK = _conf->_ecK;
    int ecM = _conf->_ecN - ecK;
    int cur, len;
    int loc_blk_id, offset_add, upd_size, upd_num, sub_set_num, blk_id, sub_set_id, coff;

    map<int, int> blk_id2sub_set_id;  // <blk_id, sub_set_id>
    map<int, vector<int> > blk_id2coff; // <blk_id, m coff>
    map<int, vector<int> > blk_id2off_size; // <blk_id, <offset_add, upd_size> >
    vector<int> remain_parity_id_vec;
    vector<int> upd_blk_id;

    cur = 8;
    len = 4;

    memcpy((char*)&loc_blk_id, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&upd_num, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&sub_set_num, upd_cmd + cur, len);
    cur += len;

    for (int i=0; i<upd_num; i++)
    {
        memcpy((char*)&blk_id, upd_cmd + cur, len);
        cur += len;

        memcpy((char*)&sub_set_id, upd_cmd + cur, len);
        cur += len;

        memcpy((char*)&offset_add, upd_cmd + cur, len);
        cur += len;

        memcpy((char*)&upd_size, upd_cmd + cur, len);
        cur += len;

        upd_blk_id.push_back(blk_id);

        blk_id2sub_set_id[blk_id] = sub_set_id;
        vector<int> off_size_t;
        off_size_t.push_back(offset_add);
        off_size_t.push_back(upd_size);
        blk_id2off_size[blk_id] = off_size_t;

        vector<int> coff_t;
        for (int j=0; j<ecM; j++)
        {
            memcpy((char*)&coff, upd_cmd + cur, len);
            cur += len;
            coff_t.push_back(coff);
        }

        blk_id2coff[blk_id] = coff_t;
    }

    for (int i=0; i<ecM-1; i++)
    {
        memcpy((char*)&blk_id, upd_cmd + cur, len);
        cur += len;
        remain_parity_id_vec.push_back(blk_id);
    }

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    if (NODE_DEBUG)
    {
        cout << "parity collector recv info: " << endl
             << "loc_blk_id = " << loc_blk_id << endl;
        
        for (int i=0; i<upd_num; i++)
        {
            blk_id = upd_blk_id[i];
            cout << "blk_id = " << blk_id << endl
                 << "sub_set_id = " << blk_id2sub_set_id[blk_id] << endl
                 << "offset_add = " << blk_id2off_size[blk_id][0] << endl
                 << "upd_size = " << blk_id2off_size[blk_id][1] << endl;
        }
    }

    map<int, char*> blk_id2data_delta_buff;
    for (int i=0; i<upd_num; i++)
    {
        blk_id = upd_blk_id[i];
        blk_id2data_delta_buff[blk_id] = (char*)malloc(_conf->_blocksize * 1024 * sizeof(char));
        memset(blk_id2data_delta_buff[blk_id], 0, _conf->_blocksize * 1024 * sizeof(char));
    }

    int count = 0;
    while(1)
    {
        cout << "waitting for data from updated data block ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop crdData 100");
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
            cout << "recv data delta block !" << endl;
            
            memcpy((char*)&blk_id, rReply -> element[1] -> str, 4);
            offset_add = blk_id2off_size[blk_id][0] * 1024 * sizeof(char);
            upd_size = blk_id2off_size[blk_id][1] * 1024 * sizeof(char);
            memcpy(blk_id2data_delta_buff[blk_id] + offset_add, rReply -> element[1] -> str + 4, upd_size);
            
            freeReplyObject(rReply);

            count ++;
            if (count == upd_num)
                break;
        }
    }

    char** data_delta_buff = (char**)malloc(upd_num * sizeof(char*));
    for (int i=0; i<upd_num; i++)
    {
        blk_id = upd_blk_id[i];
        data_delta_buff[i] = blk_id2data_delta_buff[blk_id];
    } 

    // obtain the offset_add and upd_size for per sub-set
    map<int, vector<int> > sub_set_id2off_size;  // <sub_set id, <merge offset_add, merge upd_size> >
    for (int i=0; i<sub_set_num; i++)
    {
        int start_add = INT_MAX;
        int end_add = INT_MIN;

        for (int j=0; j<upd_blk_id.size(); j++)
        {
            blk_id = upd_blk_id[j];
            if (blk_id2sub_set_id[blk_id] == i)
            {
                start_add = min(start_add, blk_id2off_size[blk_id][0]);
                end_add = max(end_add, blk_id2off_size[blk_id][0] + blk_id2off_size[blk_id][1] - 1);
            }
        }
        vector<int> off_size;
        off_size.push_back(start_add);
        off_size.push_back(end_add - start_add + 1);
        sub_set_id2off_size[i] = off_size;
    }

    char** parity_delta_buff = (char**)malloc(ecM * sizeof(char*));
    for (int i=0; i<ecM; i++)
    {
        parity_delta_buff[i] = (char*)malloc(_conf->_blocksize * 1024 * sizeof(char));
        memset(parity_delta_buff[i], 0, _conf->_blocksize * 1024 * sizeof(char));
    } 

    int* matrix = (int*)malloc(ecM * upd_num * sizeof(int));
    for (int i=0; i<ecM; i++)
    {
        for (int j=0; j<upd_num; j++)
        {
            blk_id = upd_blk_id[j];
            matrix[i*upd_num+j] = blk_id2coff[blk_id][i];
        }
    }

    jerasure_matrix_encode(upd_num, ecM, 8, matrix, data_delta_buff, parity_delta_buff, _conf->_blocksize * 1024 * sizeof(char));

    for (int i=0; i<ecM; i++)
    {
        if (ecK+i == loc_blk_id) // the parity collector block
        {
            char** data_buffer = (char**)malloc(2 * sizeof(char*));
            for (int j=0; j<2; j++)
                data_buffer[j] = (char*)malloc(_conf->_blocksize * 1024 * sizeof(char));
            
            string filename = "blk_" + to_string(loc_blk_id);
            readDataFromDisk(filename, data_buffer[0], _conf->_blocksize * 1024, 0);

            memcpy(data_buffer[1], parity_delta_buff[i], _conf->_blocksize * 1024);

            char** parity_buffer = (char**)malloc(1 * sizeof(char*));
            parity_buffer[0] = (char*)malloc(_conf->_blocksize * 1024 * sizeof(char));

            int matrix_t[2] = {1, 1};

            jerasure_matrix_encode(2, 1, 8, matrix_t, data_buffer, parity_buffer, _conf->_blocksize * 1024 * sizeof(char));

            writeDataToDisk(filename, parity_buffer[0], _conf->_blocksize * 1024 * sizeof(char), 0);

            free(parity_buffer[0]);
            free(parity_buffer);
            free(data_buffer[0]);
            free(data_buffer[1]);
            free(data_buffer);
        }
        else  // the remaining parity block
        {
            char* send_buffer = (char*)malloc((4 + _conf->_blocksize * 1024) * sizeof(char));  // sub-set id (4 bytes) + parity-delta (upd_size * 1024 bytes)
            for (int j=0; j<sub_set_num; j++)
            {
                offset_add = sub_set_id2off_size[j][0];
                upd_size = sub_set_id2off_size[j][1];
                memcpy(send_buffer, (char*)&j, 4);
                memcpy(send_buffer + 4, parity_delta_buff[i] + offset_add * 1024, upd_size * 1024);

                blk_id = ecK + i;
                redisContext *destCon = _ip2Ctx[_conf->_helpersIPs[blk_id]];
                redisReply* rReply = (redisReply*)redisCommand(destCon, "RPUSH crdData %b", send_buffer, (4 + upd_size * 1024) * sizeof(char));
                freeReplyObject(rReply);
            }
            cout << "send crdData to remaining parity node ." << (_conf->_helpersIPs[blk_id] >> 24) << endl;
            free(send_buffer);
        }
    }

    free(matrix);

    for (int i=0; i<ecM; i++)
        free(parity_delta_buff[i]);
    free(parity_delta_buff);

    for (int i=0; i<upd_num; i++)
    {
        blk_id = upd_blk_id[i];
        free(blk_id2data_delta_buff[blk_id]);
    }
    
    free(data_delta_buff);

    redisReply* rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH ack 1");
    freeReplyObject(rReply);
    cout << "send ack to coor" << endl;
}

void UPNode::recv_parity_delta_and_aggre_for_crd(char* upd_cmd, int cmd_len)
{
    if (NODE_DEBUG)
    {
        cout << "start " << __func__ << "()" << endl;
    }

    int cur, len, loc_blk_id, sub_set_num, offset_add, upd_size, sub_set_id;
    cur = 8;
    len = 4;

    map<int, vector<int> > sub_set_id2off_size; // <sub-set id, <merge offset_add, merge upd_size> >

    memcpy((char*)&loc_blk_id, upd_cmd + cur, len);
    cur += len;

    memcpy((char*)&sub_set_num, upd_cmd + cur, len);
    cur += len;

    for (int i=0; i<sub_set_num; i++)
    {
        memcpy((char*)&sub_set_id, upd_cmd + cur, len);
        cur += len;

        memcpy((char*)&offset_add, upd_cmd + cur, len);
        cur += len;

        memcpy((char*)&upd_size, upd_cmd + cur, len);
        cur += len;

        vector<int> off_size;
        off_size.push_back(offset_add);
        off_size.push_back(upd_size);
        sub_set_id2off_size[sub_set_id] = off_size;

    }

    if (cur != cmd_len)
    {
        cout << "parse cmd failed!" << endl;
        exit(-1);
    }

    char** parity_buff = (char**)malloc((sub_set_num + 1) * sizeof(char*)); // sub_set_num parity delta and one old parity
    for (int i=0; i<sub_set_num + 1; i++)
    {
        parity_buff[i] = (char*)malloc(_conf->_blocksize * 1024 * sizeof(char));
        memset(parity_buff[i], 0, _conf->_blocksize * 1024 * sizeof(char));
    } 

    int count = 0;
    while(1)
    {
        cout << "waitting for data from parity collector ..." << endl;
        redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop crdData 100");
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
            cout << "recv parity delta from parity collector !" << endl;
            
            memcpy((char*)&sub_set_id, rReply -> element[1] -> str, 4);
            offset_add = sub_set_id2off_size[sub_set_id][0];
            upd_size = sub_set_id2off_size[sub_set_id][1];
            memcpy(parity_buff[sub_set_id] + offset_add * 1024, rReply -> element[1] -> str + 4, upd_size * 1024);
            
            freeReplyObject(rReply);

            count ++;
            if (count == sub_set_num)
                break;
        }
    }

    string filename = "blk_" + to_string(loc_blk_id);
    readDataFromDisk(filename, parity_buff[sub_set_num], _conf->_blocksize * 1024, 0);

    int* matrix = (int*)malloc((sub_set_num + 1) * sizeof(int));
    for (int i=0; i<sub_set_num + 1; i++)
        matrix[i] = 1;
    
    char** new_parity_buffer = (char**)malloc(1 * sizeof(char*));
    new_parity_buffer[0] = (char*)malloc(_conf->_blocksize * 1024 * sizeof(char));

    jerasure_matrix_encode(sub_set_num + 1, 1, 8, matrix, parity_buff, new_parity_buffer, _conf->_blocksize * 1024 * sizeof(char));

    writeDataToDisk(filename, new_parity_buffer[0], _conf->_blocksize * 1024 * sizeof(char), 0);

    free(matrix);

    free(new_parity_buffer[0]);
    free(new_parity_buffer);

    for (int i=0; i<sub_set_num + 1; i++)
        free(parity_buff[i]);
    free(parity_buff);

    redisReply* rReply = (redisReply*)redisCommand(_coordinatorCtx, "RPUSH ack 1");
    freeReplyObject(rReply);
    cout << "send ack to coor" << endl;
}

// read the size of data from file to buffer content at a given offset base
void UPNode::readDataFromDisk(string fileName, char* content, int size, int base) 
{
    // pread, pwrite - read from or write to a file descriptor at a given offset  
    // #include <unistd.h> 
    // ssize_t pread(int fd, void *buf, size_t count, off_t offset);
    // ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset); 

    int fd;

    string fullName = _conf -> _updDataDir + '/' + fileName;
    fd = open(fullName.c_str(), O_RDONLY);
    if(fd < 0)
    {
        perror("open_file_fails");
        exit(1);
    }
        

    int readLen = 0, readl;
    while (readLen < size) 
    {
        if ((readl = pread(fd, content + readLen, size - readLen, base + readLen)) < 0) 
        {
            cerr << "ERROR During disk read" << endl;
            exit(1);
        } 
        else 
        {
            readLen += readl;
        }
    }

    close(fd);
}

// write the size of data from buffer content to file at a given offset base
void UPNode::writeDataToDisk(string fileName, char* content, int size, int base) 
{
    // pread, pwrite - read from or write to a file descriptor at a given offset  
    // #include <unistd.h> 
    // ssize_t pread(int fd, void *buf, size_t count, off_t offset);
    // ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset); 

    int fd;

    string fullName = _conf -> _updDataDir + '/' + fileName;
    fd = open(fullName.c_str(), O_WRONLY|O_CREAT, 0777);
    if(fd < 0)
    {
        perror("open_file_fails");
        exit(1);
    }
        

    int writeLen = 0, writel;
    while (writeLen < size) 
    {
        if ((writel = pwrite(fd, content + writeLen, size - writeLen, base + writeLen)) < 0) 
        {
            cerr << "ERROR During disk write" << endl;
            exit(1);
        } 
        else 
        {
            writeLen += writel;
        }
    }

    close(fd);
}

//generate a random string for the given length
void UPNode::gene_radm_buff(char* buff, int len)
{

    int i;
    char alphanum[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    for(i=0; i<len; i++)
        buff[i]=alphanum[i%(sizeof(alphanum)-1)];
}

//this function is to calculate the delta of two regions
void UPNode::compute_delta(char* result, char* srcA, char* srcB, int length) 
{

    int i;
    int XorCount = length / sizeof(long);

    uint64_t* srcA64 = (uint64_t*) srcA;
    uint64_t* srcB64 = (uint64_t*) srcB;
    uint64_t* result64 = (uint64_t*) result;

    // finish all the word-by-word XOR
    for (i = 0; i < XorCount; i++) {
        result64[i] = srcA64[i] ^ srcB64[i];
    }

}

void UPNode::debug()
{
    if ((_locIP >> 24) != 2)
    {
        redisContext *c = redisConnect("172.17.0.2", 6379);
        redisReply* rReply = (redisReply*)redisCommand(c, "RPUSH test 1");
        freeReplyObject(rReply);
        cout << "send test to coor" << endl;
    }
    

    while(1)
    {
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
            cout << "recv test from coor!" << endl;
            break;
        
        }
    }

    if ((_locIP >> 24) == 3)
    {
        int blk_id = 0;
        char* buffer = (char*)malloc((4 + _conf->_blocksize * 1024) * sizeof(char));  // blk_id + full-blk
        memcpy(buffer, (char*)&blk_id, 4);

        string filename = "blk_" + to_string(blk_id);
        readDataFromDisk(filename, buffer + 4, _conf->_blocksize * 1024, 0);

        redisContext *c = redisConnect("172.17.0.4", 6379);
        redisReply* rReply = (redisReply*)redisCommand(_ip2Ctx[_conf->_helpersIPs[1]], "RPUSH data %b", buffer, (4 + _conf->_blocksize * 1024) * sizeof(char));
        freeReplyObject(rReply);
        cout << "send data to .4" << endl;
        freeReplyObject(rReply);
        free(buffer);
    }
    else if ((_locIP >> 24) == 4)
    {
        while(1)
        {
            cout << "waitting for data ..." << endl;
            redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop data 100");
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
                cout << "recv data from .3" << endl;

                int blk_id;
                memcpy((char*)&blk_id, rReply -> element[1] -> str, 4);
                char* buffer = (char*)malloc((_conf->_blocksize * 1024) * sizeof(char));
                memcpy(buffer, (rReply -> element[1] -> str) + 4, (_conf->_blocksize * 1024) * sizeof(char));

                writeDataToDisk(to_string(blk_id), buffer, (_conf->_blocksize * 1024) * sizeof(char), 0);
                free(buffer);
                freeReplyObject(rReply);
                break;
            
            }
        }
    }
    
}
