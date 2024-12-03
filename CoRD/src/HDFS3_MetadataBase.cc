#include "HDFS3_MetadataBase.hh"

HDFS3_MetadataBase::HDFS3_MetadataBase(Config* conf, RSUtil* rsu):
 MetadataBase(conf, rsu),
 ecN(conf->_ecN),
 ecK(conf->_ecK)
{
     // GetMetadata();
     _metadataThread = thread([=]{GetMetadata();});
     _metadataThread.join();
}
void HDFS3_MetadataBase::GetMetadata()
{
     // get the metadatabase by using hdfs
    //struct timeVal tv1, tv2;
    if (METADATA_BASE_DEBUG)
    cout << __func__ << " starts:\n";
    
    string cmdResult;
    string cmdFsck("hdfs fsck / -files -blocks -locations");
    FILE* pipe = popen(cmdFsck.c_str(), "r");


    if(!pipe) 
        cerr << "ERROR when using hdfs fsck" << endl;
    char cmdBuffer[256];
    while(!feof(pipe))
    {
        if(fgets(cmdBuffer, 256, pipe) != NULL)
        {
            cmdResult += cmdBuffer;
        }
    }
    pclose(pipe);
    if(METADATA_BASE_DEBUG) cerr << "Get the Metadata successfully" << endl;

    set<string> blks;    //the set of blks
    vector<string> tempBlkSet;  // set<string> tempBlkSet;  
    map<string, set<string>> blk2Stripe;
    map<string, vector<string>> recoveree; // helpers' names of any block
    map<string, vector<string>> stripe2Blk;
    _blk2Stripe.clear();

    // added by jhli
    _blkName2Ip.clear();
    _Ip2BlkNames.clear();
    _Id2BlkNames.clear();

    string stripeId, ipAdd, blkName;
    size_t currentPos, endPos, ipLength;

    size_t startPos = cmdResult.find("Live_repl");

    /* here we start to parse the command and get the vector*/
    while(true){
        if(startPos == string::npos){
            break;
        }
        stripeId = cmdResult.substr(startPos - 44, 29);

        int parityIndex = 0;

        if(METADATA_BASE_DEBUG) cout << "Find the stripe: " << stripeId << endl;
        for (int i = 0; i < ecN; i++){
            currentPos = cmdResult.find("blk_", startPos);
            //get the block index
            blkName = cmdResult.substr(currentPos, 24);
            if(METADATA_BASE_DEBUG) cout << "blkName: " << blkName << endl;
            currentPos = cmdResult.find("[", currentPos);
            endPos = cmdResult.find(":", currentPos);
            ipLength = endPos - currentPos - 1;
            ipAdd = cmdResult.substr(currentPos + 1, ipLength);
            startPos = endPos;
            blks.insert(blkName);     

            //to keep the consistency with HDFS-20

            if(i < ecK){
                tempBlkSet.push_back(blkName);
            }else{
                tempBlkSet.insert(tempBlkSet.begin() + parityIndex, blkName);
                parityIndex ++;
            }

        }
        if (METADATA_BASE_DEBUG){
            for (auto it : tempBlkSet){
                cout << it << " " ;
            }
            cout << endl;
        }
        
        startPos = cmdResult.find("Live_repl", startPos + 9);
        stripe2Blk.insert(make_pair(stripeId, tempBlkSet));
        tempBlkSet.clear();
    }


    // TODO: blks used for recovery
    map<string, vector<string>>::iterator iterStripe;
    iterStripe = stripe2Blk.begin();

    // added by jhli
    int stripeIdInt = 0;
    // for each stripe
    while(iterStripe != stripe2Blk.end()){

        // blks used for recovery
        int bidInstripe;
        unsigned int idxInStripe = 0;

        //Now, for each block in a stripe, it constructs the recoveredBlks

        map<unsigned int, string> id2BlkName;

        for(auto iterBlk : iterStripe->second){

            vector<string> recoveredBlks;
            _blkIdInStripe[iterBlk] = idxInStripe;

            // added by jhli
            id2BlkName[idxInStripe] = iterBlk;
            _blkName2StripeId[iterBlk] = stripeIdInt;

            for(auto recoverBlk : iterStripe->second){
                if(iterBlk.compare(recoverBlk) != 0){
                    if (recoveredBlks.size() < ecK){
                        recoveredBlks.push_back(recoverBlk);
                        if (recoveree.find(recoverBlk) == recoveree.end()){
                            recoveree[recoverBlk] = vector<string>();
                        }
                        recoveree[recoverBlk].push_back(iterBlk);
                    }
                }
            }

            int* coef = _rsUtil -> getCoefficient(idxInStripe);
            for (int i = 0; i < recoveredBlks.size(); i++){
                _coefficient[iterBlk].insert({recoveredBlks[i], coef[i]});
            }
            idxInStripe ++;
            recoveredBlks.clear();
        }
        iterStripe++;

        // added by jhli
        map<unsigned int, string> ip2BlkName;
        _Ip2BlkNames.push_back(ip2BlkName);
        _Id2BlkNames.push_back(id2BlkName);
        stripeIdInt++;
    }
     if (METADATA_BASE_DEBUG){
        for (auto it : blk2Stripe){
            cout << it.first << ": ";
            for (auto i : it.second) cout << " " << i;
            cout << endl;
        }
    }    


    if(METADATA_BASE_DEBUG){
        cerr << "the amount of stripes: " << stripe2Blk.size() << endl;
    }

    // TODO: wait for information from redis
    struct timeval timeout = {1, 500000};
    redisContext* rContext = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
    if (rContext == NULL || rContext -> err) {
        if (rContext) {
            cerr << "Connection error: " << rContext -> errstr << endl;
            redisFree(rContext);
        } else {
            cerr << "Connection error: can't allocate redis context" << endl;
        }
        return;
    }

    redisReply* rReply;
    unsigned int holderIP;
    while (true) {
        rReply = (redisReply*)redisCommand(rContext, "BLPOP blk_init 100");
        if(rReply -> type == REDIS_REPLY_NIL){
            cerr << "HDFS3_MetadataBase::HDFSinit() empty queue " << endl;
            freeReplyObject(rReply);
            continue;
        }else if (rReply -> type == REDIS_REPLY_ERROR){
            cerr << "HDFS3_MetadataBase::HDFSinit() ERRPR happens " << endl;
            freeReplyObject(rReply);
            continue;
        }else{
             /**
            * cmd format
            * |<----IP (4 bytes) ---->|<----file name (?Byte)|
            */ 
            memcpy((char*)&holderIP, rReply -> element[1] -> str, 4);
            blkName = string(rReply -> element[1] -> str + 4);
            if(METADATA_BASE_DEBUG){
                cout << "HDFS3_MetadataBase::HDFSinit() getting blk: " << blkName << endl;
            }
            freeReplyObject(rReply);
            if(blks.find(blkName) == blks.end()) continue; 

            for (auto &it : recoveree[blkName]) {
                _blk2Stripe[it].insert({holderIP, blkName});
            }

            // added by jhli
            if (_blkName2StripeId.count(blkName)) {
              int stripeIdInt = _blkName2StripeId[blkName];
              if (!_Ip2BlkNames[stripeIdInt].count(holderIP)) 
                _Ip2BlkNames[stripeIdInt][holderIP] = blkName;
              if (!_blkName2Ip.count(blkName)) {
                _blkName2Ip[blkName] = holderIP; 
              }
            }

            blks.erase(blkName);
            if (blks.empty()) break;
            //blks.erase();
        }      
    }
    printf("%s() ends\n", __func__);
}
vector<pair<unsigned int, string>> HDFS3_MetadataBase::getStripeBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status, int opt)
{
    if (METADATA_BASE_DEBUG) {
      cout << "HDFS3_MetadataBase::getStripeBlks()" << blkName << endl;
      cout << "shuffle = " << _conf -> _shuffle << endl;
    }
    set<pair<unsigned int, string>> retVal = _blk2Stripe[blkName];
    
    vector<pair<unsigned int, string>> ret = vector<pair<unsigned int, string>>(retVal.begin(), retVal.end());
    map<unsigned int, string> rackInfos = _conf->_rackInfos;
    string reqrack = rackInfos[requestorIP];
    if (METADATA_BASE_DEBUG)
      cout << "request rack = " << reqrack << endl;

    map<string, vector<pair<unsigned int, string>>> stripe2Rack;

    for(int i=0; i<ret.size(); i++){
        unsigned int tempip = ret[i].first;
        string temprack = rackInfos[tempip];
        if(stripe2Rack.find(temprack) == stripe2Rack.end()){
            vector<pair<unsigned int, string>> tempv;
            tempv.push_back(ret[i]);
            stripe2Rack[temprack] = tempv;
        }else{
            stripe2Rack[temprack].push_back(ret[i]);
        }
    }

    map<string, vector<pair<unsigned int, string>>>::iterator it = stripe2Rack.begin();
    int idx = 0;
    for(; it!=stripe2Rack.end(); ++it){
        string temprname = it->first;
        vector<pair<unsigned int, string>> tempv = it->second;
        srand(unsigned(time(NULL)+idx));
        idx++;
        std::random_shuffle((it->second).begin(), (it->second).end());
    }

    vector<pair<unsigned int, string>> newret;
    vector<pair<unsigned int, string>> mine;

    if (_conf -> _shuffle){
        if (METADATA_BASE_DEBUG)
          std::cout << "shuffle == true" << std::endl;
        srand(unsigned(time(NULL)));
        std::random_shuffle(ret.begin(), ret.end());
        newret = ret;
    }else{
        for (it = stripe2Rack.begin(); it!=stripe2Rack.end(); ++it){
                string temprname = it->first;
                if(!temprname.compare(reqrack)){
                    vector<pair<unsigned int, string>> tempv = it->second;
                    for(int i=0; i<tempv.size(); i++){
                        mine.push_back(tempv[i]);
                    }
                }else{
                    vector<pair<unsigned int, string>> tempv = it->second;
                    for(int i=0; i<tempv.size(); i++){
                        newret.push_back(tempv[i]);
                    }
                }
        }
        for(int i=0; i<mine.size(); i++){
            newret.push_back(mine[i]);
        }
    }

    if (METADATA_BASE_DEBUG)
    for(int i=0; i<newret.size(); i++){
        cout << "ip= " << newret[i].first << " ";
        cout << "block= " << newret[i].second << " ";
        cout << "rack= " << rackInfos[newret[i].first] << endl;
    }

    return newret;
}
map<string, int> HDFS3_MetadataBase::getCoefficient(const string& blkName, map<string, int>* status)
{
    if (status == nullptr) {
      return _coefficient[blkName];
    }


    map<string, int> coef;
    coef.clear();

    int* int_status = (int*)calloc(_conf -> _ecN, sizeof(int));
    int stripeId = _blkName2StripeId[blkName];
    
    if (METADATA_BASE_DEBUG) {
      for (auto&it : (*status)) printf("%s: status (%s,%d)\n", __func__, it.first.c_str(), it.second);
      printf("%s: stripeId of %s is %d\n", __func__, blkName.c_str(), stripeId);
    }

    for (auto& it : (*status)) {
      if (it.second == 2) int_status[_blkIdInStripe[it.first]] = 1;
    }

    int idx = _blkIdInStripe[blkName];
    int *coefs = _rsUtil -> getCoefficient_multi(idx, int_status);

    int cnt = 0;

    for (int i = 0; i < _conf -> _ecN; i++) if (int_status[i]) {
        if (coefs == nullptr) { 
          printf("ERROR: coefs is empty\n"); return {}; 
        }
        coef[_Id2BlkNames[stripeId][i]] = coefs[cnt++];
    }

    return coef;
}


unsigned int HDFS3_MetadataBase::str2Ip(const string &strIP)
{
    unsigned int nRet = 0;
    char chBuf[16] = "";
    memcpy(chBuf, strIP.c_str(), 15);

    char* szBufTemp = NULL;
    char* szBuf = strtok_r(chBuf, ".", &szBufTemp);

    int i = 3;
    while (NULL != szBuf){
        nRet += atoi(szBuf)<<((3-i)*8);
        szBuf = strtok_r(NULL, ".", &szBufTemp);
        i--;
    }
    return nRet;
}

/* 
 * Return value: lost_ip or req_ip -> (block_name, [(helper IP1, helper block1), ...])
 * opt == 0: no optimization. All requestors use the same k helpers
 * opt == 1: Requestors may use different helpers. The number of helpers corresponding to each requestor remains k.
 * opt == 2: Each requestor may use more than k helpers.
 * */
map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> 
HDFS3_MetadataBase::getStripeBlksMul(const string& blkName, unsigned int requestorIP, map<string, int>* status, int opt)
{
    if (METADATA_BASE_DEBUG) {
        cout << typeid(this).name() << "::" << __func__ << "()" << endl; 
        if (!status) cout << blkName << endl;
        cout << "shuffle = " << _conf -> _shuffle << ", status " << ((status) ? "not null" : "null") << endl;
    }
    set<pair<unsigned int, string>> setVal;
    map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> ret;

    if (status == nullptr) {// single failure
        if (!_blk2Stripe.count(blkName)) {
          printf("%s: ERROR: cannot find blkName: %s\n", __func__, blkName.c_str());
          return {};
        }
        setVal = _blk2Stripe[blkName];
        vector<pair<unsigned int, string>> vec = vector<pair<unsigned int, string>>(setVal.begin(), setVal.end());
        ret[requestorIP] = {blkName, vec};
        return ret;
    }

    vector<pair<string, int>> survived;
    vector<string> failed;

    for (auto it : (*status)) {
        if (METADATA_BASE_DEBUG)
            printf("%s: (.%s, %d)\n", __func__, (it.first).c_str(), it.second);
        if (it.second != 0) survived.push_back({it.first, 0});
        else failed.push_back(it.first);
    }

    int numOfFailed = (int)failed.size();

    set<pair<unsigned int, string>> allStripeBlks = getAllBlks(blkName);

    if (opt % 10 == 0) {   // opt == 0: select k helpers
        for (auto it : allStripeBlks) {  
          if ((int)setVal.size() < _conf -> _ecK && it.first != requestorIP && (*status)[it.second]) {
              setVal.insert({it.first, it.second});
          }
        }
        if ((int)setVal.size() < _conf -> _ecK) return {};

        vector<pair<unsigned int, string>> vec = vector<pair<unsigned int, string>>(setVal.begin(), setVal.end());
        // ret[requestorIP] = {blkName, vec};

        for (auto it : (*status)) {
            if (it.second == 0) {//  && it.first != blkName) {
                unsigned ip = _blkName2Ip[it.first];
                ret[ip] = {it.first, vec};
            }
        }

    }
    else if (opt % 10 == 1) { // opt == 1
        for (int i = 0; i < numOfFailed; i++) {
        setVal.clear();
        int min_value = 999;
        for (int j = 0; j < (int)survived.size(); j++) {
            if (survived[j].second < min_value) min_value = survived[j].second;
        }

        int cnt = 0;
        for (int j = 0; j < (int)survived.size() && cnt < _conf -> _ecK; j++) {
            if (survived[j].second == min_value) {
              cnt++, 
              survived[j].second++, 
              (*status)[survived[j].first] = 2;
              setVal.insert({_blkName2Ip[survived[j].first], survived[j].first});
            }
        }



        for (int j = 0; j < (int)survived.size() && cnt < _conf -> _ecK; j++) if ((*status)[survived[j].first] != 2) {
            cnt++;
            survived[j].second++;
            (*status)[survived[j].first] = 2;
            setVal.insert({_blkName2Ip[survived[j].first], survived[j].first});
        }

        vector<pair<unsigned int, string>> vec = vector<pair<unsigned int, string>>(setVal.begin(), setVal.end());
        
        ret[_blkName2Ip[failed[i]]] = {failed[i], vec};

        for (int j = 0; j < (int)survived.size(); j++) (*status)[survived[j].first] = 1;
        }

    }

    // TODO: finish opt == 2

    if (failed.size() != ret.size()) {
      printf("ERROR: %s: ret size error, ret.size = %d\n", __func__, (int)ret.size());
      exit(1);
    }

    return ret;
}

unsigned int HDFS3_MetadataBase::getIPfromBlk(const string& blkName) {
  if (!_blkName2Ip.count(blkName)) {
    printf("ERROR: %s: no block %s !\n", __func__, blkName.c_str());
    exit(1);
  }
  return _blkName2Ip[blkName]; 
}

set<pair<unsigned int, string>> HDFS3_MetadataBase::getAllBlks(const string&blkName) {
  if (!_blk2Stripe.count(blkName)) {
    printf("ERROR: %s: no block %s !\n", __func__, blkName.c_str());
    exit(1);
  }

  int stripeId = _blkName2StripeId[blkName];
  if (_Ip2BlkNames.size() > stripeId) {
    set<pair<unsigned int, string>> ret;
    int flag = 0;
    for (auto& it : _Ip2BlkNames[stripeId]) {
      ret.insert({it.first, it.second});
    }
    return ret;
  }
  else {
    printf("ERROR: %s: stripeId %d >= _Ip2BlkNames.size() %d\n", 
        __func__, stripeId, (int)_Ip2BlkNames.size());
    exit(1);
  }
} 


