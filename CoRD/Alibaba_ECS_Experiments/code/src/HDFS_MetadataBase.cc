#include "HDFS_MetadataBase.hh"
#include <algorithm>

/*
for (int i=0; i<4; i++)
    {
      redisReply* rReply3 = (redisReply*)redisCommand(_ip2Ctx[i].second.second, "RPUSH test 1");
      freeReplyObject(rReply3);

    }
    break;

*/

HDFS_MetadataBase::HDFS_MetadataBase(Config* conf, RSUtil* rsu) : MetadataBase(conf, rsu)
{
  // TODO: read from stripe store
  DIR* dir;
  FILE* file;
  struct dirent* ent;
  int start, pos;
  unsigned int idxInStripe;
  string fileName, blkName, bName, rawStripe, stripeStore = _conf -> _stripeStore; 
  set<string> blks;
  map<string, set<string>> blk2Stripe;
  map<string, vector<string>> recoveree;

  _rackenable = _conf -> _rackAware;
  _loadbalance = _conf -> _loadbalance;
   
  _blkName2StripeId.clear();
  _blk2Stripe.clear();
  _blkName2Ip.clear();
  _Ip2BlkNames.clear();
  _Id2BlkNames.clear();
  _blk2All.clear();

  int stripeId = -1;

  

  if ((dir = opendir(stripeStore.c_str())) != NULL) 
  {
    while ((ent = readdir(dir)) != NULL) 
    {
      if (METADATA_BASE_DEBUG) 
        cout << "filename: " << ent -> d_name << endl;
      if (strcmp(ent -> d_name, ".") == 0 || strcmp(ent -> d_name, "..") == 0) 
        continue;
      
      map<unsigned int, string> ip2BlkName;
      map<unsigned int, string> id2BlkName;
      bool newStripe = false;

      // open and read in the stripe metadata
      fileName = string(ent -> d_name);
      blkName = fileName.substr(fileName.find(':') + 1);
      blkName = blkName.substr(0, blkName.find_last_of('_'));
      // TODO: remove duplicates
      blks.insert(blkName);
      ifstream ifs(stripeStore + "/" + string(ent -> d_name));
      rawStripe = string(istreambuf_iterator<char>(ifs), istreambuf_iterator<char>());
      if (METADATA_BASE_DEBUG) 
        cout << rawStripe << endl;
      ifs.close();

      if (!_blkName2StripeId.count(blkName)) stripeId++, newStripe=true;

      // parse the stripe content
      start = 0;
      idxInStripe = 0;
      // remove \newline
      if (rawStripe.back() == 10) rawStripe.pop_back();

      // blks used for recovery
      vector<string> recoveredBlks;
      int bidInStripe;
      while (true) 
      {
        pos = rawStripe.find(':', start);
        if (pos == string::npos) bName = rawStripe.substr(start);
        else bName = rawStripe.substr(start, pos - start);
        bName = bName.substr(0, bName.find_last_of('_'));
        if (METADATA_BASE_DEBUG) cout << "blkName: " << blkName << " bName: " << bName << " stripeId: " << stripeId << endl;

        if (newStripe)
          _blkName2StripeId[bName] = stripeId;
        
        if (bName != blkName) 
        {
          //add by Zuoru: rack-aware
          _blk2All[blkName].insert(bName);

          if (recoveredBlks.size() < _conf -> _ecK) 
          {
            blk2Stripe[blkName].insert(bName);
            recoveredBlks.push_back(bName);
            if (recoveree.find(bName) == recoveree.end()) 
              recoveree[bName] = vector<string>();
            recoveree[bName].push_back(blkName);
          }
          idxInStripe ++;
        } 
        else 
        {
        //  cout << " here" << endl;
          bidInStripe = _blkIdInStripe[blkName] = idxInStripe ++;
        // add by Zuoru
         // cout << "bidInStripe: " << bidInStripe << endl;
         // added by jhli
          if (!newStripe)
            _Id2BlkNames[_blkName2StripeId[blkName]][idxInStripe-1] = blkName;
          else
            id2BlkName[idxInStripe-1] = blkName;
        }

        if (pos == string::npos) break;
        start = pos + 1;
      }


      int* coef = _rsUtil -> getCoefficient(bidInStripe);
      for (int i = 0; i < recoveredBlks.size(); i ++) 
      {
        _coefficient[blkName].insert({recoveredBlks[i], coef[i]});
      }
  
      if (METADATA_BASE_DEBUG) {
        printf("start\n");
        for (auto it : _coefficient[blkName]) {
          cout << __func__ << " _coefficient of " << blkName << " is: " 
         << it.first << " " << it.second << endl;
        }
        printf("end\n");
      }

      if (METADATA_BASE_DEBUG) {
        for (auto it : blk2Stripe) {
          cout << it.first << ": ";
          for (auto i : it.second) 
            cout << " " << i;
          cout << endl;
        }
      }

      if (newStripe) {
        _Id2BlkNames.push_back(id2BlkName);
        _Ip2BlkNames.push_back(ip2BlkName);
      }
    }
    closedir(dir);
  } 
  else 
  {
    // TODO: error handling
    ;
  }

  if (_Id2BlkNames.size() != stripeId + 1 || _Ip2BlkNames.size() != stripeId + 1) 
  {
    printf("%s: ERROR: stripeId = %d, _Id2BlkNames.size() = %d, _Ip2BlkNames.size() = %d\n",
        __func__, stripeId, (int)_Id2BlkNames.size(), (int)_Ip2BlkNames.size());
    exit(1);
  }

  // TODO: wait for infomation from redis
  struct timeval timeout = {1, 500000}; // 1.5 seconds
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
  while (true) 
  {
    rReply = (redisReply*)redisCommand(rContext, "BLPOP blk_init 100");  // push block info in BlockReport.cc
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "HDFS_MetadataBase::HDFSinit() empty queue " << endl;
      freeReplyObject(rReply);
      continue;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "HDFS_MetadataBase::HDFSinit() ERROR happens " << endl;
      freeReplyObject(rReply);
      continue;
    } 
    else 
    {
      
      
      /** 
       * cmd format:
       * |<----IP (4 bytes)---->|<-----file name (?Byte)----->|
       */
      memcpy((char*)&holderIP, rReply -> element[1] -> str, 4);
      blkName = string(rReply -> element[1] -> str + 4);
      if (METADATA_BASE_DEBUG) 
      {
        cout << "HDFS_MetadataBase::HDFSinit() getting blk: " << blkName << endl;
      }
      freeReplyObject(rReply);
      if (blks.find(blkName) == blks.end()) 
        continue;
      if (_blkName2StripeId.count(blkName)) 
      {
        int stripeId = _blkName2StripeId[blkName];
        if (1 || METADATA_BASE_DEBUG) 
        {
          printf("%s: stripeId = %d, IP = .%d, blkName = %s\n", __func__, stripeId, holderIP >> 24, blkName.c_str());
        }
        if (!_blkName2Ip.count(blkName)) 
        {
          _blkName2Ip[blkName] = holderIP;
          if (_conf -> _msscheduling && !_order.count(holderIP)) 
            _order[holderIP] = 0;   // to schedule the helpers. Experiment 6(e)
        }
        if (!_Ip2BlkNames[stripeId].count(holderIP)) 
          _Ip2BlkNames[stripeId][holderIP] = blkName;
        else 
        {
          printf("ip of .%d has %s, not %s\n", holderIP >> 24, _Ip2BlkNames[stripeId][holderIP].c_str(), blkName.c_str());
        }
      }

      //for (auto &it : blk2Stripe[blkName]) {
      for (auto &it : recoveree[blkName]) 
      {
        //cout << "getting blkName: " << blkName << " it: " << it << endl;
        _blk2Stripe[it].insert({holderIP, blkName});
      }
      blks.erase(blkName);
      if (blks.empty()) break;
    }
  }

  
}

/* status: IP -> status
 * opt == 0: select the same 
 * */
vector<pair<unsigned int, string>> HDFS_MetadataBase::getStripeBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status, int opt) {
  if (METADATA_BASE_DEBUG) {
    cout << "HDFS_MetadataBase::getStripeBlks()" << blkName << endl;
    cout << "shuffle = " << _conf -> _shuffle << endl;
  }
  set<pair<unsigned int, string>> retVal;

  set<pair<unsigned int, string>> allStripeBlks = getAllBlks(blkName);

  if (status == nullptr){  // single failure
     retVal = _blk2Stripe[blkName];
     printf("retVal.size() = %d\n", (int)retVal.size());
  } else {
    for (auto it : allStripeBlks) {
      if ((!opt && (int)retVal.size() < _conf -> _ecK) && it.first != requestorIP && (*status)[it.second]) {
        retVal.insert({it.first, it.second});
        (*status)[it.second] = 2;
      }
    }
    if ((int)retVal.size() < _conf -> _ecK) return {};
  }

  // vector<pair<unsigned int, string>> ret = vector<pair<unsigned int, string>>(retVal.begin(), retVal.end());

  // add by Zuoru: rack-aware scheme
  vector<pair<unsigned int, string>> ret;
  map<unsigned int, string> rackInfos = _conf->_rackInfos;
  string reqrack = rackInfos[requestorIP];
  cout << "request rack = " <<reqrack<<endl;
  if (METADATA_BASE_DEBUG) cout << "rackenable: " << _rackenable << endl;
  /**
   * Start to select nodes to constrcut the pipeline for rack-aware
   */
  // add by Zuoru: rack-aware to select the minimum of racks
  // map<rackName, blks in this rack> 
  map<string, vector<pair<unsigned int, string>>> stripe2Rack;
  map<string, vector<string>> stripeAll2Rack;
  vector<pair<string, unsigned int>> rackSize;
  vector<pair<unsigned int, string>> candidateBlks;
  vector<pair<string, unsigned int>> candidateRacks;
  map<string, vector<string>> besidesFailureRack;
  int blkRemain = _conf -> _ecK;

  if (_rackenable){

    // classify all blocks: map blockID -->> Rack ID

    for(auto it : _blk2All[blkName]){
      unsigned int tempip = _blkName2Ip[it];
      string temprack = rackInfos[tempip];
      stripeAll2Rack[temprack].push_back(it);
    }

    // First, check the rack of requestorIP, add all blocks in that rack to candidateBlk

    if(stripeAll2Rack.find(reqrack) != stripeAll2Rack.end()){
      for(auto it : stripeAll2Rack[reqrack]){
        string blkName = it;
        candidateBlks.push_back({_blkName2Ip[blkName], blkName});
      }
      cout << "the name of the rack for requestorIP: " << reqrack << endl
           << "the number of the nodes can selected be in that rack: " 
           << candidateBlks.size() << endl;
      blkRemain -= candidateBlks.size();
      cout << "the remaining blocks: " << blkRemain << endl;

      besidesFailureRack = stripeAll2Rack;
      besidesFailureRack.erase(reqrack);
    }
    unsigned int failureRackBlkNum = candidateBlks.size();
    // Second, sort other amount of other racks in descending order.

    for(auto it : besidesFailureRack){
      cout << "rack name: " << it.first << " blk amount: " << it.second.size() << endl;
      rackSize.push_back({it.first, it.second.size()});
    }
    sort(rackSize.begin(), rackSize.end(), [=](pair<string, unsigned int> a, pair<string, unsigned int> b){
      return a.second > b.second;
    });
    cout << "After sort: " << endl;
    for(auto it : rackSize){
      cout << "rack name: " << it.first << " blk amount " << it.second <<endl;
    }

    // Third, choose the helper from minimal 

    cout << "Start to choose racks !" << endl;
    vector<pair<string, unsigned int>>::iterator it = rackSize.begin();
    
    int candidateRacksNum = 1; // because we have the failure rack initially.
    int candidateRemain = blkRemain;
    while(candidateRemain > 0){
      /* find the minimizing rack */
      candidateRacksNum ++;
      cout << "Choose rack: " << it->first << endl;
      cout << "Add rack: " << it->first << " to the candidate rack" << endl;
      candidateRacks.push_back({it->first, it->second});

      for(int i = 0; i < it->second; i++ ){
        string blkName = stripeAll2Rack[it->first][i];
        cout << "add " << blkName << " to candidateBlks." << endl;
        candidateBlks.push_back({_blkName2Ip[blkName], blkName});
      }
      candidateRemain -= it->second;
      cout << "after choose rack: " << it->first << " blkRemain: " << candidateRemain << endl;
       it++; 
    }
    cout << "candidateBlks: " << candidateBlks.size() << endl;
    // add the failure rack to the candidated rack.
    cout << "reqrack: " << reqrack << endl;
    cout << "failreRackBlkNum: " << failureRackBlkNum << endl;
    candidateRacks.push_back({reqrack, failureRackBlkNum});
    cout << "candidateRacks: " << candidateRacks.size() << endl;
    

    // Fourth for the part of load balance

    if(_loadbalance){
      if(candidateBlks.size() > _conf -> _ecK){
        //TODO: to do the load balance
        cout << "start to find the minimized load balance ratio" << endl;
        float idealAverageNum = _conf -> _ecK / (candidateRacksNum);
        map<string, int> schedPlan;
        map<string, int> rackSizeData;
        // Initilization
        for(auto it : candidateRacks){
          schedPlan[it.first] = 0;
          rackSizeData[it.first] = it.second; 
        }
        int counter = 0;
        while(counter < _conf->_ecK){
          for(auto it : candidateRacks){
            if(schedPlan[it.first] < rackSizeData[it.first]){
              schedPlan[it.first] ++;
              counter ++;

            }else if(it.second == rackSizeData[it.first]){
              continue;
            }
            if(counter == _conf->_ecK){
              break;
            }
          }
        }
        for(auto it : candidateRacks){
          cout << "For rack: " << it.first 
          << " Selected Node Number: " << schedPlan[it.first] << endl;
        }
        for(auto it : candidateRacks){
          for(int i = 0; i < schedPlan[it.first]; i++){
            string blkName = stripeAll2Rack[it.first][i];
            ret.push_back({_blkName2Ip[blkName], blkName});
          }
        }
        cout << "the size of ret after schedule: " << ret.size() << endl;
      
      }else if(candidateBlks.size() == _conf -> _ecK){
        cout << "Can not do further load balancing." << endl;
        ret = candidateBlks;
      }else{
        cout << "There is something wrong with rack selection!!";
      }
    }else{
      // do not consider the issue of load balancing
      for(auto it : candidateBlks){
        ret.push_back({it.first, it.second});
        if(ret.size() == _conf->_ecK){
          break;
        }
      }
      /**
       * for(auto it : stripeAll2Rack[reqrack]){
        string blkName = it;
        ret.push_back({_blkName2Ip[blkName], blkName});
      }
      blkRemain -= ret.size();
      for(auto it: rackSize){
        if(blkRemain != 0){
          if(it.second <= blkRemain){
            for(int i = 0; i < it.second; i++){
              string blkName = stripeAll2Rack[it.first][i];
              ret.push_back({_blkName2Ip[blkName], blkName});
            }
            blkRemain -= it.second;
          }else{
            for(int i = 0; i < blkRemain; i++){
              string blkName = stripeAll2Rack[it.first][i];
              ret.push_back({_blkName2Ip[blkName], blkName});
            }
            break;
          }
        }else{
          break;
        }
      }  
     */
      
    }
  
  }else{ 
    //the case: do not use rack-ware (shffle)
    ret = vector<pair<unsigned int, string>>(retVal.begin(), retVal.end());
  }
  
  /**
   * Start to re-schedule construct the whole pipline
   */

  for(int i=0; i<ret.size(); i++) {
          unsigned int tempip = ret[i].first;
          string temprack = rackInfos[tempip];
          if(stripe2Rack.find(temprack) == stripe2Rack.end()) {
                  vector<pair<unsigned int, string>> tempv;
                  tempv.push_back(ret[i]);
                  stripe2Rack[temprack] = tempv;
          } else {
                  stripe2Rack[temprack].push_back(ret[i]);
          }
  }

  map<string, vector<pair<unsigned int, string>>>::iterator it = stripe2Rack.begin();
  int idx=0;
  for(; it!=stripe2Rack.end(); ++it) {
          string temprname = it->first;
          vector<pair<unsigned int, string>> tempv = it->second;
          srand(unsigned(time(NULL))+idx);
          idx++;
          std::random_shuffle((it->second).begin(), (it->second).end());
  }

  vector<pair<unsigned int, string>> newret;
  vector<pair<unsigned int, string>> mine;

  if (_conf -> _shuffle) {
          std::cout<<"shuffle == true"<<std::endl;
          srand(unsigned(time(NULL)));
          std::random_shuffle(ret.begin(), ret.end());
          newret = ret;
  } else {
        for (it = stripe2Rack.begin(); it!=stripe2Rack.end(); ++it) {
                string temprname = it->first;
                if(!temprname.compare(reqrack)) {
                        vector<pair<unsigned int, string>> tempv = it->second;
                        for(int i=0; i<tempv.size(); i++) {
                                mine.push_back(tempv[i]);
                        }
                } else {
                        vector<pair<unsigned int, string>> tempv = it->second;
                        for(int i=0; i<tempv.size(); i++) {
                                newret.push_back(tempv[i]);
                        }
                }
        }

    for(int i=0; i<mine.size(); i++) {
            newret.push_back(mine[i]);
    }
  }

  for(int i=0; i<newret.size(); i++) {
          cout<<"ip = "<<(int)(newret[i].first >> 24)<<" ";
          cout<<"block = "<<newret[i].second<<" ";
          cout<<"rack = "<<rackInfos[newret[i].first]<<endl;
  }

  return newret;
}

// status: should be of size _ecN
// status: blkName -> int(1, connected; 0, not connected)
map<string, int> HDFS_MetadataBase::getCoefficient(const string& blkName, map<string, int>* status){
  if (status == nullptr) {
    printf("start\n");
    for (auto it : _coefficient[blkName]) {
      cout << it.first << " " << it.second << endl;
    }
    printf("end\n");
    return _coefficient[blkName];
  }
  map<string, int> coef;
  coef.clear();

  int* int_status = (int*)calloc(_conf -> _ecN, sizeof(int));

  int stripeId = _blkName2StripeId[blkName];

  for (auto it : (*status)) {
    if (it.second == 2) {
      int_status[_blkIdInStripe[it.first]] = 1;
    }
  }

  int idx = _blkIdInStripe[blkName];
  if (METADATA_BASE_DEBUG) {
    printf("%s: idx of %s is %d\n", __func__, blkName.c_str(), idx);
    printf("int_status: ");
    for (int i = 0; i < _conf -> _ecN; i++) printf("%d ", int_status[i]);
    printf("\n");
    printf("status: \n");
    for (auto& it : (*status)) cout << it.first << "," << it.second << endl;
  }
  int* coefs = _rsUtil -> getCoefficient_multi(idx, int_status);
  int cnt = 0;

  for (int i = 0; i < _conf -> _ecN; i++) {
    if (int_status[i]) {
      if (coefs == nullptr) { printf("coefs is empty\n"); return {};}
      coef[_Id2BlkNames[stripeId][i]] = coefs[cnt++];
    }
  }

  return coef;

}


/* 
 * Return value: lost_ip or req_ip -> (block_name, [(helper IP1, helper block1), ...])
 * opt == 0: no optimization. All requestors use the same k helpers
 * opt == 1: Requestors may use different helpers. The number of helpers corresponding to each requestor remains k.
 * opt == 2: Each requestor may use more than k helpers.
 * */
map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> 
HDFS_MetadataBase::getStripeBlksMul(const string& blkName, unsigned int requestorIP, map<string, int>* status, int opt) 
{

  cout << "HDFS_MetadataBase::getStripeBlksMul()" << blkName << endl;
  cout << "shuffle = " << _conf -> _shuffle << endl;
  set<pair<unsigned int, string>> setVal;
  map<unsigned int, pair<string, vector<pair<unsigned int, string>>>> ret;
  int ecK = _conf -> _ecK;

  if (status == nullptr) {// single failure
    setVal = _blk2Stripe[blkName];
    ret[requestorIP] = {blkName, getStripeBlks(blkName, requestorIP)};
    return ret;
  }

  vector<pair<string, int>> survived;
  vector<string> failed;

  for (auto it : (*status)) 
  {
    if (it.second != 0) 
      survived.push_back({it.first, 0});
    else 
      failed.push_back(it.first);
  }

  int numOfFailed = (int)failed.size();

  set<pair<unsigned int, string>> allStripeBlks = getAllBlks(blkName);

  // reverse so that it can be sortd by filenames
  set<pair<string, unsigned int>> allStripeBlksReverse;

  for (auto it : allStripeBlks) {
    allStripeBlksReverse.insert({it.second, it.first});
  }

  if (opt % 10 == 0) // opt == 0: select k helpers
  {   
    if (_conf -> _msscheduling) 
      setVal = getScheduledBlks(blkName, requestorIP, status, allStripeBlks);
    else 
    /*
    {
      setVal = getRandomBlks(blkName, requestorIP, status, allStripeBlks);
    }
    */
    {
      if (numOfFailed == 1) 
      {  // single failure
        for (auto it : allStripeBlksReverse) 
        {  
          if ((int)setVal.size() < _conf -> _ecK && (*status)[it.first]) 
          {
            setVal.insert({it.second, it.first});  // helper block <IP, block_name>
          }
        }
      }
      else {  // multiple failure
        for (auto it : allStripeBlks) {  
          if ((int)setVal.size() < _conf -> _ecK && it.first != requestorIP && (*status)[it.second]) {
            setVal.insert({it.first, it.second});
          }
        }
      }
    }
  
    if ((int)setVal.size() < _conf -> _ecK) {
      printf("ERROR: %s. Not enough helpers. \n", __func__);
      exit(1);
    }

    vector<pair<unsigned int, string>> vec = vector<pair<unsigned int, string>>(setVal.begin(), setVal.end());
    ret[requestorIP] = {blkName, vec};
    for (auto it : (*status)) 
    {
      if (it.second == 0 && it.first != blkName) 
      {
        ret[_blkName2Ip[it.first]] = {it.first, vec};
      }
    }

  }
  else if (opt % 10 == 1) { // opt == 1
    for (int i = 0; i < numOfFailed; i++) {
      setVal.clear();

      if (_conf -> _msscheduling) 
        setVal = getScheduledBlks(blkName, requestorIP, status, allStripeBlks);
      else {
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
      }

      vector<pair<unsigned int, string>> vec = vector<pair<unsigned int, string>>(setVal.begin(), setVal.end());
      
      ret[_blkName2Ip[failed[i]]] = {failed[i], vec};

      for (int j = 0; j < (int)survived.size(); j++) (*status)[survived[j].first] = 1;
    }

  }

  return ret;
}

unsigned int HDFS_MetadataBase::getIPfromBlk(const string& blkName) {
  if (!_blkName2Ip.count(blkName)) {
    printf("%s: no block %s !\n", __func__, blkName.c_str());
    exit(1);
  }
  return _blkName2Ip[blkName];
} 

set<pair<unsigned int, string>> HDFS_MetadataBase::getAllBlks(const string&blkName) {
  if (!_blkName2StripeId.count(blkName)) {
    printf("%s: no block %s !\n", __func__, blkName.c_str());
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
    printf("%s: ERROR: stripeId %d >= _Ip2BlkNames.size() %d\n", 
        __func__, stripeId, (int)_Ip2BlkNames.size());
    exit(1);
  }
} 

set<pair<unsigned int, string>> HDFS_MetadataBase::getScheduledBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status, set<pair<unsigned int, string>>& allStripeBlks) {
  set<pair<unsigned int, string>> setVal;
  map<unsigned int, int> statusInIP;
  set<unsigned int> usedIP;
  int stripeId = _blkName2StripeId[blkName];
  unsigned int maxOrder = 0, ecK = _conf -> _ecK;

  // status array in IP
  for (auto& it : allStripeBlks) statusInIP[it.first] = (*status)[it.second];  

  // Reset the orders if they are too large
  for (auto it : _order) {
    if (it.second > maxOrder) maxOrder = it.second;
    if (it.second >= MAX_ORDER) {
      for (auto& it : _order) it.second = 0;
      maxOrder = 0;
      break;
    }
  }

  _order[requestorIP]++;
  
  printf("%s: selected: ", __func__);
  while (setVal.size() < ecK) {
    unsigned int selectedIP, order = MAX_ORDER; 
    for (auto& it : _order) {
      if (!statusInIP[it.first] || usedIP.count(it.first)) continue;
      if (it.second < order) selectedIP = it.first, order = it.second;
    }

    if (order == MAX_ORDER || selectedIP == _conf -> _localIP) {
      exit(1);
    }
    printf(".%d ", selectedIP >> 24);
    setVal.insert({selectedIP, _Ip2BlkNames[stripeId][selectedIP]});
    usedIP.insert(selectedIP);
    _order[selectedIP]++; //  = maxOrder + 1;

  }

  printf("\n");
//  _order[requestorIP] = maxOrder + 1;

  return setVal;
}

set<pair<unsigned int, string>> HDFS_MetadataBase::getRandomBlks(const string& blkName, unsigned int requestorIP, map<string, int>* status, set<pair<unsigned int, string>>& allStripeBlks) {
  set<pair<unsigned int, string>> setVal;
  map<unsigned int, int> statusInIP;
  set<unsigned int> usedIP;
  int stripeId = _blkName2StripeId[blkName];
  unsigned int ecK = _conf -> _ecK;
  int survived = 0;

  vector<int> shuffle_array;
  map<int, unsigned int> id2IP;

  // status array in IP
  for (auto& it : allStripeBlks) {
    statusInIP[it.first] = (*status)[it.second];  
    if ((*status)[it.second]) {
      id2IP[survived] = it.first;
      survived++;
    }
  }

  for (int i = 0; i < survived; i++) shuffle_array.push_back(i);
  
  random_shuffle(shuffle_array.begin(), shuffle_array.end());

  printf("%s: selected: ", __func__);
  for (int i = 0; i < ecK; i++) {
    unsigned int selectedIP = id2IP[shuffle_array[i]]; 
    printf(".%d ", selectedIP >> 24);
    setVal.insert({selectedIP, _Ip2BlkNames[stripeId][selectedIP]});
    usedIP.insert(selectedIP);
  }
  printf("\n");

  return setVal;
}
