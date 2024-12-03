#include "Config.hh"

Config::Config(std::string confFile) 
{
  XMLDocument doc;
  doc.LoadFile(confFile.c_str());
  XMLElement* element;
  for(element = doc.FirstChildElement("setting")->FirstChildElement("attribute");
      element!=NULL;
      element=element->NextSiblingElement("attribute")){
    XMLElement* ele = element->FirstChildElement("name");
    std::string attName = ele -> GetText();
    if (attName == "erasure.code.k")
        _ecK = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "erasure.code.n")
        _ecN = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "rs.code.config.file")
        _rsConfigFile = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "packet.count")
        _packetCnt = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "packet.size")
        _packetSize = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "packet.skipsize")
        _packetSkipSize = std::stoi(ele -> NextSiblingElement("value") -> GetText());
//    else if (attName == "coordinator.request.handler.thread.num")
//        _coCmdReqHandlerThreadNum = std::stoi(ele -> NextSiblingElement("value") -> GetText());
//    else if (attName == "coordinator.distributor.thread.num")
//        _coCmdDistThreadNum = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "helper.worker.thread.num")
        _agWorkerThreadNum = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "degraded.read.policy")
        _DRPolicy = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "update.policy")
        _UPPolicy = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "update.opt")
        _flipOpt = strcmp("true", ele -> NextSiblingElement("value") -> GetText()) == 0 ? true : false;
    else if (attName == "log.size(MB)")
        _log_size = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "block.size(KB)")
        _blocksize = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "update.request.way")
        _UPWay = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "ecpipe.policy")
        _ECPipePolicy = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "file.system.type") {
        _fileSysType = ele -> NextSiblingElement("value") -> GetText();
	if (_fileSysType == "standalone") {
	  //std::cout << "standalone mode share the same block format and stripe store format with HDFS mode" << std::endl;
	  _fileSysType = "HDFS";
	}
        if (_fileSysType == "QFS") {
	  _packetSkipSize = 16384;
 	}
    } else if (attName == "stripe.store")
        _stripeStore = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "block.directory")
        _blkDir = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "update.block.directory")
        _updDataDir = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "trace.directory")
        _traceDir = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "trace.type")
        _traceType = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "coordinator.address")
        _coordinatorIP = inet_addr(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "local.ip.address")
    {
      std::cout << "local ip: " << ele -> NextSiblingElement("value") -> GetText() << std::endl;
       _localIP = inet_addr(ele -> NextSiblingElement("value") -> GetText());
    }
        
    else if (attName == "helpers.address") 
    {
      for (ele = ele -> NextSiblingElement("value"); ele != NULL; ele = ele -> NextSiblingElement("value")) 
      {
        std::string tempstr=ele -> GetText();
        int pos = tempstr.find("/");
        int len = tempstr.length();
        std::string rack=tempstr.substr(0, pos);
        std::string ip=tempstr.substr(pos+1, len-pos-1);
        _helpersIPs.push_back(inet_addr(ip.c_str()));
        _rackInfos[inet_addr(ip.c_str())] = rack;
//        _helpersIPs.push_back(inet_addr(ele -> GetText()));
      }
      //sort(_helpersIPs.begin(), _helpersIPs.end());
    }
    else if (attName == "path.selection.enabled")
        _pathSelectionEnabled = strcmp("true", ele -> NextSiblingElement("value") -> GetText()) == 0 ? true : false;
    else if (attName == "link.weight.config.file")
        _linkWeightConfigFile = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "rack.aware.enable") {
        bool result = strcmp("true", ele -> NextSiblingElement("value") -> GetText()) == 0 ? true : false;
        if(result){
          _rackAware = 1;
          _shuffle = 0;
        }else{
          _rackAware = 0;
          _shuffle = 1; 
        }
    }
    else if (attName == "rack.load.balance"){
      _loadbalance = strcmp("true", ele -> NextSiblingElement("value") -> GetText()) == 0 ? true : false;
    }
    else if (attName == "multi.stripe.scheduling"){
      _msscheduling = strcmp("true", ele -> NextSiblingElement("value") -> GetText()) == 0 ? true : false;
    }
  }

  if (_fileSysType == "HDFS3") {
    std::cout << "FSTYPE: HDFS3" << std::endl;
  }

}

void Config::display() {
  std::cout << "Global info: " << std::endl;
  std::cout << "_ecK = " << _ecK << std::endl;
  
}





