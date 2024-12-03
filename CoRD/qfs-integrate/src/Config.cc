#include "Config.hh"

Config::Config(std::string confFile) {
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
//    else if (attName == "helper.worker.thread.num")
//        _agWorkerThreadNum = std::stoi(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "degraded.read.policy")
        _DRPolicy = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "ecpipe.policy")
        _ECPipePolicy = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "file.system.type") {
        _fileSysType = ele -> NextSiblingElement("value") -> GetText();
	if (_fileSysType == "standalone") {
	  std::cout << "standalone mode share the same block format and stripe store format with HDFS mode" << std::endl;
	  _fileSysType = "HDFS";
	}
        if (_fileSysType == "QFS") {
	  _packetSkipSize = 16384;
 	}
    } else if (attName == "stripe.store")
        _stripeStore = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "block.directory")
        _blkDir = ele -> NextSiblingElement("value") -> GetText();
    else if (attName == "coordinator.address")
        _coordinatorIP = inet_addr(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "local.ip.address")
        _localIP = inet_addr(ele -> NextSiblingElement("value") -> GetText());
    else if (attName == "helpers.address") {
      for (ele = ele -> NextSiblingElement("value"); ele != NULL; ele = ele -> NextSiblingElement("value")) {
	std::string tempstr=ele -> GetText();
	int pos = tempstr.find("/");
	int len = tempstr.length();
	std::string rack=tempstr.substr(0, pos);
	std::string ip=tempstr.substr(pos+1, len-pos-1);
	_helpersIPs.push_back(inet_addr(ip.c_str()));
	_rackInfos[inet_addr(ip.c_str())] = rack;
//        _helpersIPs.push_back(inet_addr(ele -> GetText()));
      }
      sort(_helpersIPs.begin(), _helpersIPs.end());
    }
    else if (attName == "path.selection.enabled")
        _pathSelectionEnabled = strcmp("true", ele -> NextSiblingElement("value") -> GetText()) == 0 ? true : false;
    else if (attName == "link.weight.config.file")
        _linkWeightConfigFile = ele -> NextSiblingElement("value") -> GetText();
  }
}

void Config::display() {
  std::cout << "Global info: " << std::endl;
  std::cout << "_ecK = " << _ecK << std::endl;
  std::cout << "_packetSize = " << _packetSize << std::endl;
  std::cout << "_packetSkipSize = " << _packetSkipSize << std::endl;
  std::cout << "_packetCnt = " << _packetCnt << std::endl << std::endl;
  std::cout << "_fileSysType = " << _fileSysType << std::endl << std::endl;

  std::cout << "Coordinator info: " << std::endl;
  std::cout << "\t _coordinatorIP = " << _coordinatorIP << std::endl;
  std::cout << "\t _coCmdReqHandlerThreadNum = " << _coCmdReqHandlerThreadNum << std::endl;
  std::cout << "\t _coCmdDistThreadNum = " << _coCmdDistThreadNum << std::endl << std::endl;

  std::cout << "helper info: " << std::endl;
  std::cout << "\t _agWorkerThreadNum = " << _agWorkerThreadNum << std::endl;
  std::cout << "\t _helpersIPs: " << std::endl;
  for (auto it : _helpersIPs) {
    std::cout << "\t\t" << it << std::endl;
  }
}




