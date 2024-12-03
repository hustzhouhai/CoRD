#include <iostream>
#include <string>

#include <hiredis/hiredis.h>

#include "ECPipeInputStream.hh"
#include "Config.hh"

using namespace std;

int main(int argc, char** argv) 
{
  if (argc < 2) 
  {
    cout << "Us age: " << argv[0] << " [lost file names]" << endl;
    return 0;
  }
  Config* conf = new Config("conf/config.xml");
  
  // modified by jhli
  // string filename(argv[1]);
  vector<string> filenames;
  for (int i = 1; i < argc; i++) 
  {
    filenames.push_back(string(argv[i]));
  } 

  ECPipeInputStream cip(conf, 
                        conf->_packetCnt, 
                        conf->_packetSize, 
                        conf->_DRPolicy, 
                        conf->_ECPipePolicy, 
                        conf->_coordinatorIP, 
                        conf->_localIP, filenames);

  // cip.output2File("testfileOut");
  return 0;
}

