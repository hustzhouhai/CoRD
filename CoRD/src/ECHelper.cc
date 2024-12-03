#include <iostream>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "Config.hh"

#include "UPNode.hh"

#include "BlockReporter.hh"

#include "ConvDRWorker.hh"
#include "CyclDRWorker.hh"
#include "DRWorker.hh"
#include "PPRPullDRWorker.hh"
#include "PipeDRWorker.hh"
#include "PipeMulDRWorker.hh"

// Full node Recovery workers
#include "CyclMulFRWorker.hh"


using namespace std;

int main(int argc, char** argv) 
{
  setbuf(stdout, NULL);  // for debugging
  setbuf(stderr, NULL);

  Config* conf = new Config("conf/config.xml");
  

  if (conf -> _coordinatorIP == conf -> _localIP) 
  {
    printf("%s: ERROR: local IP is wrong\n", __func__);
    return 1;
  }

  UPNode* node = new UPNode(conf);
  //node -> debug();
  node -> doProcess();

  /*
  BlockReporter::report(conf -> _coordinatorIP, conf -> _blkDir.c_str(), conf -> _localIP);
  int workerThreadNum = conf -> _agWorkerThreadNum;
  thread thrds[workerThreadNum];
  DRWorker** drWorkers = (DRWorker**)calloc(sizeof(DRWorker*), workerThreadNum);
  for (int i = 0; i < workerThreadNum; i ++) 
  {
    if (conf -> _DRPolicy == "ecpipe") 
    {
      cout << "ECHelper: starting ECPipe helper" << endl;
      if (conf -> _ECPipePolicy == "cyclicSingle") 
        drWorkers[i] = new CyclDRWorker(conf);
      else if (conf -> _ECPipePolicy == "cyclic") 
        drWorkers[i] = new CyclMulFRWorker(conf);
      else if (conf -> _ECPipePolicy == "basic") 
        drWorkers[i] = new PipeMulDRWorker(conf);
      else if (conf -> _ECPipePolicy == "basicSingle") 
        drWorkers[i] = new PipeDRWorker(conf);
      else 
      {
        printf("%s: ECPipe policy ERROR: %s\n", argv[0], conf -> _DRPolicy.c_str());
        return 1;
      }
    } 
    else if (conf -> _DRPolicy == "ppr") 
    {
      drWorkers[i] = new PPRPullDRWorker(conf);
    } 
    else if (conf -> _DRPolicy == "conv") 
    {
      drWorkers[i] = new ConvDRWorker(conf);
    } 
    else 
    {
      printf("%s: Degraded Read policy ERROR: %s\n", argv[0], conf -> _DRPolicy.c_str());
      return 1;
    }

    thrds[i] = thread([=]{drWorkers[i] -> doProcess();});
  }
  
  for (int i = 0; i < workerThreadNum; i ++) 
  {
    thrds[i].join();
  }
  */
  return 0;
}

