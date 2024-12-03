#include <iostream>
#include <thread>
#include <vector>

#include "PipeCoordinator.hh"
#include "PipeMulCoordinator.hh"
#include "ConvCoordinator.hh"
#include "CyclCoordinator.hh"
#include "PPRCoordinator.hh"
#include "MetadataBase.hh"
#include "UPCoordinator.hh"

using namespace std;


int main(int argc, char** argv) 
{
  Coordinator *coord;
  if (conf -> _DRPolicy == "ppr") 
  {
    cout << "ECCoordinator: starting PPR coordinator" << endl;
    coord = new PPRCoordinator(conf);
  } 
  else if (conf -> _DRPolicy == "conv") 
  {
    cout << "ECCoordinator: starting conventional coordinator" << endl;
    coord = new ConvCoordinator(conf);
  } 
  else if (conf -> _DRPolicy == "ecpipe") 
  {
    cout << "ECCoordinator: starting ECPipe coordinator" << endl;
    if (conf -> _ECPipePolicy == "extCyclic") coord = new CyclCoordinator(conf);
    else if (conf -> _ECPipePolicy.find("Single") != string::npos) 
    {
      cout << "Selecting PipeCoordinator\n";
      coord = new PipeCoordinator(conf);
    }
    else 
    {
      cout << "Selecting PipeMulCoordinator\n";
      coord = new PipeMulCoordinator(conf);
    }
  }
  
  coord -> doProcess();
  
  
  return 0;
}

