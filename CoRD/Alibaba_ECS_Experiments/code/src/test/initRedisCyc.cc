#include <iostream>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <hiredis/adapters/libevent.h>
#include <hiredis/async.h>
#include <hiredis/hiredis.h>
#include <netinet/in.h>
#include <signal.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define PACKET_SIZE 1048576
#define PACKET_CNT 64

using namespace std;

static int __sentCnt;

int main(int argc, char** argv) {
  if (argc < 4) {
    cout << "Usage: " << argv[0] << " [file name] [next ip] [local ip]" << endl;
    return 0;
  }

  struct timeval timeout = {1, 500000};
  redisContext* rContext = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
  if (rContext -> err) {
    /* Let *c leak for now... */
    printf("Error: %s\n", rContext -> errstr);
    return 1;
  }

  redisReply* rReply = (redisReply*)redisCommand(rContext, "FLUSHALL");
  freeReplyObject(rReply);

  char cmd[256];
  string filename("blkDir/blk_1");
  string localFileName("blkDir/blk_1");
  unsigned int ip = inet_addr(argv[3]), 
               rip = inet_addr("192.168.0.25"), 
               nip = inet_addr("192.168.0.23"),
               id = 0, ecK = 3, requestedFileNameLen = strlen(argv[1]);
  int localFileNameLen = requestedFileNameLen;

  memcpy(cmd, (char*)&ecK, 4);
  memcpy(cmd + 4, (char*)&rip, 4);
  memcpy(cmd + 12, (char*)&nip, 4);
  memcpy(cmd + 16, (char*)&id, 4);
  memcpy(cmd + 20, (char*)&requestedFileNameLen, 4);
  memcpy(cmd + 24, argv[1], requestedFileNameLen);
  int localFileBase = 24 + requestedFileNameLen;
  memcpy(cmd + localFileBase, (char*)&localFileNameLen, 4);
  memcpy(cmd + localFileBase + 4, argv[1], localFileNameLen);

  rReply = (redisReply*)redisCommand(rContext, "RPUSH dr_cmds %b", 
      cmd, localFileBase + 4 + localFileNameLen);
  freeReplyObject(rReply);

  timeval tv1, tv2;
  
  int fd = open(filename.c_str(), O_RDONLY), fetched, base = 0;
  char** data = (char**)malloc(PACKET_CNT * sizeof(char*));
  for (int i = 0; i < PACKET_CNT; i ++) {
    data[i] = (char*)malloc(PACKET_SIZE * sizeof(char));
  }
  gettimeofday(&tv1, NULL);
  __sentCnt = 0;

  vector<int> pos;
  for (int i = 0; i < 32; i ++) {
    pos.push_back(i * 2 + 1);
    pos.push_back(i * 2);
  }
  for (auto it : pos) {
    cout << __sentCnt << " "<< it << endl;
    base = it * PACKET_SIZE;
    fetched = 0;
    //cout << i << endl;
    while (fetched < PACKET_SIZE) {
      fetched += pread(fd, data[it] + fetched, PACKET_SIZE - fetched, base + fetched);
      //cout << " " << fetched << endl;
    }
    rReply = (redisReply*)redisCommand(rContext, "RPUSH tmp:%s %b", 
        argv[1], data[it], PACKET_SIZE);
    freeReplyObject(rReply);
  }
  close(fd);
  gettimeofday(&tv2, NULL);

  //event_base_dispatch(ebase);

  return 0;
}
