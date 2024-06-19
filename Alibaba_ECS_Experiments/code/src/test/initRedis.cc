#include <iostream>
#include <string>

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

void cmdCallback(redisAsyncContext *c, void *r, void *privdata) {
  cout << "cmd sent" << endl;
  redisAsyncDisconnect(c);
}

void rpushCallback(redisAsyncContext *c, void *r, void *privdata) {
  char* data = (char*) privdata;
  free(data);
  if (++ __sentCnt == PACKET_CNT) {
    struct timeval tv2;
    gettimeofday(&tv2, NULL);
    cout << "ends at " << tv2.tv_sec << "." << tv2.tv_usec << endl;
    redisAsyncDisconnect(c);
  }
}

void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
    printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Error: %s\n", c->errstr);
        return;
    }
    printf("Disconnected...\n");
}

int main(int argc, char** argv) {
  if (argc < 4) {
    cout << "Usage: " << argv[0] << " [file name] [next ip] [local ip]" << endl;
    return 0;
  }
  signal(SIGPIPE, SIG_IGN);
  struct event_base *ebase = event_base_new();

  redisAsyncContext* rContext = redisAsyncConnect(argv[2], 6379);
  if (rContext -> err) {
    /* Let *c leak for now... */
    printf("Error: %s\n", rContext -> errstr);
    return 1;
  }
  redisLibeventAttach(rContext, ebase);

  redisAsyncSetConnectCallback(rContext, connectCallback);
  redisAsyncSetDisconnectCallback(rContext, disconnectCallback);
  redisAsyncCommand(rContext, NULL, NULL, "FLUSHALL");

  char cmd[256];
  string filename("testFile");
  string localFileName("testFile");
  unsigned int ip = inet_addr(argv[3]), id = 1, ecK = 3, requestedFileNameLen = filename.length();
  int localFileNameLen = localFileName.length();

  memcpy(cmd, (char*)&ecK, 4);
  memcpy(cmd + 12, (char*)&ip, 4);
  memcpy(cmd + 16, (char*)&id, 4);
  memcpy(cmd + 20, (char*)&requestedFileNameLen, 4);
  memcpy(cmd + 24, filename.c_str(), requestedFileNameLen);
  int localFileBase = 24 + requestedFileNameLen;
  memcpy(cmd + localFileBase, (char*)&localFileNameLen, 4);
  memcpy(cmd + localFileBase + 4, localFileName.c_str(), localFileNameLen);

  redisAsyncCommand(rContext, cmdCallback, NULL, "RPUSH dr_cmds %b", 
      cmd, localFileBase + 4 + localFileNameLen);

  timeval tv1, tv2;
  
  int fd = open(argv[1], O_RDONLY), fetched, base = 0;
  char** data = (char**)malloc(PACKET_CNT * sizeof(char*));
  for (int i = 0; i < PACKET_CNT; i ++) {
    data[i] = (char*)malloc(PACKET_SIZE * sizeof(char));
  }
  gettimeofday(&tv1, NULL);
  __sentCnt = 0;
  cout << "request starts at " << tv1.tv_sec << "." << tv1.tv_usec << endl;
  for (int i = 0; i < PACKET_CNT; i ++) {
    fetched = 0;
    //cout << i << endl;
    while (fetched < PACKET_SIZE) {
      fetched += pread(fd, data[i] + fetched, PACKET_SIZE - fetched, base + fetched);
      //cout << " " << fetched << endl;
    }
    sleep(2);
    redisAsyncCommand(rContext, rpushCallback, data[i], "RPUSH tmp:%s %b", 
        argv[1], data[i], PACKET_SIZE);
    base += PACKET_SIZE;
  }
  close(fd);
  gettimeofday(&tv2, NULL);

  event_base_dispatch(ebase);

  return 0;
}
