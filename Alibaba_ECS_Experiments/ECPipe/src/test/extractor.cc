#include <iostream>
#include <string>

#include <string.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../Util/hiredis.h"

#define PACKET_SIZE 1048576
#define PACKET_CNT 64

using namespace std;

int main(int argc, char** argv) {
  if (argc < 2) {
    cout << "Usage: " << argv[0] << " [file name]";
    return 0;
  }
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
  if (rContext == NULL || rContext -> err) {
    if (rContext) {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } else {
      cerr << "Connection error: can't allocate redis context" << endl;
    }
    return 0;
  }
  redisReply* rReply;
  
  string outputFile = string(argv[1]) + "Out";
  int fd = open(outputFile.c_str(), O_WRONLY | O_CREAT, 0644), wrote, base = 0;

  for (int i = 0; i < PACKET_CNT; i ++) {
    //cout << "reading packet[" << i << "]" << endl;
    rReply = (redisReply*)redisCommand(rContext, "BLPOP tmp:%s 100", 
        argv[1]);
    const char* data = rReply -> element[1] -> str;

    //cout << rReply -> element[1] -> len << endl;
    for (int j = 0; j < PACKET_SIZE; j ++) {
      //if (data[j] != 0) cout << "something wrong happened" << endl;
      ;
    }

    wrote = 0;
    while (wrote < PACKET_SIZE) {
      wrote += pwrite(fd, data + wrote, PACKET_SIZE - wrote, base + wrote);
      //cout << " wrote: " << wrote << endl;
    }
    base += PACKET_SIZE;
    freeReplyObject(rReply);
  }
  close(fd);

  redisFree(rContext);

  return 0;
}
