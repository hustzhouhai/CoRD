#include "BlockReporter.hh"

void BlockReporter::report(unsigned int coIP, const char* blkDir, unsigned int localIP) 
{
  cerr << blkDir << endl;
  DIR* dir;
  FILE* file;
  struct dirent* ent;

  string rServer;
  rServer += to_string(coIP & 0xff);
  rServer += ".";
  rServer += to_string((coIP >> 8) & 0xff);
  rServer += ".";
  rServer += to_string((coIP >> 16) & 0xff);
  rServer += ".";
  rServer += to_string((coIP >> 24) & 0xff);

  cout << "coordinator server: " << rServer << endl;
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout(rServer.c_str(), 6379, timeout);
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

  char info[256];
 
  if ((dir = opendir(blkDir)) != NULL) 
  {
    while ((ent = readdir(dir)) != NULL) 
    {
        string strName(ent -> d_name);
      if (strcmp(ent -> d_name, ".") == 0 || strcmp(ent -> d_name, "..") == 0 || strName.find("meta") == 30) 
      {
        continue;
      }
      //add by Zuoru: add to handle the multiple dir in Hadoop3
      if (ent -> d_type == 4) // 4: means this file is a dir instead of files.
      {
        string dir(blkDir);
        string subBlkDir =  dir + '/' + strName;
        DIR* subdir;
        if((subdir = opendir(subBlkDir.c_str())) != NULL)
        {
            while((ent = readdir(subdir)) != NULL) 
            {
                string curName(ent -> d_name);
                if (strcmp(ent -> d_name, ".") == 0 || strcmp(ent -> d_name, "..") == 0 || curName.find("meta") == 30) 
                {
                    continue;
                }
                memcpy(info, (char*)&localIP, 4);
                memcpy(info + 4, ent -> d_name, strlen(ent -> d_name));
                rReply = (redisReply*)redisCommand(rContext,
                "RPUSH blk_init %b", info, 4 + strlen(ent -> d_name));
                freeReplyObject(rReply);
            }
            closedir(subdir);
        }
      } 
      else if (ent -> d_type == 8) // 8: means this file is a file that can read directly;
      {
        memcpy(info, (char*)&localIP, 4);
        memcpy(info + 4, ent -> d_name, strlen(ent -> d_name));
        rReply = (redisReply*)redisCommand(rContext, "RPUSH blk_init %b", info, 4 + strlen(ent -> d_name));
        freeReplyObject(rReply);
      }
    }
    closedir(dir);
  } 
  else 
  {
    // TODO: error handling
    cerr << "BlockReporter::report() opening directory error" << endl;
  }
  /////
  redisFree(rContext);
}

