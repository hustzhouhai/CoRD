#include <sys/time.h>
#include <iostream>
#include <string>
#include <array>
#include <stdio.h>
#include <sys/types.h>

std::string exec(const char* cmd){
    FILE* pipe = popen(cmd,"r");
    if(!pipe) return "ERROE, the command undone";

    char buffer[256];
    std::string result("");
    while(!feof(pipe))
    {
        if(fgets(buffer, 256, pipe) != NULL)
        {
            result += buffer;
        }
    }
    pclose(pipe);
    return result;

}

int main(int argc, char** argv){
    std::string CmdFsck("hdfs fsck /test_ec/TestFile -files -blocks -locations ");
    std::string GetResult;
    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);
    GetResult = exec(CmdFsck.c_str());
    gettimeofday(&tv2, NULL);
    std::cout << "overall time: " << ((tv2.tv_sec - tv1.tv_sec) * 1000000.0 + tv2.tv_usec - tv1.tv_usec) / 1000000.0 << std::endl;
    std::cout << "This result is: " << GetResult << std::endl;
    return 0;

}
