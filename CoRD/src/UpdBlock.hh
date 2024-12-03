#ifndef _UPD_BLOCK_HH_
#define _UPD_BLOCK_HH_

#include <iostream>
#include <string>

using namespace std;

class UpdBlock
{
  public: 
    int _id; 
    unsigned int _ip;
    int _offset_add;
    int _upd_size;

    UpdBlock() : _id(-1), _ip(0), _offset_add(0), _upd_size(0){};
    ~UpdBlock() = default;
    UpdBlock(int id, unsigned int ip, size_t offset_add, size_t upd_size): _id(id), _ip(ip), _offset_add(offset_add), _upd_size(upd_size){}
    void showblkInfo()
    {
        cout << "---upd blk info---" << endl
             << "_id = " << _id << endl
             << "_ip = ." << to_string((_ip >> 24) & 0xff) << endl
             << "_offset_add = " << _offset_add << endl
             << "_upd_size = " << _upd_size << endl << endl;
    }

};

#endif
