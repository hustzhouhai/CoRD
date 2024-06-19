#!/bin/bash

WK_DIR=ins_pac

apt install sudo

sudo apt-get update

sudo apt-get install -y g++ make psmisc git

cd $WK_DIR

# install redis 3.2.8
tar -zxvf redis-3.2.8.tar.gz
cd redis-3.2.8/
make
sudo make install

cd utils/
sudo ./install_server.sh

sudo service redis_6379 stop
sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/6379.conf
sudo sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/6379.conf
sudo service redis_6379 start

cd ..
cd ..

# install hiredis
tar -zxvf hiredis.tar.gz
cd hiredis/
make 
sudo make install

cd ..

# install wondershaper
tar -zxvf wondershaper.tar.gz
cd wondershaper/
sudo make install

cd ..

# install gf-complete
tar -zxvf gf-complete.tar.gz
cd gf-complete/
chmod +x ./autogen.sh
./autogen.sh
./configure
make
sudo make install

sudo cp /usr/local/lib/libgf_complete.so.1 /usr/lib/ 
sudo ldconfig

cd ..
cd ..

