#!/bin/bash
iperf3 -v
if [ $? -ne 0 ];then
wget -P /opt http://10.239.47.156/software/iperf-3.0.6.tar.gz
tar zxvf /opt/iperf-3.0.6.tar.gz -C /opt
cd /opt/iperf-3.0.6
./configure
make
make install
rm /opt/iperf-3.0.6.tar.gz
fi
