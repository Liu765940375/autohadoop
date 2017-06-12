#!/bin/bash
if [[ ! -z "$ZSH_NAME" ]]; then
    export BEAVER_HOME=$(dirname "$(cd $(dirname ${(%):-%x}) >/dev/null && pwd)")
else
    export BEAVER_HOME=$(dirname "$(cd $(dirname "${BASH_SOURCE[0]}") >/dev/null && pwd)")
fi

pexpect_file="../package/pexpect-2.3.tar.gz"

if [ ! -f "$pexpect_file" ];
then
    wget -P ../package http://10.239.47.156/python/pexpect-2.3.tar.gz
fi
cd ../package
tar zxvf pexpect-2.3.tar.gz
cd ../package/pexpect-2.3
python ./setup.py install
cd ~
export PYTHONPATH=$BEAVER_HOME
yum -y install gcc python-devel.x86_64 libffi-devel.x86_64 openssl-devel.x86_64
python $BEAVER_HOME/utils/get-pip.py
pip install --upgrade paramiko
pip install xlwt xlrd

yum -y install gawk
yum -y install sysstat
yum -y install perf
yum -y install python-matplotlib
pip install --upgrade numpy scipy matplotlib
pip install XlsxWriter
export no_proxy="127.0.0.1, localhost, *.intel.com, 10.239.47.*, *.sh.intel.com"

if [ ! -f "/usr/bin/pssh" ]; then
    wget http://10.239.47.156/software/pssh-2.3.1.tar.gz
    tar zxf pssh-2.3.1.tar.gz
    cd pssh-2.3.1
    python setup.py build
    python setup.py install
    cd ..
    rm -rf pssh-2.3.1.tar.gz
    rm -rf pssh-2.3.1
fi
