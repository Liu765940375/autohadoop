#!/bin/bash
if [[ ! -z "$ZSH_NAME" ]]; then
    export BEAVER_HOME=$(dirname "$(cd $(dirname ${(%):-%x}) >/dev/null && pwd)")
else
    export BEAVER_HOME=$(dirname "$(cd $(dirname "${BASH_SOURCE[0]}") >/dev/null && pwd)")
fi

export PYTHONPATH=$BEAVER_HOME
yum -y install gcc python-devel.x86_64 libffi-devel.x86_64 openssl-devel.x86_64
python $BEAVER_HOME/utils/get-pip.py
pip install --upgrade paramiko
pip install xlwt xlrd

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
