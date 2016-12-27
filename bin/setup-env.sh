#!/bin/bash
if [[ ! -z "$ZSH_NAME" ]]; then
    export BEAVER_HOME=$(dirname "$(cd $(dirname ${(%):-%x}) >/dev/null && pwd)")
else
    export BEAVER_HOME=$(dirname "$(cd $(dirname "${BASH_SOURCE[0]}") >/dev/null && pwd)")
fi

export PYTHONPATH=$BEAVER_HOME
yum -y install gcc python-devel.x86_64 libffi-devel.x86_64 openssl-devel.x86_64
python $BEAVER_HOME/utils/get-pip.py
pip install paramiko
export no_proxy="127.0.0.1, localhost, *.intel.com, 10.239.47.*, *.sh.intel.com" 