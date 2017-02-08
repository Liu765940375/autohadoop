#!bin/bash

alias_python="alias python='python2.7'"
bashalias_file=".bash_aliases"
python_file="../package/Python-2.7.13.tgz"


if [ ! -f "$python_file" ];
then
    wget -P ../package http://10.239.47.156/software/Python-2.7.13.tgz
fi

cd ../package
tar zxvf Python-2.7.13.tgz
cd ../package/Python-2.7.13
./configure
make
make install

if [ ! -f "$bashalias_file" ];
then
    cd ~/
    touch "$bashalias_file"
    echo $alias_python >> "$bashalias_file"
else
    echo $alias_python >> "$bashalias_file"
fi
source /root/.bashrc