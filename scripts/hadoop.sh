#!/bin/bash

SCRIPT_PATH="`dirname $0`/../scripts"
CONFIG_SCRIPT=$SCRIPT_PATH/py/config.py

function gen_hadoop_conf(){
    echo "gen_hadoop_conf"
}

function setup_hadoop_env(){
    echo "setup_hadoop_env"
}

# APIs
function start_hadoop(){
    echo "start_hadoop"
}

function restart_hadoop(){
    echo "restart_hadoop"
}

function stop_hadoop(){
    echo "stop_hadoop"
}

function deploy_hadoop(){
    ./deploy_slave.py
}

function remove_hadoop(){
    echo "remove_hadoop"
}
