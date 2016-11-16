#!/bin/bash

SCRIPT_PATH="`dirname $0`/../scripts"
CONFIG_SCRIPT=$SCRIPT_PATH/py/config.py

function gen_hadoop_conf(){

}

function setup_hadoop_env(){

}

# APIs
function start_hadoop(){

}

function restart_hadoop(){

}

function stop_hadoop(){

}

function deploy_hadoop(){
  python $CONFIG_SCRIPT $CORE_SITE_TEMPLATE_PATH $CORE_SITE_CONF_NAME $CORE_SITE_PATH
  python $CONFIG_SCRIPT $HDFS_SITE_TEMPLATE_PATH $HDFS_SITE_CONF_NAME $HDFS_SITE_PATH
  python $CONFIG_SCRIPT $MAPRED_SITE_TEMPLATE_PATH $MAPRED_SITE_CONF_NAME $MAPRED_SITE_PATH
  python $CONFIG_SCRIPT $YARN_SITE_TEPMLATE_PATH $YARN_SITE_CONF_NAME $YARN_SITE_PATH
}

function remove_hadoop(){

}
