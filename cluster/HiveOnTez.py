#!/usr/bin/python

import sys
from utils.node import *
from utils.util import *
from infra.hadoop import *
from infra.hive import *
from infra.tez import *
from infra.spark import *

default_conf = os.path.join(project_path, "conf")

def deploy_hive_on_tez(custom_conf):
    # Deploy Hadoop
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_hadoop(default_conf, custom_conf, master, slaves, beaver_env)

    # Deploy Hive
    deploy_hive(default_conf, custom_conf, master, beaver_env)
    # Deploy Spark
    deploy_spark(default_conf, custom_conf, master, slaves, beaver_env)
    # Deploy Tez
    deploy_tez_internal(default_conf, custom_conf, master, beaver_env)

def undeploy_hive_on_tez(custom_conf, beaver_env):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    undeploy_hadoop(master, slaves, custom_conf, beaver_env)
    undeploy_hive(master)
    undeploy_tez(master)
    undeploy_spark(master)

def populate_hive_on_tez_conf(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_tez_internal(default_conf, custom_conf, master, beaver_env)
    update_copy_hadoop_conf(default_conf, custom_conf, master, slaves, beaver_env)
    update_copy_hive_conf(default_conf, custom_conf, master, beaver_env)
    update_copy_spark_conf(master, slaves, default_conf, custom_conf, beaver_env)
    copy_tez_conf_to_hadoop(default_conf, custom_conf,[master], beaver_env)

def start_hive_on_tez(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    start_hadoop_service(master, slaves, beaver_env)
    copy_tez_package_to_hadoop(master)
    start_hive_service(master, beaver_env)
    start_spark_history_server(master, beaver_env)

def stop_hive_on_tez(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    stop_spark_history_server(master)
    stop_hive_service(master)
    stop_hadoop_service(master, slaves)

def restart_hive_on_tez(custom_conf):
    stop_hive_on_tez(custom_conf)
    start_hive_on_tez(custom_conf)
