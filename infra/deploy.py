#!/usr/bin/python

from utils.node import *
from infra.hadoop import *
from infra.hive import *
from infra.spark import *
from infra.bigbench import *
from infra.other_components import *

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
default_conf = os.path.join(project_path, "conf")

# for all kinds fo deployment, hadoop should be deployed first to setup nopass, stop firewall.

def deploy_hos(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_start_hadoop(default_conf, custom_conf, master, slaves, beaver_env)
    deploy_start_hive_internal(default_conf, custom_conf, master, beaver_env)
    deploy_start_spark(default_conf, master, custom_conf, beaver_env)
    copy_lib_for_spark(master, beaver_env, True)
    deploy_bb(default_conf, custom_conf, master)

def deploy_spark_sql(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_start_hadoop(default_conf, custom_conf, master, slaves, beaver_env)
    deploy_start_spark(default_conf, master, custom_conf, beaver_env)
    copy_lib_for_spark(master, beaver_env, True)
    deploy_bb(default_conf, custom_conf, master)

def deploy_hive_sql(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_start_hadoop(default_conf, custom_conf, master, slaves, beaver_env)
    deploy_start_hive_internal(default_conf, custom_conf, master, beaver_env)
    deploy_bb(default_conf, custom_conf, master)

def update_conf_all(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    update_copy_hadoop_conf(default_conf, custom_conf, master, slaves, beaver_env)
    update_copy_hive_conf(default_conf, custom_conf, master, beaver_env)
    update_copy_spark_conf(master, default_conf, custom_conf, beaver_env)
    update_copy_bb_conf(master, default_conf, custom_conf, beaver_env)

def restart_services(custom_conf, restart_hadoop, restart_hive, restart_spark):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    if restart_hadoop:
        start_hadoop_service(master, slaves, beaver_env)
    if restart_hive:
        start_hive_service(master, beaver_env)
    if restart_spark:
        start_spark_service(master, beaver_env)

def clean_components(custom_conf, clean_hadoop, clean_hive, clean_spark):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    if clean_hadoop:
        clean_hadoop(slaves)
    if clean_hive:
        clean_hive(master)
    if clean_spark:
        clean_spark(master)

if __name__ == '__main__':
    custom_conf = "/home/colin/project/beaver/source/Beaver/colin"
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    #restart_services(custom_conf, True, False, False)
    #deploy_start_hadoop(default_conf, custom_conf, master, slaves, beaver_env)
    #stop_hadoop_service(master, slaves)
    #deploy_start_hive(default_conf, custom_conf, master, beaver_env)
    #deploy_start_spark(default_conf, custom_conf, master, beaver_env)
    deploy_bb(default_conf, custom_conf, master)


