from utils.node import *
from infra.hive import *
from infra.spark import *
from infra.other_components import *

default_conf = os.path.join(project_path, "conf")


def link_spark_defaults(custom_conf):
    print("create a link file at the Hive path")


def deploy_hive_on_spark(custom_conf):
    # Deploy Hadoop
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_hadoop(default_conf, custom_conf, master, slaves, beaver_env)

    # Deploy Spark
    deploy_spark(default_conf, custom_conf, master, slaves, beaver_env)

    # Deploy Hive
    deploy_hive_internal(default_conf, custom_conf, master, beaver_env)
    copy_lib_for_spark(master, beaver_env, True)
    link_spark_defaults(custom_conf)

def populate_hive_on_spark_conf(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    update_copy_hadoop_conf(default_conf, custom_conf, master, slaves, beaver_env)
    update_copy_hive_conf(default_conf, custom_conf, master, beaver_env)
    update_copy_spark_conf(master, slaves, default_conf, custom_conf, beaver_env)


def start_hive_on_spark(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    start_hadoop_service(master, slaves, beaver_env)
    start_spark_history_server(master, beaver_env)
    start_hive_service(master, beaver_env)


def stop_hive_on_spark(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    stop_spark_history_server(master)
    stop_hive_service(master)
    stop_hadoop_service(master, slaves)


def restart_hive_on_spark(custom_conf):
    stop_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)


def undeploy_hive_on_spark(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    undeploy_hadoop(master, slaves, custom_conf)
    undeploy_hive(master)
    undeploy_spark(master)
