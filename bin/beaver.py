#!/usr/bin/python

import sys
import runBBonHoS
import runBBonSparkSQL

from infra.bigbench import *
from cluster.HiveOnSpark import *
from utils.util import *
from utils.node import *


current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)


default_conf = os.path.join(project_path, "conf")


def undeploy_components(custom_conf, hadoop_flg, hive_flg, spark_flg, bb_flg):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    if hadoop_flg:
        undeploy_hadoop(master, slaves, custom_conf)
    if hive_flg:
        undeploy_hive(master)
    if spark_flg:
        undeploy_spark(master)
    if bb_flg:
        undeploy_bb(master)


def deploy_components(custom_conf, hadoop_flg, hive_flg, spark_flg, bb_flg):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    if hadoop_flg:
        deploy_hadoop(default_conf, custom_conf, master, slaves, beaver_env)
    if hive_flg:
        deploy_hive(default_conf, custom_conf, master, beaver_env)
    if spark_flg:
        deploy_spark(default_conf, custom_conf, master, slaves, beaver_env)
        copy_lib_for_spark(master, beaver_env, custom_conf, True)
    if bb_flg:
        deploy_bb(default_conf, custom_conf, master)


def update_component_conf(custom_conf, hadoop_flg, hive_flg, spark_flg, bb_flg):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    if hadoop_flg:
        update_copy_hadoop_conf(default_conf, custom_conf, master, slaves, beaver_env)
    if hive_flg:
        update_copy_hive_conf(default_conf, custom_conf, master, beaver_env)
    if spark_flg:
        update_copy_spark_conf(master, slaves, default_conf, custom_conf, beaver_env)
    if bb_flg:
        update_copy_bb_conf(master, default_conf, custom_conf, beaver_env)


def restart_services(custom_conf, hadoop_flg, hive_flg, spark_flg):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    if hadoop_flg:
        start_hadoop_service(master, slaves, beaver_env)
    if hive_flg:
        start_hive_service(master, beaver_env)
    if spark_flg:
        start_spark_service(master, beaver_env)


def stop_services(custom_conf, hadoop_flg, hive_flg, spark_flg):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    if hadoop_flg:
        stop_hadoop_service(master, slaves)
    if hive_flg:
        stop_hive_service(master)
    if spark_flg:
        stop_spark_service(master)

def run_bigbench(custom_conf):
    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    run_BB(master, beaver_env)


def run_BB(custom_conf, hos_flg, sparkSQL_flg, run_flg):
    if run_flg == "deploy_run":
        if hos_flg:
            runBBonHoS.deploy_run(custom_conf)
        if sparkSQL_flg:
            runBBonSparkSQL.deploy_run(custom_conf)
    if run_flg == "replace_conf_run":
        if hos_flg:
            runBBonHoS.replace_conf_run(custom_conf)
        if sparkSQL_flg:
            runBBonSparkSQL.replace_conf_run(custom_conf)


def usage():
    print("Usage: bin/beaver.py [component] [action] [path/to/conf]/n")
    print("   Component option includes: Hadoop, HiveOnSpark, HiveOnTez, SparkSQL /n")
    print("   Action option includes: deploy, undeploy, replace_conf, start, stop, run /n")
    print("           deploy means just replacing configurations and trigger a run /n")
    print("           undeploy means remove all and redeploy a new run /n")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 4:
        usage()
    component = args[1]
    actions = args[2]
    conf_p = args[3]

    hadoop_flg = False
    hive_flg = False
    spark_flg = False
    bb_flg = False
    hos_flg=False
    sparkSQL_flg=False

    if component == "hadoop":
        hadoop_flg = True
    if component == "hive":
        hive_flg = True
    if component == "spark":
        spark_flg = True
    if component == "bb":
        bb_flg = True
    if component == "hos":
        hos_flg = True
    if component == "sparksql":
        sparkSQL_flg = True

    actions_list = actions.split(',')
    for action in actions_list:
        if action == "deploy":
            deploy_components(conf_p, hadoop_flg, hive_flg, spark_flg,bb_flg)
        elif action == "undeploy":
            undeploy_components(conf_p, hadoop_flg, hive_flg, spark_flg,bb_flg)
        elif action == "replace_conf":
            update_component_conf(conf_p, hadoop_flg, hive_flg, spark_flg, bb_flg)
        elif action == "start":
            restart_services(conf_p, hadoop_flg, hive_flg, spark_flg)
        elif action == "stop":
            stop_services(conf_p, hadoop_flg, hive_flg, spark_flg)
        elif action == "deploy_run":
            run_BB(conf_p, hos_flg, sparkSQL_flg, action)
        elif action == "replace_conf_run":
            run_BB(conf_p, hos_flg, sparkSQL_flg, action)
        else:
            usage()
