#!/usr/bin/python

import sys
import os
from cluster.HiveOnMR import *
from cluster.SparkSQL import *
from infra.bigbench import *

def deploy_bigbench(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    update_mr_bb(custom_conf)
    deploy_bb(default_conf, custom_conf, master)

def update_mr_bb(custom_conf):
    bb_custom_hive_engineSettings_sql = os.path.join(custom_conf, "BB/engines/hive/conf/engineSettings.sql")
    update_mr_bb_settings_sql(bb_custom_hive_engineSettings_sql)

def update_mr_bb_settings_sql(conf_file):
    with open(conf_file, 'r') as file_read:
        total_line = file_read.read()
    origin_pattern = r'set hive.execution.engine=.*;'
    replace_pattern = 'set hive.execution.engine=mr;'
    new_total_line = re.sub(origin_pattern, replace_pattern, total_line)
    with open(conf_file, 'w') as file_write:
        file_write.write(new_total_line)

def replace_conf_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    populate_hive_on_mr_conf(custom_conf)
    restart_hive_on_mr(custom_conf)
    populate_bb_conf(master, default_conf, custom_conf, beaver_env)
    run_BB(master, beaver_env)


def deploy_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_hive_on_mr(custom_conf)
    deploy_hive_on_mr(custom_conf)
    start_hive_on_mr(custom_conf)
    deploy_bigbench(custom_conf)
    run_BB(master, beaver_env)

def undeploy_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_hive_on_mr(custom_conf)
    undeploy_spark_sql(custom_conf)
    undeploy_bb(master)

def usage():
    print("Usage: sbin/runBBonHoS.sh [action] [path/to/conf]/n")
    print("   Action option includes: deploy_run, replace_conf_run, undeploy /n")
    print("           replace_conf_run means just replacing configurations and trigger a run /n")
    print("           deploy_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        usage()
    action = args[1]
    conf_p = args[2]
    if action == "replace_conf_run":
        replace_conf_run(conf_p)
    elif action == "deploy_run":
        deploy_run(conf_p)
    elif action == "undeploy":
        undeploy_run(conf_p)
    else:
        usage()
	exit(0)

