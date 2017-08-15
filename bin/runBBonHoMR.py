#!/usr/bin/python

import sys
import os
from cluster.HiveOnMR import *
from cluster.SparkSQL import *
from infra.bigbench import *

spark_Phive_component="spark-Phive"
def deploy_bigbench(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    update_mr_bb(custom_conf)
    deploy_bb_(default_conf, custom_conf, master, spark_Phive_component)

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


def replace_conf_run(custom_conf, use_pat):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    spark_Phive_version = beaver_env.get("SPARK_PHIVE_VERSION")
    # Redeploy Hive
    undeploy_hive(master)
    deploy_hive(default_conf, custom_conf, master, beaver_env)
    populate_hive_on_mr_conf(custom_conf)
    restart_hive_on_mr(custom_conf)
    undeploy_bb_(master, spark_Phive_version, spark_Phive_component)
    deploy_bigbench(custom_conf)
    if use_pat:
        run_BB_PAT(master, slaves, beaver_env, custom_conf)
    else:
        run_BB(master, beaver_env)


def deploy_run(custom_conf, use_pat):
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_hive_on_mr(custom_conf, beaver_env)
    deploy_hive_on_mr(custom_conf)
    start_hive_on_mr(custom_conf)
    deploy_bigbench(custom_conf)
    if use_pat:
        run_BB_PAT(master, slaves, beaver_env, custom_conf)
    else:
        run_BB(master, beaver_env)

def undeploy_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    spark_Phive_version = beaver_env.get("SPARK_PHIVE_VERSION")
    undeploy_hive_on_mr(custom_conf)
    undeploy_bb_(master, spark_Phive_version, spark_Phive_component)

def usage():
    print("Usage: sbin/runBBonHoS.sh [action] [path/to/conf] [-pat]/n")
    print("   Action option includes: deploy_run, replace_conf_run, undeploy /n")
    print("           replace_conf_run means just replacing configurations and trigger a run /n")
    print("           deploy_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        usage()
    action = args[1]
    conf_p = args[2]
    use_pat = False
    if len(args) > 3:
        if args[3] == "-pat":
            use_pat = True
    if action == "replace_conf_run":
        replace_conf_run(conf_p, use_pat)
    elif action == "deploy_run":
        deploy_run(conf_p, use_pat)
    elif action == "undeploy":
        undeploy_run(conf_p)
    elif action == "run_bb":
        run_BB_direct(conf_p, use_pat)
    else:
        usage()
    exit(0)

