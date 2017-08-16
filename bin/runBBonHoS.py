#!/usr/bin/python

import sys
import re
from cluster.HiveOnSpark import *
from infra.bigbench import *

#following package is http://10.239.47.156/spark-Phive/spark-Phive-2.0.0.tar.gz,
#this package is for to make q05(with ML case) pass
spark_Phive_component="spark-Phive"
def deploy_bigbench(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    deploy_bb_(default_conf, custom_conf, master, spark_Phive_component)


def replace_conf_run(custom_conf, use_pat):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    populate_hive_on_spark_conf(custom_conf)
    spark_Phive_version = beaver_env.get("SPARK_PHIVE_VERSION")
    undeploy_spark(master)
    deploy_spark(default_conf, custom_conf, master, slaves, beaver_env)
    restart_hive_on_spark(custom_conf)
    undeploy_bb_(master, spark_Phive_version, spark_Phive_component)
    deploy_bigbench(custom_conf)
    populate_bb_conf(master, default_conf, custom_conf, beaver_env)
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
    undeploy_hive_on_spark(custom_conf, beaver_env)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
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

    undeploy_hive_on_spark(custom_conf, beaver_env)
    undeploy_bb_(master, spark_Phive_version, spark_Phive_component)

def usage():
    print("Usage: sbin/runBBonHoS.sh [action] [path/to/conf] [-pat]/n")
    print("   Action option includes: deploy_run, replace_conf_run, undeploy /n")
    print("           replace_conf_run means just replacing configurations and trigger a run /n")
    print("           deploy_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)


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

