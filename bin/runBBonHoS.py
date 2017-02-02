#!/usr/bin/python

import sys
from cluster.HiveOnSpark import *
from infra.bigbench import *


def deploy_bigbench(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_bb(default_conf, beaver_env, master)


def replace_conf_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    populate_hive_on_spark_conf(custom_conf)
    restart_hive_on_spark(custom_conf)
    populate_bb_conf(master, default_conf, custom_conf, beaver_env)
    run_BB(master, beaver_env)


def deploy_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_hive_on_spark(custom_conf)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
    deploy_bigbench(custom_conf)
    run_BB(master, beaver_env)


def usage():
    print "Usage: sbin/runBBonHoS.sh [action] [path/to/conf]/n"
    print "   Action option includes: deploy_run, replace_conf_run /n"
    print "           replace_conf_run means just replacing configurations and trigger a run /n"
    print "           deploy_run means remove all and redeploy a new run /n"
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if args.__sizeof__() != 2:
        usage
    action = args[1]
    conf_p = args[2]
    if action == "replace_conf_run":
        replace_conf_run(conf_p)
    elif action == "deploy_run":
        deploy_run(conf_p)
    else:
        usage

