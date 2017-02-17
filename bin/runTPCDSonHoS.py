#!/usr/bin/python

import os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from cluster.HiveOnSpark import *
from infra.hive_tpc_ds import *


def deploy_tpc_ds(custom_conf):
    deploy_hive_tpc_ds(custom_conf)


def replace_conf_run(custom_conf):
    populate_hive_on_spark_conf(custom_conf)
    restart_hive_on_spark(custom_conf)
    populate_hive_tpc_ds_conf(custom_conf)
    run_hive_tpc_ds(custom_conf)


def deploy_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy TPC-DS" + colors.ENDC)
    get_env_list(os.path.join(custom_conf, "env"))
    undeploy_hive_on_spark(custom_conf)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
    deploy_hive_tpc_ds(custom_conf)
    run_hive_tpc_ds(custom_conf)


def undeploy_run(custom_conf):
    undeploy_hive_on_spark(custom_conf)
    undeploy_hive_tpc_ds(custom_conf)


def usage():
    print("Usage: sbin/runTPCDSonHoS.sh [action] [path/to/conf]/n")
    print("   Action option includes: deploy_run, replace_conf_run, undeploy /n")
    print("           replace_conf_run means just replacing configurations and trigger a run /n")
    print("           deploy_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

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

