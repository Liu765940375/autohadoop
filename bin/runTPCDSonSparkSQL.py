#!/usr/bin/python

import sys
from cluster.SparkSQL import *
from infra.spark_sql_perf import *
from infra.tpcds_kit import *


def deploy_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy TPC-DS" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_spark_sql(custom_conf)
    deploy_spark_sql(custom_conf)
    start_spark_sql(custom_conf)
    deploy_tpcds_kit(custom_conf, master, slaves)
    deploy_spark_sql_perf(custom_conf, master)


def replace_conf_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    populate_spark_sql_conf(custom_conf)
    restart_spark_sql(custom_conf)
    deploy_tpcds_kit(custom_conf, master, slaves)
    deploy_spark_sql_perf(custom_conf, master)


def undeploy_run(custom_conf):
    undeploy_spark_sql_perf(custom_conf)


def usage():
    print("Usage: sbin/runTPCDSonSparkSQL.sh [action] [path/to/conf]/n")
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