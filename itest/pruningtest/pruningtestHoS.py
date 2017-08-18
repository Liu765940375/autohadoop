#!/usr/bin/python

from cluster.HiveOnSpark import *
from infra.bigbench import *


def deploy(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_hive_on_spark(custom_conf, beaver_env)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)


def usage():
    print("Usage: pruningtest/deployHoS.py [path/to/conf]/n")
    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 1:
        usage()
    conf_p = args[1]
    deploy(conf_p)
