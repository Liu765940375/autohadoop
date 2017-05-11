#!/usr/bin/python

from cluster.HiveOnSpark import *
from infra.bigbench import *


def deploy(pruningdir,custom_conf):
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_hive_on_spark(custom_conf)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
    os.system("scp -r "+pruningdir+" root@"+master.ip+":/opt/Beaver")
    ssh_execute(master,"/opt/Beaver/pruningtest/pruningtest.sh /opt/Beaver/pruningtest")


def usage():
    print("Usage: milestone/deployHoS.py projectpath/itest/pruningtest [path/to/conf]/n")
    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 1:
        usage()
    pruningdir = args[1]
    conf_p = args[2]
    deploy(pruningdir,conf_p)
