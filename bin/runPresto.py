#!/usr/bin/python

import sys
from utils.node import *
from infra.presto import *

default_conf = os.path.join(project_path, "conf")
def deploy_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_presto(slaves)
    deploy_presto(default_conf, custom_conf, master, slaves, beaver_env)
    start_presto_service(slaves)

def undeploy_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    undeploy_presto(slaves)

def usage():
    print("Usage: sbin/runPresto.py [action] [path/to/conf] [-pat]/n")
    print("   Action option includes: deploy_run, undeploy /n")
    print("           deploy_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        usage()
    action = args[1]
    conf_p = args[2]
    if action == "deploy_run":
        deploy_run(conf_p)
    elif action == "undeploy":
        undeploy_run(conf_p)
    else:
        usage()