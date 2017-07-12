#!/usr/bin/python

from infra.presto import *
from infra.hive_tpc_ds import *
from cluster.HiveOnSpark import *

default_conf = os.path.join(project_path, "conf")
def deploy_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy Presto" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_presto(slaves)
    deploy_presto(default_conf, custom_conf, master, slaves, beaver_env)
    stop_presto_service(slaves)
    undeploy_hive_on_spark(custom_conf)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
    deploy_hive_tpc_ds(default_conf, custom_conf, master)
    generate_tpc_ds_data_onhive(master, custom_conf, beaver_env)
    start_presto_service(slaves)
    run_presto_tpc_ds(master, beaver_env)


def usage():
    print("Usage: bin/runTPCDSonPresto.py [action] [path/to/conf]/n")
    print("   Action option includes: deploy_run, undeploy /n")
    print("           deploy_run means remove all and redeploy a new run /n")
    print("           undeploy means remove all components based on the specified configuration /n")
    exit(1)

def undeploy_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_presto(slaves)
    undeploy_hive_on_spark(custom_conf)
    undeploy_hive_tpc_ds(master)


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