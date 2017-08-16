#!/usr/bin/python

from cluster.HiveOnMR import *
from infra.hive_tpc_ds import *

def deploy_tpc_ds(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    deploy_hive_tpc_ds(default_conf, custom_conf, master)

def deploy_run(custom_conf):
    print (colors.LIGHT_BLUE + "Deploy TPC-DS" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    update_mr_tpcds(custom_conf)
    undeploy_hive_on_mr(custom_conf, beaver_env)
    deploy_hive_on_mr(custom_conf)
    start_hive_on_mr(custom_conf)
    deploy_hive_tpc_ds(default_conf, custom_conf, master)
    run_hive_tpc_ds(master, custom_conf, beaver_env)


def update_mr_tpcds(custom_conf):
    tpcds_custom_hive_enginesettings_sql = os.path.join(custom_conf, "TPC-DS/testbench.settings")
    update_mr_tpcds_settings_sql(tpcds_custom_hive_enginesettings_sql)


def update_mr_tpcds_settings_sql(conf_file):
    with open(conf_file, 'r') as file_read:
        total_line = file_read.read()
    origin_pattern = r'.*set hive.execution.engine=.*;'
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
    populate_hive_tpc_ds_conf(master, default_conf, custom_conf, beaver_env)
    run_hive_tpc_ds(master, custom_conf, beaver_env)

def undeploy_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_hive_on_mr(custom_conf)
    undeploy_hive_tpc_ds(master)

def usage():
    print("Usage: sbin/runTPCDSonHoMR.sh [action] [path/to/conf]/n")
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
    elif action == "run_tpcds":
        run_tpcds_direct(conf_p)
    else:
        usage()