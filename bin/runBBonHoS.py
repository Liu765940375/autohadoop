#!/usr/bin/python

import sys
import re
from cluster.HiveOnSpark import *
from infra.bigbench import *

#following package is http://10.239.47.156/spark-Phive/spark-Phive-2.0.0.tar.gz,
#this package is for to make q05(with ML case) pass
spark_Phive_component="spark-Phive"
spark_Phive_version= "2.0.0"
spark_Phive_pkg=  spark_Phive_component+"-"+spark_Phive_version
def deploy_bigbench(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    deploy_sparkPhive(custom_conf,master)
    deploy_bb(default_conf, custom_conf, master)


def deploy_sparkPhive(custom_conf,master):
    download_spark_Phive_pkg(master)
    update_spark_ml(custom_conf)
    copy_hive_site(master)

def copy_hive_site(master):
    ssh_execute(master, "cp /opt/Beaver/hive/conf/hive-site.xml /opt/Beaver/"+spark_Phive_pkg+"/conf")


def download_spark_Phive_pkg(master):
    copy_packages([master],spark_Phive_component,spark_Phive_version)

def update_spark_ml(custom_conf):
    bb_custom_hive_engineSettings_conf = os.path.join(custom_conf, BB_COMPONENT+"/engines/hive/conf/engineSettings.conf")
    update_spark_ml_setting(bb_custom_hive_engineSettings_conf)

def update_spark_ml_setting(conf_file):
    tmp_filename = conf_file + '.tmp'
    os.system("mv " + conf_file + " " + tmp_filename)
    with open(tmp_filename, 'r') as file_read:
        total_line = file_read.read()
    origin_pattern=r'export BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY="spark-submit"'
    replace_pattern= r'export BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY="/opt/Beaver/'+spark_Phive_pkg+'/bin/spark-submit"'
    new_total_line=re.sub(origin_pattern,replace_pattern,total_line)
    with open(conf_file, 'w') as file_write:
        file_write.write(new_total_line)
    os.system("rm -rf " + tmp_filename)

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
    print (colors.LIGHT_BLUE + "Deploy BigBench" + colors.ENDC)
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    undeploy_hive_on_spark(custom_conf)
    deploy_hive_on_spark(custom_conf)
    start_hive_on_spark(custom_conf)
    deploy_bigbench(custom_conf)
    run_BB(master, beaver_env)

def undeploy_run(custom_conf):
    cluster_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_file)
    master = get_master_node(slaves)
    undeploy_hive_on_spark(custom_conf)
    undeploy_sparkPhive(master)
    undeploy_bb(master)

def undeploy_sparkPhive(master):
    ssh_execute(master, "rm -rf /opt/Beaver/"+spark_Phive_pkg)

def usage():
    print("Usage: sbin/runBBonHoS.sh [action] [path/to/conf]/n")
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

