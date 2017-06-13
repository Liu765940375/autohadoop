#!/usr/bin/python

from utils.util import *
from utils.ssh import *

BB_COMPONENT = "BB"
PAT_COMPONENT = "pat"


def deploy_bb(default_conf, custom_conf, master, slaves):
    clean_bb(master)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    copy_packages([master], BB_COMPONENT, beaver_env.get("BB_VERSION"))
    update_copy_bb_conf(master, default_conf, custom_conf, beaver_env)
    deploy_pat(custom_conf, master)


def deploy_pat(custom_conf, master):
    clean_pat(master)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    copy_packages([master], PAT_COMPONENT, beaver_env.get("PAT_VERSION"))


def populate_bb_conf(master, default_conf, custom_conf, beaver_env):
    update_copy_bb_conf(master, default_conf, custom_conf, beaver_env)


def update_copy_bb_conf(master, default_conf, custom_conf, beaver_env):
    bb_custom_conf = os.path.join(custom_conf, BB_COMPONENT)
    bb_default_conf = os.path.join(default_conf, BB_COMPONENT)
    output_conf = os.path.join(custom_conf, "output/")
    bb_output_conf = os.path.join(output_conf, BB_COMPONENT)
    os.system("rm -rf " + bb_output_conf)
    os.system("cp -r " + bb_default_conf + " " + output_conf)
    os.system("cp -r " + bb_custom_conf + " " + output_conf)
    bb_tar_file_name = "bb.conf.tar"
    bb_tar_file = os.path.join(output_conf, bb_tar_file_name)
    os.system("cd " + bb_output_conf + ";" + "tar cf " + bb_tar_file + " *")
    bb_home = beaver_env.get("BB_HOME")
    remote_tar_file = os.path.join(bb_home, bb_tar_file_name)
    ssh_copy(master, bb_tar_file, remote_tar_file)
    ssh_execute(master, "tar xf " + remote_tar_file + " -C " + bb_home)


def clean_bb(master):
    ssh_execute(master, "rm -rf /opt/Beaver/BB*")


def clean_pat(master):
    ssh_execute(master, "rm -rf /opt/Beaver/pat*")


def undeploy_bb(master):
    clean_bb(master)


def run_BB(master, beaver_env):
    print (colors.LIGHT_BLUE + "Run BigBench" + colors.ENDC)
    # in order to pass q05, we can not export SPARK_HOME so here to unset this variable
    ssh_execute(master, "unset SPARK_HOME;" + beaver_env.get("BB_HOME") + "/bin/bigBench runBenchmark")
    copy_res(master, get_bb_log_dir(beaver_env), get_res_dir(beaver_env))

def get_res_dir(beaver_env):
    res_dir = os.path.join(beaver_env.get("RES_DIR"), str(time.strftime("%Y-%m-%d-%H-%M-%S",
                                                                        time.localtime())))
    return res_dir


def get_bb_log_dir(beaver_env):
    log_dir = os.path.join(beaver_env.get("BB_HOME"), "logs")
    return log_dir


def copy_res(master, log_dir, res_dir):
    print("Copying result from " + log_dir + "to dir " + res_dir)
    ssh_execute(master, "mkdir -p " + res_dir + " && cp -r " + log_dir + " " + res_dir)


def run_BB_PAT(master, slaves, beaver_env):
    print (colors.LIGHT_BLUE + "run pat " + colors.ENDC)
    pat_home = beaver_env.get("PAT_HOME")
    bb_home = beaver_env.get("BB_HOME")
    pat_config_xml_conf = os.path.join(pat_home, "PAT-post-processing/config.xml")
    print pat_config_xml_conf
    tree = ET.parse(pat_config_xml_conf)
    root = tree.getroot()
    all_nodes_content = "ALL_NODES: "
    for node in slaves:
        all_nodes_content += node.hostname + ":22 "
    cmd = "cp -r " + pat_home + "/PAT-collecting-data/config.template " + pat_home + "/PAT-collecting-data/config;"
    cmd += "sed -i 's/ALL_NODES/#ALL_NODES/g' " + pat_home + "/PAT-collecting-data/config;"
    cmd += "echo " + all_nodes_content + " >> " + pat_home + "/PAT-collecting-data/config;"
    ssh_execute(master, cmd)
    bigBench_properties = get_env_list(bb_home + "/conf/bigBench.properties")
    bigBench_command = read_bigBench_workloads(bigBench_properties, bb_home)
    for i in range(len(bigBench_command)/2):
        cmd += "sed -i 's/CMD_PATH/#CMD_PATH/g' " + pat_home + "/PAT-collecting-data/config;"
        if bigBench_command[i*2+1] != " ":
            cmd += "echo CMD_PATH: " + bigBench_command[i*2+1]+ " >> " + pat_home + "/PAT-collecting-data/config;"
            cmd += "unset SPARK_HOME;cd " + pat_home + "/PAT-collecting-data;./pat run " + bigBench_command[i*2]
            ssh_execute(master, cmd)
            root.find("source").text = pat_home + "/PAT-collecting-data/results/" + bigBench_command[i*2] + "/instruments"
            tree.write(pat_config_xml_conf)
            cmd = "cd " + pat_home + "/PAT-post-processing;./pat-post-process.py;"
            ssh_execute(master, cmd)
    res_dir = get_res_dir(beaver_env)
    pat_log_dir = os.path.join(pat_home, "/PAT-collecting-data/results/")
    copy_res(master, get_bb_log_dir(beaver_env), res_dir)
    copy_res(master, pat_log_dir, res_dir)
