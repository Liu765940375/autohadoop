#!/usr/bin/python

from utils.util import *
from utils.ssh import *

BB_COMPONENT = "BB"


def deploy_bb(default_conf, custom_conf, master):
    clean_bb(master)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    copy_packages([master], BB_COMPONENT, beaver_env.get("BB_VERSION"))
    update_copy_bb_conf(master, default_conf, custom_conf, beaver_env)


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
    os.system("cd " + bb_output_conf + ";"+ "tar cf " + bb_tar_file + " *")
    bb_home = beaver_env.get("BB_HOME")
    remote_tar_file = os.path.join(bb_home, bb_tar_file_name)
    ssh_copy(master, bb_tar_file, remote_tar_file)
    ssh_execute(master, "tar xf " + remote_tar_file + " -C " + bb_home)


def clean_bb(master):
    ssh_execute(master, "rm -rf /opt/Beaver/BB*")


def undeploy_bb(master):
    clean_bb(master)


def run_BB(master, beaver_env):
    print (colors.LIGHT_BLUE + "Run BigBench" + colors.ENDC)
    ssh_execute(master, beaver_env.get("BB_HOME") + "/bin/bigBench runBenchmark")
    copy_res(master, beaver_env)


def copy_res(master, beaver_env):
    res_dir = os.path.join(beaver_env.get("RES_DIR"), str(time.strftime("%Y-%m-%d-%H-%M-%S",
                                                                        time.localtime())))
    print("Copying result to dir " + res_dir)
    log_dir = os.path.join(beaver_env.get("BB_HOME"), "logs")
    ssh_execute(master, "mkdir -p " + res_dir + " && cp -r " + log_dir + " " + res_dir)
