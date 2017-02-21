#!/usr/bin/python

from infra.maven import *

TPC_DS_COMPONENT = "TPC-DS"


def deploy_hive_tpc_ds(default_conf, custom_conf, master):
    print("Deploy Hive TPC_DS")
    clean_hive_tpc_ds(master)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    deploy_maven(master, beaver_env)
    copy_packages([master], TPC_DS_COMPONENT, beaver_env.get("TPC_DS_VERSION"))
    update_copy_tpc_ds_conf(master, default_conf, custom_conf, beaver_env)


def undeploy_hive_tpc_ds(master):
    print("Undeploy Hive TPC_DS")
    clean_hive_tpc_ds(master)


def clean_hive_tpc_ds(master):
    ssh_execute(master, "rm -rf /opt/Beaver/TPC-DS*")


def populate_hive_tpc_ds_conf(master, default_conf, custom_conf, beaver_env):
    print("replace Hive TPC_DS configurations")
    update_copy_tpc_ds_conf(master, default_conf, custom_conf, beaver_env)


def update_copy_tpc_ds_conf(master, default_conf, custom_conf, beaver_env):
    tpc_ds_custom_conf = os.path.join(custom_conf, TPC_DS_COMPONENT)
    tpc_ds_default_conf = os.path.join(default_conf, TPC_DS_COMPONENT)
    output_conf = os.path.join(custom_conf, "output/")
    tpc_ds_output_conf = os.path.join(output_conf, TPC_DS_COMPONENT)
    os.system("rm -rf " + tpc_ds_output_conf)
    os.system("cp -r " + tpc_ds_default_conf + " " + output_conf)
    os.system("cp -r " + tpc_ds_custom_conf + " " + output_conf)
    tpc_ds_tar_file_name = "tpc_ds.conf.tar"
    tpc_ds_tar_file = os.path.join(output_conf, tpc_ds_tar_file_name)
    os.system("cd " + tpc_ds_output_conf + ";"+ "tar cf " + tpc_ds_tar_file + " *")
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    remote_tar_file = os.path.join(tpc_ds_home, tpc_ds_tar_file_name)
    ssh_copy(master, tpc_ds_tar_file, remote_tar_file)
    ssh_execute(master, "tar xf " + remote_tar_file + " -C " + tpc_ds_home)


def build_tpc_ds(master, tpc_ds_home):
    print("+++++++++++++++++++++++++++++")
    print("Install gcc. Downloads, compiles and packages the TPC-DS data generator.")
    print("+++++++++++++++++++++++++++++")
    cmd = "yum -y install gcc make flex bison byacc;"
    cmd += "cd " + tpc_ds_home + ";./tpcds-build.sh;"
    ssh_execute(master, cmd)


def generate_tpc_ds_data(master, tpc_ds_home, scale, data_format):
    print("+++++++++++++++++++++++++++++")
    print("Generate tpc-ds data and load data")
    print("+++++++++++++++++++++++++++++")
    cmd = "cd " + tpc_ds_home + ";FORMAT=" + data_format + " ./tpcds-setup.sh " + scale + ";"
    ssh_execute(master, cmd)


def run_hive_tpc_ds(master, custom_conf, beaver_env):
    print (colors.LIGHT_BLUE + "Run TPC-DS..." + colors.ENDC)
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    tpc_ds_config_file = os.path.join(custom_conf, "TPC-DS/config")
    config_dict = get_configs_from_properties(tpc_ds_config_file)
    scale = config_dict.get("scale")
    build_flg = config_dict.get("build")
    generate_flg = config_dict.get("generate")
    data_format = config_dict.get("format")
    if build_flg == "yes":
        build_tpc_ds(master, tpc_ds_home)
    if generate_flg == "yes":
        if data_format == "":
            print(colors.LIGHT_RED + "Please set the format in <custom_conf>/TPC-DS/config file" + colors.ENDC)
            return
        generate_tpc_ds_data(master, tpc_ds_home, scale, data_format)
    tpc_ds_result = os.path.join(beaver_env.get("TPC_DS_RES_DIR"), str(time.strftime("%Y-%m-%d-%H-%M-%S",
                                                                     time.localtime())))
    cmd = "cd " + tpc_ds_home + ";perl runSuite.pl tpcds " + scale + " >> " + tpc_ds_result + ";"
    ssh_execute(master, cmd)
    copy_res_hive_tpc_ds(master, beaver_env)


def copy_res_hive_tpc_ds(master, beaver_env):
    print("Collect Hive TPC_DS benchmark result")
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    res_dir = os.path.join(beaver_env.get("TPC_DS_RES_DIR"), str(time.strftime("%Y-%m-%d-%H-%M-%S",
                                                                        time.localtime())))
    log_dir = os.path.join(tpc_ds_home, "sample-queries-tpcds")
    ssh_execute(master, "mkdir -p " + res_dir + " && cp -r " + log_dir + "*log" + " " + res_dir)