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
    output_tpc_ds_conf = update_conf(TPC_DS_COMPONENT, default_conf, custom_conf)
    conf_path = os.path.join(beaver_env.get("TPC_DS_HOME"), str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())))
    ssh_execute(master, "mkdir -p " + conf_path)
    tpc_ds_output_conf = glob.glob(output_tpc_ds_conf + "/*")
    for file in tpc_ds_output_conf:
        ssh_copy(master, file, conf_path + "/" + os.path.basename(file))
    ssh_execute(master, "rm -f " + os.path.join(beaver_env.get("TPC_DS_HOME"),"sample-queries-tpcds/testbench.settings"))
    ssh_execute(master, "ln -s " + conf_path + "/testbench.settings " + os.path.join(beaver_env.get("TPC_DS_HOME"),"sample-queries-tpcds/testbench.settings"))
    ssh_execute(master, "rm -f " + os.path.join(beaver_env.get("TPC_DS_HOME"),"runSuite.pl"))
    ssh_execute(master, "ln -s " + conf_path + "/runSuite.pl " + os.path.join(beaver_env.get("TPC_DS_HOME"),"runSuite.pl"))
    ssh_execute(master, "\cp -f " + conf_path + "/settings.xml.bak ~/.m2/settings.xml" )
#    ssh_execute(master, "\cp -f " + conf_path + "/config " + beaver_env.get("TPC_DS_HOME"))


'''
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
'''


def build_tpc_ds(master, tpc_ds_home):
    print("+++++++++++++++++++++++++++++")
    print("Install gcc. Downloads, compiles and packages the TPC-DS data generator.")
    print("+++++++++++++++++++++++++++++")
    cmd = "yum -y install gcc make flex bison byacc unzip;"
    cmd += "yum -y install patch;"
    cmd += "cd " + tpc_ds_home + ";bash -x ./tpcds-build.sh;"
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
        if int(scale) < 2:
            print(colors.LIGHT_RED + "The scale in <custom_conf>/TPC-DS/config file must greater than 1" + colors.ENDC)
            return
        generate_tpc_ds_data(master, tpc_ds_home, scale, data_format)
    tpc_ds_result = os.path.join(beaver_env.get("TPC_DS_RES_DIR"), str(time.strftime("%Y-%m-%d-%H-%M-%S",
                                                                     time.localtime())))
    cmd = "mkdir -p " + tpc_ds_result + ";cd " + tpc_ds_home + ";perl runSuite.pl tpcds " + scale + " >> " + tpc_ds_result + "/result.log;"
    ssh_execute(master, cmd)
    copy_res_hive_tpc_ds(master, beaver_env, tpc_ds_result)


def copy_res_hive_tpc_ds(master, beaver_env, res_dir):
    print("Collect Hive TPC_DS benchmark result")
    tpc_ds_home = beaver_env.get("TPC_DS_HOME")
    log_dir = os.path.join(tpc_ds_home, "sample-queries-tpcds")
    ssh_execute(master, "cp -r " + log_dir + "/*.log" + " " + res_dir)
