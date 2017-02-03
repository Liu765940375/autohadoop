#!/usr/bin/python

from infra.other_components import *

HIVE_COMPONENT = "hive"

def deploy_hive_internal(default_conf, custom_conf, master, beaver_env):
    setup_env_dist([master], beaver_env, HIVE_COMPONENT)
    set_path(HIVE_COMPONENT, [master], beaver_env.get("HIVE_HOME"))
    clean_hive(master)
    copy_packages([master], HIVE_COMPONENT, beaver_env.get("HIVE_VERSION"))
    update_copy_hive_conf(default_conf, custom_conf, master, beaver_env)

def update_hive_conf(default_conf, custom_conf, master):
    output_hive_conf = update_conf(HIVE_COMPONENT, default_conf, custom_conf)
    # for all conf files, replace the related value, eg, replace master_hostname with real hostname
    for conf_file in [file for file in os.listdir(output_hive_conf) if fnmatch.fnmatch(file, '*.xml')]:
        output_conf_file = os.path.join(output_hive_conf, conf_file)
        replace_conf_value(output_conf_file, "master_hostname", master.hostname)
        format_xml_file(output_conf_file)
    return output_hive_conf

def hive_init_schema(master, hive_home):
    ssh_execute(master, hive_home + "/bin/schematool --initSchema -dbType mysql")

def hive_start_metastore(master, hive_home):
    hive_stop_metastore(master)
    print (colors.LIGHT_BLUE + "Starting Hive metastore service" + colors.ENDC)
    ssh_execute_forMetastore(master, "nohup " + hive_home + "/bin/hive --service metastore &")

def hive_stop_metastore(master):
    print (colors.LIGHT_BLUE + "Stopping Hive metastore service" + colors.ENDC)
    ssh_execute(master, "ps aux | grep 'hive-metastore' | grep 'org.apache.hadoop.hive.metastore.HiveMetaStore' | awk '{print $2}' | xargs -r kill -9")

def clean_hive(master):
    ssh_execute(master, "rm -rf /opt/Beaver/hive*")
    ssh_execute(master, "source ~/.bashrc")

def deploy_hive(default_conf, custom_conf, master, beaver_env):
    hive_home = beaver_env.get("HIVE_HOME")
    deploy_mysql(master, default_conf)
    stop_hive_service(master)
    deploy_hive_internal(default_conf, custom_conf, master, beaver_env)
    hive_init_schema(master, hive_home)

def deploy_start_hive_internal(default_conf, custom_conf, master, beaver_env):
    hive_home = beaver_env.get("HIVE_HOME")
    deploy_mysql(master, default_conf)
    stop_hive_service(master)
    deploy_hive_internal(default_conf, custom_conf, master, beaver_env)
    hive_init_schema(master, hive_home)
    start_hive_service(master, beaver_env)

def undeploy_hive(master):
    hive_stop_metastore(master)
    clean_hive(master)

def start_hive_service(master, beaver_env):
    hive_start_metastore(master, beaver_env.get("HIVE_HOME"))

def stop_hive_service(master):
    hive_stop_metastore(master)

def update_copy_hive_conf(default_conf, custom_conf, master, beaver_env):
    output_hive_conf = update_hive_conf(default_conf, custom_conf, master)
    copy_configurations([master], output_hive_conf, HIVE_COMPONENT, beaver_env.get("HIVE_HOME"))

'''
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--conf',
                      dest='conf_dir',
                      default="")
    parser.add_option('--action',
                      dest='action')

    options, remainder = parser.parse_args()

    custom_conf = options.conf_dir
    action = options.action

    cluster_config_file = os.path.join(custom_conf, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)

    if action == "deploy":
        deploy_hive(custom_conf, master)
    elif action == "update_conf":
        update_copy_conf(os.path.join(default_conf_path, HIVE_COMPONENT), custom_conf, master)
    elif action == "undeploy":
        clean_hive(slaves)
    elif action == "start_meta":
        hive_stop_metastore(master)
        hive_start_metastore(master)
    elif action == "stop_meta":
        hive_stop_metastore(master)
    else:
        print ("Not support")
'''