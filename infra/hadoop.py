#!/usr/bin/python
from utils.util import *
from utils.ssh import *
from infra.jdk import *

HADOOP_COMPONENT = "hadoop"

# Deploy Hadoop component
def deploy_hadoop_internal(default_conf, custom_conf, master, slaves):
    setup_nopass(slaves)
    update_etc_hosts(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    clean_hadoop(slaves, custom_conf)
    setup_env_dist(slaves, beaver_env, HADOOP_COMPONENT)
    set_path(HADOOP_COMPONENT, slaves, beaver_env.get("HADOOP_HOME"))
    copy_packages(slaves, HADOOP_COMPONENT, beaver_env.get("HADOOP_VERSION"))
    update_copy_hadoop_conf(default_conf, custom_conf, master, slaves, beaver_env)
    stop_firewall(slaves)

def clean_hadoop(slaves, custom_conf):
    name_dir = "/opt/Beaver/hadoop/data/nn"
    data_dir = "/opt/Beaver/hadoop/data/dn"
    custom_conf_file = ""

    custom_hadoop_conf = os.path.join(custom_conf, "hadoop")
    for conf_file in [file for file in os.listdir(custom_hadoop_conf) if fnmatch.fnmatch(file, 'hdfs-site.xml')]:
        custom_conf_file = os.path.join(custom_hadoop_conf, conf_file)
        break
    tree_custom = ET.parse(custom_conf_file)
    root_custom = tree_custom.getroot()
    if os.path.basename(custom_conf_file) == "hdfs-site.xml":
        for property_tag in root_custom.findall("./property"):
            property_name = property_tag.find("name").text
            if property_name == "dfs.namenode.name.dir":
                name_dir = property_tag.find("value").text
                name_dir = name_dir.replace(",", " ")
                continue
            if property_name == "dfs.datanode.data.dir":
                data_dir = property_tag.find("value").text
                data_dir = data_dir.replace(",", " ")
                continue

    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/hadoop*")
        ssh_execute(node, "rm -rf " + name_dir)
        ssh_execute(node, "rm -rf " + data_dir)
        ssh_execute(node, "source ~/.bashrc")


def auto_hardware_config():
    print ("Get configs from hardware detect")

def display_hadoop_perf(project_path, hadoop_home):
    shell_path = os.path.join(project_path, "shell")
    print (colors.LIGHT_BLUE + "Test cluster performance, this will take a while..." + colors.ENDC)
    print (colors.LIGHT_GREEN + "Test hdfs io..." + colors.ENDC)
    cmd = ". " + os.path.join(shell_path, "dfstest.sh") + " " + hadoop_home
    os.system(cmd)
    print (colors.LIGHT_GREEN + "Test disk io..." + colors.ENDC)
    data_dir = "/opt/Beaver/hadoop/data"
    cmd = ". " + os.path.join(shell_path, "diskiotest.sh") + " " + data_dir
    os.system(cmd)

# Generate Hadoop slaves config file
def generate_slaves(slaves, conf_dir):
    with open(os.path.join(conf_dir, "slaves"), "w") as f:
        for node in slaves:
            #if node.role == "slave":
            f.write(node.ip + "\n")

# merge configuration file
def update_hadoop_conf(default_conf, custom_conf, master, slaves):
    output_hadoop_conf = update_conf(HADOOP_COMPONENT, default_conf, custom_conf)
    # generate slaves file
    generate_slaves(slaves, output_hadoop_conf)
    # for all conf files, replace the related value, eg, replace master_hostname with real hostname
    for conf_file in [file for file in os.listdir(output_hadoop_conf) if fnmatch.fnmatch(file, '*.xml')]:
        output_conf_file = os.path.join(output_hadoop_conf, conf_file)
        replace_xml_conf_value(output_conf_file, "master_hostname", master.hostname)
        format_xml_file(output_conf_file)
    return output_hadoop_conf

def copy_hadoop_conf(default_hadoop_conf, beaver_custom_conf):
    os.system("cp -r " + default_hadoop_conf + "/hadoop " + beaver_custom_conf)

# Stop hadoop related services
def stop_hadoop_service(master, slaves):
    print (colors.LIGHT_BLUE + "Stop hadoop related services, it may take a while..." + colors.ENDC)
    process_list = ["NameNode", "DataNode", "SecondaryNameNode", "NodeManager", "ResourceManager", "WebAppProxyServer", "JobHistoryServer"]
    ssh_execute(master, "$HADOOP_HOME/sbin/stop-all.sh")
    for node in slaves:
        ssh_execute(node, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver")
        stdout = ssh_execute_withReturn(node, "jps")
        process_dict = {}
        for line in stdout:
            k, v = line.partition(" ")[::2]
            process_dict[v.strip()] = k.strip()
        for process in process_list:
            if process in process_dict:
                ssh_execute(node, "kill -9 " + process_dict.get(process))
        del process_dict

# Start Hadoop related services
def start_hadoop_service(master, slaves, beaver_env):
    hadoop_home = beaver_env.get("HADOOP_HOME")
    stop_hadoop_service(master, slaves)
    print (colors.LIGHT_BLUE + "Start hadoop related services,  it may take a while..." + colors.ENDC)
    ssh_execute(master, hadoop_home + "/sbin/start-all.sh")
    ssh_execute(master, hadoop_home + "/sbin/yarn-daemon.sh start proxyserver")
    ssh_execute(master, hadoop_home + "/sbin/mr-jobhistory-daemon.sh start historyserver")

def update_copy_hadoop_conf(default_conf, custom_conf, master, slaves, beaver_env):
    output_hadoop_conf = update_hadoop_conf(default_conf, custom_conf, master, slaves)
    copy_configurations(slaves, output_hadoop_conf, HADOOP_COMPONENT, beaver_env.get("HADOOP_HOME"))

def hdfs_format(master, hadoop_home):
    print (colors.LIGHT_BLUE + "format hdfs" + colors.ENDC)
    ssh_execute(master, "yes | " + hadoop_home + "/bin/hdfs namenode -format")

def deploy_hadoop(default_conf, custom_conf, master, slaves, beaver_env):
    deploy_jdk(slaves, beaver_env)
    stop_hadoop_service(master, slaves)
    deploy_hadoop_internal(default_conf, custom_conf, master, slaves)
    hdfs_format(master, beaver_env.get("HADOOP_HOME"))

def deploy_start_hadoop(default_conf, custom_conf, master, slaves, beaver_env):
    deploy_jdk(slaves, beaver_env)
    stop_hadoop_service(master, slaves)
    deploy_hadoop_internal(default_conf, custom_conf, master, slaves)
    hdfs_format(master, beaver_env.get("HADOOP_HOME"))
    start_hadoop_service(master, slaves, beaver_env)

def undeploy_hadoop(master, slaves, custom_conf):
    stop_hadoop_service(master, slaves)
    clean_hadoop(slaves, custom_conf)

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
        deploy_hadoop(custom_conf, slaves)
    elif action == "undeploy":
        clean_hadoop(slaves)
    elif action == "start":
        stop_hadoop_service(master, slaves)
        start_hadoop_service(master)
    elif action == "stop":
        stop_hadoop_service(master, slaves)
    elif action == "format":
        hdfs_format(master)
    else:
        print ("Not support")
'''