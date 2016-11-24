import glob

from node import *
from ssh import *
from config import *


def setup_config_dist(slaves, config_files, component):
    print "Distribute config xml for " + component
    if component == "hadoop":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/hadoop/etc/hadoop/" + os.path.basename(file))

# Copy and unpack a package to slave nodes
def copy_package_dist(slaves, file, component):
    print "Distrubte package for " + component
    for node in slaves:
        print "\tCopy packge to " + node.hostname + ":" + node.ip
        ssh_execute(node, "mkdir -p /opt")
        ssh_copy(node, file, "/opt/" + os.path.basename(file))
        cmd = "mkdir -p /opt/" + component + ";tar zxf /opt/" + os.path.basename(file) +\
              " -C /opt/" + component + " --strip-components=1 > /dev/null &"
        ssh_execute(node, cmd)

# Execute command on slave nodes
def execute_command_dist(slaves, command, component):
    print "Execute commands over slaves"
    for node in slaves:
        ssh_execute(node, command)

def get_env_list(filename):
    envs = {}
    print "Get env list from " + filename
    if not os.path.isfile(filename):
        return envs
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            envs[key.strip()] = value.strip()

    return envs

def setup_env_dist(slaves, envs, component):
    print "Setup Environment over slaves"
    cmd = ""
    for node in slaves:
        cmd += "rm -f /opt/" + component + "rc;"
        for key, value in envs.iteritems():
            cmd += "echo \"export " + key + "=" + value + "\">> /opt/" + component + "rc;"
        cmd += "echo \". /opt/" + component + "rc" + "\" >> ~/.bashrc;"
        ssh_execute(node, cmd)

def deploy(component, version, project_path):
    config_path = project_path + "/conf/" + component
    package_path = project_path +"/packages"
    config_file_names = ["hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml"]

    # Setup Nopass for slave nodes
    slaves = get_slaves(os.path.join(config_path, "slaves.custom"))
    with open(os.path.join(config_path, "slaves"), "w") as f:
        for node in slaves:
            f.write(node.ip + "\n")
    setup_nopass(slaves)

    # Setup ENV on slave nodes
    envs = get_env_list(os.path.join(config_path, "env"))
    setup_env_dist(slaves, envs, component)

    # Copy component package to slave nodes
    download_server = "10.239.47.156"
    package = component + "-" + version + ".tar.gz";
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/" + component
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    copy_package_dist(slaves, os.path.join(package_path, package), component)

    # Copy JDK
    # TODO: It should only setup one time.
    jdk_version = envs["JDK_VERSION"]
    package = "jdk-" + jdk_version + "-linux-x64.tar.gz";
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    copy_package_dist(slaves, os.path.join(package_path, package), "jdk")

    # Add binary path to PATH
    for node in slaves:
        cmd="echo \"export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin\" >> /opt/" + component +"rc"
        ssh_execute(node, cmd)

    # Generate configration XML files
    master = get_master_node(slaves)
    yarn_custom_file = os.path.join(config_path, "yarn-site.xml.custom")
    if not os.path.isfile(yarn_custom_file):
        with open(yarn_custom_file, "w") as f:
            f.wirte("yarn.resourcemanager,hostname="+master.ip+"\n")

    core_custom_file = os.path.join(config_path, "core-site.xml.custom")
    if not os.path.isfile(core_custom_file):
        with open(core_custom_file, "w") as f:
            f.write("fs.defaultFS=hdfs://"+master.ip+":9000")

    for config_file in config_file_names:
        template_config = os.path.join(config_path, config_file) + ".template"
        custom_config = os.path.join(config_path, config_file) + ".custom"
        target_config = os.path.join(config_path, config_file)
        generate_configuration(template_config, custom_config, target_config)

    path = config_path + "/*"
    final_config_files = glob.glob(path)
    setup_config_dist(slaves, final_config_files, component)

    # Create namenode and datanode direcotry on slave nodes
    cmd = ""
    if component == "hadoop":
        datanode_dir = get_config(os.path.join(config_path, "hdfs-site.xml"), "dfs.namenode.name.dir").split(':')[1]
        namenode_dir = get_config(os.path.join(config_path, "hdfs-site.xml"), "dfs.datanode.data.dir").split(':')[1]
        cmd = "mkdir -p " + namenode_dir + ";"
        cmd += "mkdir -p " + datanode_dir + ";"
    execute_command_dist(slaves, cmd, component)

