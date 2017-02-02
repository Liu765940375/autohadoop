#!/usr/bin/python

import os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from utils.util import *

config_file_names = get_config_files("hive")
hive_conf_path = os.path.join(config_path, "hive")

def deploy_hive(version):
    setup_env_dist([master], hive_env, "hive")
    set_path("hive", [master])

    copy_packages([master], "hive", version)

    stop_metastore_service()
    cmd = "rpm -qa | grep mysql-community-server;"
    installed = ""
    for line in os.popen(cmd).readlines():
        installed += line.strip()
    if installed.find("mysql-community-server-5.6.35-2.el7.x86_64") != -1:
        print colors.LIGHT_BLUE + "\tMysql5.6 has been installed, we do not have to install it." + colors.ENDC
    else:
        install_mysql(master)

    copy_configurations([master], config_file_names, hive_conf_path, "hive", "deploy")
    ssh_execute(master, hive_home + "/bin/schematool --initSchema -dbType mysql")
    print "Metastore is starting, it may take a while..."
    start_metastore_service()

def undeploy_hive(version):
    stop_metastore_service()
    os.system("rm -rf /opt/Beaver/hive-" + version + ";" + "rm -rf /opt/Beaver/hive;rm -rf /opt/Beaver/hiverc")

# Stop Hive metastore service
def stop_metastore_service():
    print colors.LIGHT_BLUE + "Stop Hive metastore service..." + colors.ENDC
    stdout = ssh_execute_withReturn(master, "ps -ef | grep metastore")
    for line in stdout:
        if "org.apache.hadoop.hive.metastore.HiveMetaStore" in line:
            process_id = line.split()[1]
            os.system("kill -9 " + process_id)

# Stop Hive metastore service
def start_metastore_service():
    print colors.LIGHT_BLUE + "Start Hive metastore service" + colors.ENDC
    ssh_execute_forMetastore(master, "nohup " + hive_home + "/bin/hive --service metastore &")

def restart_metastore_service():
    stop_metastore_service()
    copy_configurations([master], config_file_names, hive_conf_path, "hive", "restart")
    start_metastore_service()

# Install mysql, start mysql service; configure username and password
def install_mysql(node):
    print colors.LIGHT_GREEN + "Install mysql:" + colors.ENDC
    package = "mysql-community-release-el7-9.noarch.rpm"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, package)
    if not os.path.isfile(download_package):
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
        print colors.LIGHT_BLUE+ "\tDownload mysql rpm file from repo:" + colors.ENDC
    ssh_copy(node, download_package, "/opt/Beaver/" + package)

    repo_package = "mysql-community.repo"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, repo_package)
    if not os.path.isfile(download_package):
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + repo_package)
    ssh_copy(node, download_package, "/etc/yum.repos.d/" + repo_package)

    mysql_configs = {}
    get_configs_from_kvfile(os.path.join(config_path, "hive/hive-site.xml.custom"), mysql_configs)
    username ="root"
    password = "123456"
    if mysql_configs.has_key("javax.jdo.option.ConnectionUserName"):
        username = mysql_configs.get("javax.jdo.option.ConnectionUserName")
    if mysql_configs.has_key("javax.jdo.option.ConnectionPassword"):
        password = mysql_configs.get("javax.jdo.option.ConnectionPassword")

    # For mysql5.7, command "mysqladmin -u username password pass" is not effective.
    cmd = "systemctl stop mysqld;yum -y remove mysql-*;rm -rf /var/lib/mysql;"
    install_cmd = "cd /opt/Beaver/;rpm -ivh " + package + ";yum -y reinstall mysql-community-server;"\
                  + "systemctl start mysqld;mysqladmin -u " + username + " password " + password

    cmd += install_cmd
    os.system(cmd)
    print colors.LIGHT_BLUE + "\tComplete mysql installation, and mysql service has been started" + colors.ENDC

if __name__ == '__main__':
    args = sys.argv
    action = args[1]

    if len(args) == 3:
        version = args[2]
    else:
        version = hive_env.get("HIVE_VERSION")

    if action == "deploy":
        deploy_hive(version)
    if action == "undeploy":
        undeploy_hive(version)
    if action == "start":
        stop_metastore_service()
        start_metastore_service()
    if action == "stop":
        stop_metastore_service()
    if action == "restart":
        restart_metastore_service()

    if action != "deploy" and action != "undeploy" and action != "start" and action != "stop" and action != "restart":
        print colors.LIGHT_BLUE + "We currently only support following services: install, uninstall, start, stop and restart." \
                                  " Please enter correct parameters!" + colors.ENDC