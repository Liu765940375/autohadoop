from utils.util import *
from utils.ssh import *


def install_mysql(node, user, password):
    package = "mysql-community-release-el7-9.noarch.rpm"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    ssh_copy(node, download_package, "/opt/" + package)

    repo_package = "mysql-community.repo"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, repo_package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + repo_package)
    ssh_copy(node, download_package, "/etc/yum.repos.d/" + repo_package)

    cmd = "rpm -qa | grep mysql-community-server;"
    installed = ""
    for line in os.popen(cmd).readlines():
        installed += line.strip('\r\n')
    # For mysql5.7, command "mysqladmin -u username password pass" is not effective.
    install_cmd = "cd /opt;rpm -ivh " + package + ";yum -y install mysql-community-server;" \
                  + "systemctl start mysqld;mysqladmin -u " + user + " password " + password
    if installed.find("mysql-community-server") != -1:
        cmd = "systemctl stop mysqld;yum -y remove mysql-*;rm -rf /var/lib/mysql;"
        cmd += install_cmd
    else:
        cmd = install_cmd
    ssh_execute(node, cmd)
    cmd_grant_privilege = "mysql -u root -p123456 -Bse \"GRANT ALL PRIVILEGES ON *.* TO '" + user \
                          + "'@'" + node.hostname + "' IDENTIFIED BY '" + password + "' with grant option;FLUSH PRIVILEGES;\""
    ssh_execute(node, cmd_grant_privilege)


def deploy_mysql(master, default_conf):
    default_hive_conf_file = os.path.join(default_conf, "hive/hive-site.xml")
    username = get_config_value(default_hive_conf_file, "javax.jdo.option.ConnectionUserName")
    password = get_config_value(default_hive_conf_file, "javax.jdo.option.ConnectionPassword")
    install_mysql(master, username, password)