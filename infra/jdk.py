#!/usr/bin/python

from utils.util import *
from utils.ssh import *

JAVA_COMPONENT = "jdk"


def deploy_jdk(slaves, beaver_env):
    jdk_version = beaver_env.get("JDK_VERSION")
    clean_jdk(slaves)
    copy_jdk(jdk_version, slaves)
    setup_env_dist(slaves, beaver_env, JAVA_COMPONENT)
    set_path(JAVA_COMPONENT, slaves, jdk_version)


def clean_jdk(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/jdk*")


def copy_jdk(version, slaves):
    package = "jdk-" + version + "-linux-x64.tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print (colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist(slaves, os.path.join(package_path, package), JAVA_COMPONENT, version)