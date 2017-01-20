#!/usr/bin/python

import os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from utils.util import bb_home, colors

bb_config_path = os.path.join(bb_home, "config")
bb_conf_path = os.path.join(bb_home, "conf")
hive_conf_path = os.path.join(bb_home, "engines/hive/conf")

def cat_current_version():
    cmd = "readlink -f " + bb_conf_path
    config_version_path = ""
    for line in os.popen(cmd).readlines():
        config_version_path = line.strip()
    print "The current config version is: " + colors.LIGHT_BLUE + os.path.basename(config_version_path) + colors.ENDC + ", and the path is: " + colors.LIGHT_BLUE + config_version_path + colors.ENDC
    return config_version_path

def list_all_configs():
    cmd = "ls " + bb_config_path
    print "All config versions are as bellows:\n" + colors.LIGHT_BLUE + os.popen(cmd).read() + colors.ENDC
    cat_current_version()

def rename_config_name(current_name, newname):
    bb_current_config_file = os.path.join(bb_config_path, current_name)
    bb_new_config_file = os.path.join(bb_config_path, newname)
    cmd = "mv " + bb_current_config_file + " " + bb_new_config_file + ";"
    cmd += "rm -rf " + bb_conf_path + ";" + "ln -s " + bb_new_config_file + " " + bb_conf_path + ";"
    cmd += "rm -rf " + hive_conf_path + ";" + "ln -s " + bb_new_config_file + " " + hive_conf_path + ";"
    os.system(cmd)

def switch_config(config_version):
    config_version_path = cat_current_version()
    if os.path.basename(config_version_path) != config_version:
        cmd = "rm -rf " + bb_conf_path + ";" + "ln -s " + os.path.join(bb_config_path, config_version) + " " + bb_conf_path + ";"
        cmd += "rm -rf " + hive_conf_path + ";" + "ln -s " + os.path.join(bb_config_path, config_version) + " " + hive_conf_path + ";"
        os.system(cmd)
	print "After swtich, the current version is: " + colors.LIGHT_BLUE + config_version + colors.ENDC

if __name__ == '__main__':
    args = sys.argv
    action = args[1]

    if action == "list":
        list_all_configs()
    if action == "rename":
        current_name = args[2]
        newname = args[3]

        list_all_configs()
        rename_config_name(current_name, newname)
    if action == "switch":
        config_version = args[2]
        switch_config(config_version)
