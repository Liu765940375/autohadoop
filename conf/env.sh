#!/bin/bash

CONF_PATH="`dirname $0`"
echo $CONF_PATH
# the path to the tool configuration
export SLAVES_CONF_NAME="slaves.property"
export CORE_SITE_PATH="$CONF_PATH/core-site.xml"
export HDFS_SITE_PATH="$CONF_PATH/hdfs-site.xml"
export YARN_SITE_PATH="$CONF_PATH/yarn-site.xml"
export MAPRED_SITE_PATH="$CONF_PATH/mapred-site.xml"

export CORE_SITE_TEMPLATE_PATH="$CONF_PATH/core-site.xml.template"
export HDFS_SITE_TEMPLATE_PATH="$CONF_PATH/hdfs-site.xml.template"
export YARN_SITE_TEMPLATE_PATH="$CONF_PATH/yarn-site.xml.template"
export MAPRED_SITE_TEMPLATE_PATH="$CONF_PATH/mapred-site.xml.template"

export CORE_SITE_CONF_NAME="core-site.property"
export HDFS_SITE_CONF_NAME="hdfs-site.property"
export YARN_SITE_CONF_NAME="yarn-site.property"
export MAPRED_SITE_CONF_NAME="mapred-site.property"
