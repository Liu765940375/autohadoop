#!/bin/bash

CONF_PATH="`dirname $0`/../conf/"

function setup_login_without_password(){
  echo "setup_login_without_password"

  echo $2 $1 >> /etc/hosts
  user=$3
  node=$1
  rsafile=~/.ssh/id_rsa
  if [ ! -f "$rsafile" ]
  then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  fi

  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

  chmod 600 ~/.ssh/authorized_keys

  ssh-copy-id -i ~/.ssh/id_rsa.pub $user@$node

}

function config_repo(){
  echo "config_repo"
}

function add_node(){
  echo "Add node with hostname $1, IP $2, username $3 and password $4"
  setup_login_without_password
}

function add_nodes(){
  while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ ${line:0:1} != \# ]];then
      IFS=' ' read -r -a node <<< "$line"
      echo "Add node with hostname ${node[0]}, IP ${node[1]}, username ${node[2]} and password ${node[3]}"
      add_node ${node[@]}
    fi
  done < "$1"


  # deploy configurations to all nodes
}

source $CONF_PATH/env.sh
add_nodes $CONF_PATH/$SLAVES_CONF_NAME
