#!/bin/bash

yum -y install mysql-community-server.x86_64
/sbin/service/mysqld start
mysqladmin -u root password $1