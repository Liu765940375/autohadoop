#!/bin/bash

set -e
source ./test_utils.sh

echo "run BB on HoS(switchworkload)">>log.txt
../bin/runBBonHoS.py deploy_run /home/custom/
service_check
../bin/runBBonHoS.py undeploy /home/custom/
echo "run BB on SparkSQL(switchworkload)">>log.txt
../bin/runBBonSparkSQL.py deploy_run /home/custom1/
service_check
../bin/runBBonSparkSQL.py undeploy /home/custom1/
