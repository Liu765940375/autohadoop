#!/bin/bash

set -e
source ./utils.sh

echo "run BB on HOS with conf1(replaceconf)">>log.txt
../bin/runBBonHoS.py deploy_run /home/custom/
service_check
echo "run BB on HOS with conf2(replaceconf)">>log.txt
../bin/runBBonHoS.py replace_conf_run /home/custom/
service_check
../bin/runBBonHoS.py undeploy /home/custom/
