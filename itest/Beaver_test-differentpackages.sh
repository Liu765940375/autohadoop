#!/bin/bash

set -e
source ./utils.sh

echo "run BB on HOS with HIVE_VERSION=2.2.0(differentpatch1)">>log.txt
sed -i 's/^HIVE_VERSION=.*/HIVE_VERSION=2.2.0/' /home/custom/env
../bin/runBBonHoS.py deploy_run /home/custom/
service_check
../bin/runBBonHoS.py undeploy /home/custom/
echo "run BB on HOS with HIVE_VERSION=2.2.0(differentpatch2)">>log.txt
sed -i 's/^HIVE_VERSION=.*/HIVE_VERSION=2.2.0/' /home/custom/env
../bin/runBBonHoS.py deploy_run /home/custom/
service_check
../bin/runBBonHoS.py undeploy /home/custom/
