#!/bin/bash
sed -i 's/^HIVE_VERSION=.*/HIVE_VERSION=2.2.0/' /home/custom/env
../bin/runBBonHoS.py deploy_run /home/custom/
../bin/runBBonHoS.py undeploy /home/custom/
sed -i 's/^HIVE_VERSION=.*/HIVE_VERSION=2.2.0/' /home/custom/env
../bin/runBBonHoS.py deploy_run /home/custom/
../bin/runBBonHoS.py undeploy /home/custom/
