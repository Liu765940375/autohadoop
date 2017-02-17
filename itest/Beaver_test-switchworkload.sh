#!/bin/bash

set -e

../bin/runBBonHoS.py deploy_run /home/custom/
../bin/runBBonHoS.py undeploy /home/custom/
../bin/runBBonSparkSQL.py deploy_run /home/custom1/
../bin/runBBonSparkSQL.py undeploy /home/custom1/
../bin/runTPCDSonHoS.py deploy_run /home/custom2
../bin/runTPCDSonHoS.py undeploy /home/custom2
