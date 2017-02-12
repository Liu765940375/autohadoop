#!/bin/bash

set -e

../bin/runBBonHoS.py deploy_run /home/custom/
../bin/runBBonHoS.py replace_conf_run /home/custom/
../bin/runBBonHoS.py undeploy /home/custom/
