#!/usr/bin/python

import os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from hosBB import deploy_hosBB, undeploy_hosBB, run_hosBB

if __name__ == '__main__':
    args = sys.argv
    action = args[1]
    benchmark = ""
    if len(args) == 3:
        benchmark = args[2]
    version = ""
    if len(args) == 4:
        version = args[3]

    if action == "deploy":
        if benchmark == "hosbb" or benchmark == "":
            deploy_hosBB(version)
        # Todo: deploy_tpcds

    if action == "undeploy":
        if benchmark == "hosbb" or benchmark == "":
            undeploy_hosBB(version)
        # Todo: undeploy_tpcds

    if action == "run":
        if benchmark == "hosbb" or benchmark == "":
            run_hosBB(version)
        # Todo: run_tpcds