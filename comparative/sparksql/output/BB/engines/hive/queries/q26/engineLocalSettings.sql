--
--Copyright (C) 2016 Transaction Processing Performance Council (TPC) and/or
--its contributors.
--
--This file is part of a software package distributed by the TPC.
--
--The contents of this file have been developed by the TPC, and/or have been
--licensed to the TPC under one or more contributor license agreements.
--
-- This file is subject to the terms and conditions outlined in the End-User
-- License Agreement (EULA) which can be found in this distribution (EULA.txt)
-- and is available at the following URL:
-- http://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA.txt
--
--Unless required by applicable law or agreed to in writing, this software
--is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
--ANY KIND, either express or implied, and the user bears the entire risk as
--to quality and performance as well as the entire cost of service or repair
--in case of defect.  See the EULA for more details.
--

--"INTEL CONFIDENTIAL"
--Copyright 2016 Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

--If your query will not run with  hive.optimize.sampling.orderby=true because of a hive bug => disable orderby sampling locally in this file. 
--Default behaviour is to use whatever is default in ../engines/hive/conf/engineSettings.sql
--No need to disable orderby sampling globally for all queries and no need to modifiy the query files q??.sql.
set bigbench.hive.optimize.sampling.orderby=${hiveconf:bigbench.hive.optimize.sampling.orderby};
set bigbench.hive.optimize.sampling.orderby.number=${hiveconf:bigbench.hive.optimize.sampling.orderby.number};
set bigbench.hive.optimize.sampling.orderby.percent=${hiveconf:bigbench.hive.optimize.sampling.orderby.percent};
set spark.sql.autoBroadcastJoinThreshold=209715200;
set spark.sql.shuffle.partitions=40;
