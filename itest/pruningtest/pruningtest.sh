#!/bin/bash
hive -f $1/createtextfile.sql
hive -f $1/createparquet.sql
mkdir -p /home/test
echo "generate data,this may take a while"
for (( i=0; i<20; i++ ))
do
	java -cp $1 Datagen /home/test/data 50
	hive -f $1/loaddata.sql
done
resultdir='/opt/Beaver/result/pruningtest'
mkdir -p $resultdir
hive -f $1/insertinto.sql
echo "starting pruning test,result will be put in $resultdir"
hive -f $1/test1.sql 2> $resultdir/test.log 1>/dev/null
echo "pruning #1">$resultdir/result.log
grep "Time taken" $resultdir/test.log >>$resultdir/result.log

hive -f $1/test5.sql 2> $resultdir/test.log 1>/dev/null
echo "pruning #5">>$resultdir/result.log
grep "Time taken" $resultdir/test.log >>$resultdir/result.log

hive -f $1/test10.sql 2> $resultdir/test.log 1>/dev/null
echo "pruning #10">>$resultdir/result.log
grep "Time taken" $resultdir/test.log >>$resultdir/result.log

hive -f $1/test15.sql 2> $resultdir/test.log 1>/dev/null
echo "pruning #15">>$resultdir/result.log
grep "Time taken" $resultdir/test.log >>$resultdir/result.log

hive -f $1/test20.sql 2> $resultdir/test.log 1>/dev/null
echo "pruning #20">>$resultdir/result.log
grep "Time taken" $resultdir/test.log >>$resultdir/result.log

hive -f $1/test30.sql 2> $resultdir/test.log 1>/dev/null
echo "pruning #30">>$resultdir/result.log
grep "Time taken" $resultdir/test.log >>$resultdir/result.log
