#!/usr/bin/python

from infra.maven import *
import pexpect

SPARK_SQL_PERF_COMPONENT = "spark-sql-perf"
SBT_COMPONENT = "sbt"

def deploy_spark_sql_perf(custom_conf, master):
    print("Deploy spark_sql_perf")
    clean_spark_sql_perf(master)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    copy_packages([master], SPARK_SQL_PERF_COMPONENT, beaver_env.get("SPARK_SQL_PERF_VERSION"))
    deploy_sbt(master, beaver_env)
    run_spark_sql_perf(master, custom_conf, beaver_env)


def deploy_sbt(master, beaver_env):
    copy_packages([master], SBT_COMPONENT, beaver_env.get("SBT_VERSION"))
    setup_env_dist([master], beaver_env, SBT_COMPONENT)
    set_path(SBT_COMPONENT, [master], beaver_env.get("SBT_HOME"))


def undeploy_spark_sql_perf(master):
    print("Undeploy spark_sql_perf")
    clean_spark_sql_perf(master)


def run_spark_sql_perf(master, custom_conf, beaver_env):
    print (colors.LIGHT_BLUE + "RUN SPARK-SQL-PERF..." + colors.ENDC)
    spark_sql_perf_home = beaver_env.get("SPARK_SQL_PERF_HOME")
    spark_sql_home = beaver_env.get("SPARK_HOME")
    tpcds_kit_home = beaver_env.get("TPCDS_KIT_HOME")
    spark_env_home = os.path.join(custom_conf, "spark")
    ssh_copy(master, spark_env_home + "/spark-env.sh", spark_sql_home + "/conf/spark-env.sh")
    run_tpcds(master, custom_conf, spark_sql_home, spark_sql_perf_home, tpcds_kit_home)
    ssh_execute(master, "hadoop fs -get /spark/sql/performance/* /opt/Beaver/result/")


def run_tpcds(master, custom_conf, spark_sql_home, spark_sql_perf_home, tpcds_kit_home):
    tpc_ds_config_file = os.path.join(custom_conf, "TPC-DS/config")
    config_dict = get_configs_from_properties(tpc_ds_config_file)
    scale = config_dict.get("scale")
    data_format = config_dict.get("format")
    table_dir = config_dict.get("table_dir")
    user = master.username
    ip = master.ip
    pexpect_child = pexpect.spawn('ssh %s@%s' % (user, ip))
    pexpect_child.expect('#')
    pexpect_child.sendline('cd ' + spark_sql_home)
    pexpect_child.sendline('bin/spark-shell --jars ' + spark_sql_perf_home + '/target/scala-2.11/spark-sql-perf_2.11-0.4.12-SNAPSHOT.jar --conf "spark.driver.extraJavaOptions=-Dhttp.proxyHost=child-prc.intel.com -Dhttp.proxyPort=913 -Dhttps.proxyHost=child-prc.intel.com -Dhttps.proxyPort=913" --packages com.typesafe.scala-logging:scala-logging-slf4j_2.10:2.1.2 --num-executors 8')
    pexpect_child.sendline('val sqlContext = new org.apache.spark.sql.SQLContext(sc)')
    pexpect_child.sendline('import sqlContext.implicits._')
    pexpect_child.sendline('import com.databricks.spark.sql.perf.tpcds.Tables')
    pexpect_child.sendline('val tables = new Tables(sqlContext, "' + tpcds_kit_home + '/tools", ' + scale + ')')
    pexpect_child.sendline('tables.genData("hdfs://'+ ip +':9000/' + table_dir +'","' + data_format + '", true, false, false, false, false)')
    pexpect_child.sendline('tables.createExternalTables("hdfs://' + ip +':9000/' + table_dir + '","' + data_format +'","finaltest", false)')
    pexpect_child.sendline('import com.databricks.spark.sql.perf.tpcds.TPCDS')
    pexpect_child.sendline('val tpcds = new TPCDS (sqlContext = sqlContext)')
    pexpect_child.sendline('val experiment = tpcds.runExperiment(tpcds.tpcds1_4Queries)')
    print pexpect_child.before
    pexpect_child.interact()

    pass


def clean_spark_sql_perf(master):
    ssh_execute(master, "rm -rf /opt/Beaver/spark-sql-perf*")

