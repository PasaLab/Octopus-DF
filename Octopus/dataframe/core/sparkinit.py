#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
from pyspark.sql import SparkSession
from configparser import ConfigParser

__all__ = ["spark"]

logging.getLogger().setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s: %(filename)s:line %(lineno)s:%(message)s')

print("*************************************************")
print("************ Loading Spark Environment **********")
print("*************************************************")

YARN = False

if YARN:
    conf = ConfigParser()
    user_home = os.path.expanduser('~')
    conf_path = user_home + "/.config/octopus/config.ini"
    conf.read(conf_path)

    YARN_CONF_DIR = conf.get('installation', 'YARN_CONF_DIR')
    HADOOP_CONF_DIR = conf.get('installation', 'HADOOP_CONF_DIR')
    PYSPARK_PYTHON = conf.get('installation', 'PYSPARK_PYTHON')
    SPARK_YARN_QUEUE = conf.get('installation', 'SPARK_YARN_QUEUE')
    SPARK_YARN_DIST_ARCHIVES = conf.get('installation', 'SPARK_YARN_DIST_ARCHIVES')
    SPARK_EXECUTOR_CORES = conf.get('installation', 'SPARK_EXECUTOR_CORES')
    SPARK_EXECUTOR_MEMORY = conf.get('installation', 'SPARK_EXECUTOR_MEMORY')
    SPARK_EXECUTOR_INSTANCES = conf.get('installation', 'SPARK_EXECUTOR_INSTANCES')
    SPARK_MEMORY_STORAGE_FRACTION = conf.get('installation', 'SPARK_MEMORY_STORAGE_FRACTION')
    SPARK_YARN_JARS = conf.get('installation', 'SPARK_YARN_JARS')
    SPARK_DEBUG_MAX_TO_STRING_FIELDS = conf.get('installation', 'SPARK_DEBUG_MAX_TO_STRING_FIELDS')
    SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = conf.get('installation', 'SPARK_YARN_EXECUTOR_MEMORYOVERHEAD')
    # os.environ["ARROW_LIBHDFS_DIR"] = "/opt/cloudera/parcels/CDH/lib64/"
    # os.environ["HADOOP_HOME"]="/opt/cloudera/parcels/CDH/lib/hadoop"
    os.environ['JAVA_TOOL_OPTIONS'] = '-Xss1280K'
    os.environ["YARN_CONF_DIR"] = YARN_CONF_DIR
    os.environ["HADOOP_CONF_DIR"] = HADOOP_CONF_DIR
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    os.environ["SPARK_YARN_DIST_ARCHIVES"] = SPARK_YARN_DIST_ARCHIVES
    os.environ["SPARK_EXECUTOR_CORES"] = SPARK_EXECUTOR_CORES
    os.environ["SPARK_EXECUTOR_INSTANCES"] = SPARK_EXECUTOR_INSTANCES
    os.environ["SPARK_YARN_EXECUTOR_MEMORYOVERHEAD"] = SPARK_YARN_EXECUTOR_MEMORYOVERHEAD
    # os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"

    try:
        spark = SparkSession \
            .builder \
            .master("yarn") \
            .appName("Octopus SparkDataFrame") \
            .config(key="spark.yarn.queue", value=SPARK_YARN_QUEUE) \
            .config(key="spark.executor.cores", value=int(SPARK_EXECUTOR_CORES)) \
            .config(key="spark.executor.instances", value=int(SPARK_EXECUTOR_INSTANCES)) \
            .config(key="spark.executor.memory", value=SPARK_EXECUTOR_MEMORY) \
            .config(key="spark.debug.maxToStringFields", value=SPARK_DEBUG_MAX_TO_STRING_FIELDS) \
            .config(key="spark.memory.storageFraction", value=float(SPARK_MEMORY_STORAGE_FRACTION)) \
            .config(key="spark.yarn.am.memory", value=SPARK_EXECUTOR_MEMORY) \
            .config(key="spark.yarn.executor.memoryOverhead", value=SPARK_YARN_EXECUTOR_MEMORYOVERHEAD) \
            .config(key="spark.yarn.dist.archives", value=SPARK_YARN_DIST_ARCHIVES) \
            .getOrCreate()

        spark.conf.set("spark.sql.execution.pandas.respectSessionTimeZone", "true")
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
            #            .config(key="spark.yarn.jars", value=SPARK_YARN_JARS) \

    except Exception as exp:
        print("Spark init Exception")
        raise exp
else:
    try:
        del os.environ["YARN_CONF_DIR"]
        del os.environ["HADOOP_CONF_DIR"]
    except Exception as exp:
        pass
    try:
        os.environ["SPARK_HOME"] = "/Users/jun/workspace/sbin/spark-2.3.2-bin-hadoop2.7"
        spark = SparkSession \
            .builder \
            .master("local") \
            .appName("Octopus SparkDataFrame") \
            .config(key="spark.serializer", value="org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    except Exception as exp:
        logging.exception("Spark init Exception")
        raise exp

spark.sparkContext.setLogLevel(logLevel='WARN')