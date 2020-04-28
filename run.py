#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
from datetime import datetime
import os
import logging
import re


logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%y/%m/%d %H:%M:%S", level=logging.INFO)
logger = logger = logging.getLogger(__name__)


# https://pythex.org/
# string test: 127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s?" (\d{3}) (\S+)'
PWD = os.getenv("PWD")


def schema():
    return StructType([StructField("host", StringType(), True),
                       StructField("timestamp", TimestampType(), True),
                       StructField("request", StringType(), True),
                       StructField("http_code", StringType(), True),
                       StructField("bytes", IntegerType(), True)])


def parser(row):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, row.__getattr__("value"))
    # host      = match.group(1),
    # client_id = match.group(2),
    # user_id   = match.group(3),
    # timestamp = parse_apache_time(match.group(4)),
    # method    = match.group(5),
    # endpoint  = match.group(6),
    # protocol  = match.group(7),
    # http_code = int(match.group(8)),
    # bytes     = 0 if match.group(9) == '-' else int(match.group(9))
    try:
        return {
            "host"      : match.group(1),
            "timestamp" : datetime.strptime(match.group(4), "%d/%b/%Y:%H:%M:%S %z"),
            "request"   : "{} {} {}".format(match.group(5), match.group(6), match.group(7)),
            "http_code" : match.group(8),
            "bytes"     : 0 if match.group(9) == '-' else int(match.group(9)),
        }
    except Exception as e:
        # logger.error(e)
        return {}


def run():
    data = [ "{pwd}/data/NASA_access_log_{y}.gz".format(pwd=PWD, y=x) for x in ["Jul95", "Aug95"] ]
    
    conf = SparkConf().setAll([("spark.executor.memory", "4g"), \
                               ("spark.executor.cores", "4"), \
                               ("spark.cores.max", "4"), \
                               ("spark.driver.memory","4g")])

    spark = SparkSession.builder \
                        .appName("Test Spark") \
                        .getOrCreate()   
    
    # spark.sparkContext.setLogLevel("INFO")
    # log = spark._jvm.org.apache.log4j.LogManager.getLogger("NASA Test")
    # log.warn("build a new instance.")
    
    logger.info("Build a new instance.")
    logger.info("Read files: {data}.".format(data=data))
    df = spark.read.text(data)

    logger.info("Create rdd used method parser.")
    rdd = df.rdd.map(parser)

    logger.info("Parser rdd to df with schema.")
    df = rdd.toDF(schema=schema())

    logger.info("Solution 5 questions.")
    logger.info("Question 1: What is the number of unique hosts?")
    df.select(df.host).distinct().agg(F.count(df.host).alias("hosts")).show(truncate=False)

    logger.info("Question 2: What is the total number of 404 errors?")
    df.filter(df.http_code == "404").agg(F.count(df.http_code).alias("errors")).show(truncate=False)

    logger.info("Question 3: Which ​5 URLs​​ cause the most 404 errors?")
    grouped = df.filter(df.http_code == "404") \
                .groupBy(df.host,df.http_code) \
                .agg(F.count(df.host).alias("errors"))
    grouped.orderBy(grouped.errors.desc()).show(5, truncate=False)

    logger.info("Question 4: How many 404 erros per day?")
    grouped = df.filter(df.http_code == "404") \
                .withColumn("date",F.to_date(df.timestamp))  
    grouped.groupBy("date","http_code") \
           .agg(F.count("http_code").alias("errors")) \
           .orderBy(grouped.date.desc()).show(62, truncate=False)

    logger.info("Question 5: What is the total number of bytes returned?")
    df.agg(F.sum(df.bytes).alias("total_bytes")).show(truncate=False)


if __name__ == "__main__":
    run()