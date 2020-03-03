#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
from datetime import datetime
import os
import logging


logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%y/%m/%d %H:%M:%S", level=logging.INFO)
logger = logger = logging.getLogger(__name__)

PWD = os.getenv("PWD")


def schema():
    return StructType([StructField("host", StringType(), True),
                       StructField("timestamp", TimestampType(), True),
                       StructField("request", StringType(), True),
                       StructField("http_code", StringType(), True),
                       StructField("bytes", IntegerType(), True)])


def parser(row):
    raw = {}
    item = row.asDict()["value"]
    item = str(item).split(' ')
    try:
        raw["host"] = item[0]
        raw["timestamp"] = datetime.strptime(' '.join(item[3:5]).replace('[','').replace(']',''), "%d/%b/%Y:%H:%M:%S %z")
        raw["request"] = ' '.join(item[5:8])
        raw["http_code"] = item[8]
        raw["bytes"] = 0 if item[9] == '-' else int(item[9])
    except Exception as e:
        # logger.error(e)
        return {}
    return raw


def run():
    data = [ "{pwd}/data/NASA_access_log_{y}.gz".format(pwd=PWD, y=x) for x in ["Jul95", "Aug95"] ]
    
    conf = SparkConf().setAll([("spark.executor.memory", "4g"), \
                               ("spark.executor.cores", "4"), \
                               ("spark.cores.max", "4"), \
                               ("spark.driver.memory","4g")])

    spark = SparkSession.builder \
                        .config(conf=conf) \
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