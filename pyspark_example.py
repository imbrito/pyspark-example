#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
from datetime import datetime
import logging

def schema():
    return StructType([StructField("host", StringType(), True),
                       StructField("timestamp", TimestampType(), True),
                       StructField("request", StringType(), True),
                       StructField("http_code", StringType(), True),
                       StructField("bytes", IntegerType(), True)])

def parser(row):
    raw = {}
    item = row.asDict()['value']
    item = str(item).split(' ')
    try:
        raw["host"] = item[0]
        raw["timestamp"] = datetime.strptime(' '.join(item[3:5]).replace('[','').replace(']',''), "%d/%b/%Y:%H:%M:%S %z")
        raw["request"] = ' '.join(item[5:8])
        raw["http_code"] = item[8]
        raw["bytes"] = 0 if item[9] == '-' else int(item[9])
    except Exception as e:
        # logging.error(e)
        return {}
    return raw

def format_logger():
    return {"datetime":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"level":"INFO "}

if __name__ == "__main__":
    
    print("{datetime} {level} Build a new instance.".format(**format_logger()))
    spark = SparkSession.builder \
                        .appName("Test Spark") \
                        .getOrCreate()    

    print("{datetime} {level} Read file: NASA_access_log_Jul95.gz.".format(**format_logger()))
    df = spark.read.text("NASA_access_log_Jul95.gz")

    print("{datetime} {level} Create rdd used method parser.".format(**format_logger()))
    rdd = df.rdd.map(parser)

    print("{datetime} {level} Parser rdd to df with schema.".format(**format_logger()))
    df = rdd.toDF(schema=schema())

    print("{datetime} {level} Solution 5 questions.".format(**format_logger()))
    print("{datetime} {level} Question 1: Número​ de​ hosts​ únicos.".format(**format_logger()))
    df.select(df.host).distinct().agg(F.count(df.host).alias("hosts")).show(truncate=False)

    print("{datetime} {level} Question 2: O total de erros 404.".format(**format_logger()))
    df.filter(df.http_code == "404").agg(F.count(df.http_code).alias("errors")).show(truncate=False)

    print("{datetime} {level} Question 3: Os​ ​5 URLs​​ que​ mais​​ causaram​ ​erro​ 404.".format(**format_logger()))
    grouped = df.filter(df.http_code == "404") \
                .groupBy(df.host,df.http_code) \
                .agg(F.count(df.host).alias("errors"))
    grouped.orderBy(grouped.errors.desc()).show(5, truncate=False)

    print("{datetime} {level} Question 4: Quantidade de erros 404 por dia.".format(**format_logger()))
    grouped = df.filter(df.http_code == "404") \
                .withColumn("date",F.to_date(df.timestamp))  
    grouped.groupBy("date","http_code") \
           .agg(F.count("http_code").alias("errors")) \
           .orderBy(grouped.date.desc()).show(31, truncate=False)

    print("{datetime} {level} Question 5: O total de bytes retornados.".format(**format_logger()))
    df.agg(F.sum(df.bytes).alias("total_bytes")).show(truncate=False)