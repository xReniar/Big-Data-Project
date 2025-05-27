#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os

USER = os.getenv("USER")

spark = SparkSession.builder \
    .appName("spark-core#job-1") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(f"/user/{USER}/data/data_cleaned.csv")

# read file and adjust datatypes
processed_RDD = rdd \
    .map(f=lambda line: line.split(",")) \
    .map(f=lambda x: (x[5], x[6], x[7], x[8])) \
    .map(f=lambda x: (x[0], x[1], float(x[2]), int(x[3])))

processed_RDD = processed_RDD.map(f=lambda line: ((line[0], line[1]), (line[2], line[3])))

grouped = processed_RDD.groupByKey()

for x in grouped.take(5):
    print(x)