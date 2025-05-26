#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os

USER = os.getenv("USER")

spark = SparkSession.builder \
    .appName("spark-core#job-2") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(f"/user/{USER}/data/data_cleaned.csv")