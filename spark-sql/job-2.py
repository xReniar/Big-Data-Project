#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


USER = os.getenv("USER")

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("job-2") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .csv(f"/user/{USER}/data/data_cleaned.csv") \
    .select("city", "daysonmarket", "description", "price", "year")

df.printSchema()