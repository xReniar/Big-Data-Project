#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, round as spark_round, sort_array
import os


USER = os.getenv("USER")

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("spark-sql#job-1") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .csv(f"/user/{USER}/data/data_cleaned.csv") \
    .select("make_name", "model_name", "price", "year")

df.createOrReplaceTempView("dataset")

model_stats_query = """
SELECT 
    make_name,
    model_name,
    COUNT(*) as num_cars,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    COLLECT_SET(year) as years_list
FROM dataset
GROUP BY make_name, model_name
"""

model_stats = spark.sql(model_stats_query)
model_stats.createOrReplaceTempView("model_statistics")

model_stats = model_stats \
    .withColumn("avg_price", spark_round(col("avg_price"), 2)) \
    .withColumn("years_list", concat_ws(",", col("years_list")))

model_stats.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"/user/{USER}/spark-sql/job-1")

model_stats.show(n=10)

spark.stop()