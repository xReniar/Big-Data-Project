#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os


ROOT_DIR = os.getenv("ROOT_DIR")
spark = SparkSession.builder.appName("data-cleaning").getOrCreate()

df = spark.read \
    .option("header", True) \
    .csv(f"file://{ROOT_DIR}/data/used_cars_data.csv") \
    .select(
        "city",
        "daysonmarket",
        "description",
        "engine_displacement",
        "horsepower",
        "make_name", 
        "model_name",
        "price",
        "year"
    )


df.coalesce(1).write \
    .option("header", True) \
    .mode("append") \
    .csv(f"file://{ROOT_DIR}/data")