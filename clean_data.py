#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType
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

df = df.filter(
    col("price").rlike("^[0-9]+(\.[0-9]+)?$") &
    col("year").rlike("^[0-9]{4}$")
).withColumn("price", col("price").cast(DoubleType())) \
 .withColumn("year", col("year").cast(IntegerType()))


df.coalesce(1).write \
    .option("header", True) \
    .mode("append") \
    .csv(f"file://{ROOT_DIR}/data")