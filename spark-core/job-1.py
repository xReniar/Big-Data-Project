#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os

USER = os.getenv("USER")
ROOT_DIR = os.getenv("ROOT_DIR")


def function(x):
    make_name: str = x[0]
    model_name: str = x[1]
    price_year_list: list[tuple[float, int]] = x[2]

    prices, years = zip(*price_year_list)

    min_price = min(prices)
    max_price = max(prices)
    avg_price = round(sum(prices) / len(prices), 2)
    distinct_years = sorted(set(years))

    return (make_name, model_name, len(price_year_list), min_price, max_price, avg_price, distinct_years)


spark = SparkSession.builder \
    .appName("spark-core#job-1") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(f"/user/{USER}/data/data_cleaned.csv")

# read file and adjust datatypes
processed_RDD = rdd \
    .map(f=lambda line: line.split(",")) \
    .map(f=lambda x: ((x[5], x[6]), (float(x[7]), int(x[8]))))

grouped = processed_RDD \
    .groupByKey() \
    .map(lambda x: (x[0][0], x[0][1], x[1])) \
    .map(lambda x: function(x))

grouped.coalesce(1).saveAsTextFile(f"file:///{ROOT_DIR}/spark-core/job-1-result")