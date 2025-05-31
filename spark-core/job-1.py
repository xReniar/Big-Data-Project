#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os
import argparse


ROOT_DIR = os.getenv("ROOT_DIR")

parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

spark = SparkSession.builder \
    .appName("spark-core#job-1") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(args.input)

# read file and adjust datatypes
processed_RDD = rdd \
    .map(f=lambda line: line.split(",")) \
    .map(f=lambda x: (
        (x[5], x[6]),
        (1, float(x[7]), float(x[7]), float(x[7]), set([int(x[8])]))
    )) \
    .reduceByKey(func=lambda v1, v2: (
        v1[0] + v2[0],      # num_cars
        v1[1] + v2[1],      # sum_price
        min(v1[2], v2[2]),  # min_price
        max(v1[3], v2[3]),  # max_price
        v1[4] | v2[4]       # years
    )) \
    .map(lambda x: (
        x[0][0],                     # make_name
        x[0][1],                     # model_name
        x[1][0],                     # num_cars
        x[1][2],                     # min_price
        x[1][3],                     # max_price
        round(x[1][1]/x[1][0], 2),   # avg_price
        sorted(list(x[1][4]))        # years
    ))

for line in processed_RDD.take(10):
    print(line)

spark.stop()
#processed_RDD.saveAsTextFile(args.output)