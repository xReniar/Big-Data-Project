#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os
import argparse


ROOT_DIR = os.getenv("ROOT_DIR")

parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

def group_by_price(x):
    if x > 50000:
        return "alto"
    elif x > 20000:
        return "medio"
    else:
        return "basso"

def top_3_words(description: str):

    word_counts = {}
    for word in description.split():
        if len(word) > 0 and word.isalpha():
            word_counts[word] = word_counts.get(word, 0) + 1

    sorted_words = sorted(word_counts.items(), key=lambda x: (x[1]), reverse=True)
    top_3 = list(map(lambda x: x[0], sorted_words[:3]))

    return top_3

spark = SparkSession.builder \
    .appName("spark-core#job-2") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(args.input)

processed_RDD = rdd \
    .map(f=lambda line: line.split(",")) \
    .filter(f=lambda x: x[1].isdigit()) \
    .map(f=lambda x: (x[0], int(x[1]), x[2], group_by_price(float(x[7])), int(x[8]))) \
    .map(f=lambda x: ((x[0], x[4], x[3]), (1, x[1], x[2]))) \
    .reduceByKey(func=lambda a, b:(
        a[0] + b[0],
        a[1] + b[1],
        a[2] + b[2]
    )) \
    .map(lambda x: (
            x[0][0], # city
            x[0][1], # year
            x[0][2], # price category
            x[1][1], # number of cars
            round(x[1][1] / x[1][0], 2), # avg of daysonmarket
            top_3_words(x[1][2]) # top 3 words
        )
    )

for line in processed_RDD.take(10):
    print(line)

spark.stop()
#processed_RDD.saveAsTextFile(args.output)