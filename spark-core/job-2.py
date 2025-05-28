#!/usr/bin/env python3

from pyspark.sql import SparkSession
import os

USER = os.getenv("USER")
ROOT_DIR = os.getenv("ROOT_DIR")


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

rdd = spark.sparkContext.textFile(f"/user/{USER}/data/data_cleaned.csv")

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

processed_RDD.coalesce(1).saveAsTextFile(f"file:///{ROOT_DIR}/spark-core/job-2-result")