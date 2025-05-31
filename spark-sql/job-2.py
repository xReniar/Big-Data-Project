#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, concat_ws, array, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

def process_row(row):
    city = row['city']
    year = row['year']
    fascia = row['fascia']
    numero_macchine = row['numero_macchine']
    avg_daysonmarket = row['avg_daysonmarket']
    descriptions:list[str] = row['descriptions_list']

    # count words
    word_counts = {}
    for word in descriptions:
        if len(word) > 0 and word.isalpha():
            word_counts[word] = word_counts.get(word, 0) + 1

    sorted_words = sorted(word_counts.items(), key=lambda x: (x[1]), reverse=True)
    top_3 = list(map(lambda x: x[0], sorted_words[:3]))

    return (city, year, fascia, numero_macchine, avg_daysonmarket, top_3)

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("spark-sql#job-2") \
    .getOrCreate()

schema = StructType([
    StructField(name="city", dataType=StringType(), nullable=True),
    StructField(name="daysonmarket", dataType=IntegerType(), nullable=True),
    StructField(name="description", dataType=StringType(), nullable=True),
    StructField(name="engine_displacement", dataType=DoubleType(), nullable=True),
    StructField(name="horsepower", dataType=DoubleType(), nullable=True),
    StructField(name="make_name", dataType=StringType(), nullable=True),
    StructField(name="model_name", dataType=StringType(), nullable=True),
    StructField(name="price", dataType=DoubleType(), nullable=True),
    StructField(name="year", dataType=IntegerType(), nullable=True)
])

df = spark.read \
    .csv(args.input, schema=schema) \
    .select("city", "daysonmarket", "description", "price", "year")

df = df.filter(
    col("daysonmarket").rlike("^[0-9]+$")
).createOrReplaceTempView("dataset")

query = """
SELECT 
    city, 
    year, 
    CASE 
        WHEN price < 20000 THEN 'basso'
        WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
        ELSE 'alto'
    END AS fascia,
    COUNT(*) AS numero_macchine,
    AVG(daysonmarket) AS avg_daysonmarket,
    COLLECT_LIST(description) AS descriptions_list
FROM dataset
GROUP BY city, year, 
    CASE 
        WHEN price < 20000 THEN 'basso'
        WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
        ELSE 'alto'
    END
"""

final_report = spark.sql(query)

# concatenate all the descriptions and adjust average value
final_report = final_report \
    .withColumn("avg_daysonmarket", spark_round(col("avg_daysonmarket"), 2)) \
    .withColumn("descriptions_list", array(concat_ws(" ", col("descriptions_list"))))

# turn column from array to string and split using space
final_report = final_report.withColumn("descriptions_list", split(col("descriptions_list")[0], " "))

df_rdd = final_report.select("city", "year", "fascia", "numero_macchine", "avg_daysonmarket", "descriptions_list").rdd

processed_rdd = df_rdd.map(process_row)

schema = StructType([
    StructField("city", StringType(), False),
    StructField("year", StringType(), False),
    StructField("fascia", StringType(), False),
    StructField("num_macchine", IntegerType(), False),
    StructField("avg_daysonmarket", DoubleType(), False),
    StructField("top_3_words", ArrayType(StringType()), False)
])

final_result_df = spark.createDataFrame(processed_rdd, schema)
final_result_df = final_result_df.withColumn("top_3_words", concat_ws(",", col("top_3_words")))

final_result_df.write \
    .option("header", False) \
    .mode("append") \
    .csv(args.output)

final_result_df.show(n = 10)

spark.stop()