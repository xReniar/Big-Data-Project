#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round
from pyspark.sql.types import IntegerType
import os


USER = os.getenv("USER")

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("spark-sql#job-2") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .csv(f"/user/{USER}/data/data_cleaned.csv") \
    .select("city", "daysonmarket", "description", "price", "year", "model_name")

df = df.filter(
    col("daysonmarket").rlike("^[0-9]+$")
).withColumn("daysonmarket", col("daysonmarket").cast(IntegerType()))

df.createOrReplaceTempView("dataset")

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
    AVG(daysonmarket) AS avg_daysonmarket
FROM dataset
GROUP BY city, year, 
    CASE 
        WHEN price < 20000 THEN 'basso'
        WHEN price BETWEEN 20000 AND 50000 THEN 'medio'
        ELSE 'alto'
    END
"""

final_report = spark.sql(query)
fina = final_report.withColumn("avg_price", spark_round(col("avg_price"), 2))
final_report.show()

