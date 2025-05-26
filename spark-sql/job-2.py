#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
import os


USER = os.getenv("USER")

spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("job-2") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .csv(f"/user/{USER}/data/data_cleaned.csv") \
    .select("city", "daysonmarket", "description", "price", "year", "model_name")

df = df.filter(
    col("daysonmarket").rlike("^[0-9]+$")
).withColumn("daysonmarket", col("daysonmarket").cast(IntegerType()))

df.createOrReplaceTempView("dataset")

#df_fasce.groupBy("city", "year", "fascia_prezzo").count().orderBy("city", "year", "fascia_prezzo").show()

query_completa = """
SELECT 
    city,
    year,
    SUM(CASE WHEN price > 50000 THEN 1 ELSE 0 END) AS num_fascia_alta,
    ROUND(AVG(CASE WHEN price > 50000 THEN daysonmarket ELSE NULL END), 2) AS giorni_fascia_alta,
    SUM(CASE WHEN price >= 20000 AND price <= 50000 THEN 1 ELSE 0 END) AS num_fascia_media,
    ROUND(AVG(CASE WHEN price >= 20000 AND price <= 50000 THEN daysonmarket ELSE NULL END), 2) AS giorni_fascia_media,
    SUM(CASE WHEN price < 20000 THEN 1 ELSE 0 END) AS num_fascia_bassa,
    ROUND(AVG(CASE WHEN price < 20000 THEN daysonmarket ELSE NULL END), 2) AS giorni_fascia_bassa
FROM dataset
GROUP BY city, year
ORDER BY city, year
"""

final_report = spark.sql(query_completa)
final_report.show()

