#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max, avg, collect_set, sort_array, struct
from pyspark.sql.types import IntegerType, DoubleType
import os


USER = os.getenv("USER")

spark = SparkSession.builder.appName("job-1").getOrCreate()
df = spark.read \
    .option("header", True) \
    .csv(f"/user/{USER}/data/data_cleaned.csv") \
    .select("make_name", "model_name", "price", "year")


df_clean = df.filter(
    col("price").rlike("^[0-9]+(\.[0-9]+)?$") &
    col("year").rlike("^[0-9]{4}$")
).withColumn("price", col("price").cast(DoubleType())) \
 .withColumn("year", col("year").cast(IntegerType()))

model_stats = df_clean.groupBy("make_name", "model_name").agg(
    count("*").alias("count"),
    spark_min("price").alias("min_price"),
    spark_max("price").alias("max_price"),
    avg("price").alias("avg_price"),
    sort_array(collect_set("year")).alias("years")
)

brand_stats = model_stats.groupBy("make_name").agg(
    collect_set(
        struct(
            col("model_name"),
            col("count"),
            col("min_price"),
            col("max_price"),
            col("avg_price"),
            col("years")
        )
    ).alias("models")
)

def format_row(row):
    lines = []
    make_name = row["make_name"]
    lines.append(f"MARCA: {make_name}")
    lines.append("-" * 50)

    for model in row["models"]:
        lines.append(f"  Modello: {model['model_name']}")
        lines.append(f"    - Numero di auto: {model['count']}")
        lines.append(f"    - Prezzo minimo: ${model['min_price']:,.2f}")
        lines.append(f"    - Prezzo massimo: ${model['max_price']:,.2f}")
        lines.append(f"    - Prezzo medio: ${model['avg_price']:,.2f}")
        lines.append(f"    - Anni disponibili: {list(model['years'])}")
        lines.append("")

    lines.append("=" * 60)
    lines.append("")
    return "\n".join(lines)

formatted_rdd = brand_stats.rdd.map(format_row)

USER = os.getenv("USER")
formatted_rdd.saveAsTextFile(f"/user/{USER}/spark-sql/job-1")