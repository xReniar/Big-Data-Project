from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max, avg, collect_set
import os


ROOT_DIR = os.getenv("ROOT_DIR")
CSV_PATH = f"file:///{ROOT_DIR}/data/used_cars_data.csv"

# Inizializzazione Spark
spark = SparkSession.builder \
    .appName("UsedCarsAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# Caricamento dei dati
df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)

# Pulizia dei dati (esempio base)
cleaned_df = df.na.drop(subset=["make_name", "model_name", "price", "year"])

# Job 1: Statistiche per marca e modello
results = cleaned_df.groupBy("make_name", "model_name") \
    .agg(
        count("*").alias("count_vehicles"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("price").alias("avg_price"),
        collect_set("year").alias("years_available")
    ) \
    .orderBy("make_name", "model_name")

# Mostra i primi 10 risultati
results.show(10, truncate=False)

# Salva i risultati
results.write.mode("overwrite").csv("output/job1_results", header=True)