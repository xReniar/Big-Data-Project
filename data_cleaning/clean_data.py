from pyspark.sql import SparkSession


CSV_PATH = "../data/used_cars_data.csv"

spark = SparkSession.builder.appName("Cleaning").getOrCreate()
df = spark.read.csv(CSV_PATH, header=True)
cleaned_df = df.na.drop()