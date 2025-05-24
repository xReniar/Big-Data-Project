from pyspark.sql import SparkSession


CSV_PATH = "file:///home/rainer/Code/Big-Data-Project/data/used_cars_data.csv"

'''spark = SparkSession.builder.appName("Cleaning").getOrCreate()
df = spark.read.csv(CSV_PATH, header=True)
cleaned_df = df.na.drop()'''


spark = SparkSession.builder \
    .appName("UsedCarsCSV") \
    .master("local[*]") \
    .getOrCreate()

# Legge il CSV (anche file grandi)
df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)

# Mostra prime 5 righe
#df.select("make_name").show()
valori_make_unici = df.select("model_name").rdd.flatMap(lambda x: x).collect()
with open("file.txt", "w") as file:
    for item in valori_make_unici:
        file.write(f"{str(item)}\n")