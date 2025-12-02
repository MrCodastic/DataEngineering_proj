from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("InspectSilver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n--- SILVER LAYER (Clean Data) ---")
df = spark.read.parquet("./datalake/silver/gps_data")
df.show(5, truncate=False)
print(f"Total Clean Records: {df.count()}")