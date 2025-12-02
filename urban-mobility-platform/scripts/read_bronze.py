from pyspark.sql import SparkSession

# 1. Initialize Spark
spark = SparkSession.builder.appName("InspectBronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Read the Bronze Lake
# We read it as a standard batch (snapshot) to see what's there right now
df = spark.read.parquet("./datalake/bronze/gps_data")

print(f"\n--- Total Records Captured: {df.count()} ---")

# 3. Show the Raw Data
print("--- Sample Data ---")
df.show(5, truncate=False)

# 4. Show the Schema (Notice how everything is in the 'data' struct or raw columns)
df.printSchema()