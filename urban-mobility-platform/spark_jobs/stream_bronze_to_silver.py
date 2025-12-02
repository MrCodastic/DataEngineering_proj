from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp

# 1. Initialize Spark
spark = SparkSession.builder.appName("MobilitySilverLayer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("--- Starting Silver Stream ---")

# 2. Schema Inference
# We grab the schema from the existing bronze files
bronze_schema = spark.read.parquet("./datalake/bronze/gps_data").schema

# 3. Read Stream (From Bronze Files)
raw_bronze_df = spark.readStream \
    .schema(bronze_schema) \
    .format("parquet") \
    .load("./datalake/bronze/gps_data")

# 4. Transformations (The "Cleaning" Logic)
# Clean version without interleaved comments
silver_df = raw_bronze_df \
    .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp")))) \
    .filter(col("speed") < 200) \
    .dropDuplicates(["vehicle_id", "timestamp"]) \
    .select("vehicle_id", "speed", "latitude", "longitude", "event_time")

# 5. Write Stream (To Silver Files)
query = silver_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./datalake/silver/gps_data") \
    .option("checkpointLocation", "./datalake/checkpoints/silver_gps") \
    .start()

print("Streaming active... reading from Bronze -> writing to Silver.")
query.awaitTermination()