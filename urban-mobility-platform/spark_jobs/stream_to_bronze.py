import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# 1. Initialize Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("MobilityBronzeIngestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Reduce noise

# 2. Define the Schema (Structure of the incoming data)
# Matches the generator: {'vehicle_id': ..., 'latitude': ..., 'longitude': ..., 'speed': ..., 'timestamp': ...}
schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

# 3. Read from Kafka
# We read the raw data as a binary key/value pair
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle_gps") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transform (Parse JSON)
# Convert binary 'value' column to string, then parse using our schema
parsed_df = raw_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp()) # Add metadata

# 5. Write to Data Lake (Bronze Layer)
# We use "checkpointLocation" so Spark knows where it left off if it crashes
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./datalake/bronze/gps_data") \
    .option("checkpointLocation", "./datalake/checkpoints/gps_data") \
    .trigger(processingTime='5 seconds') \
    .start()

print("Streaming started... Data is writing to ./datalake/bronze/gps_data")
query.awaitTermination()