from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count

# 1. Initialize
spark = SparkSession.builder.appName("MobilityGoldLayer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("--- Starting Gold Stream (Aggregations) ---")

# 2. Schema Inference (from Silver)
silver_schema = spark.read.parquet("./datalake/silver/gps_data").schema

# 3. Read Stream (From Silver)
silver_df = spark.readStream \
    .schema(silver_schema) \
    .format("parquet") \
    .load("./datalake/silver/gps_data")

# 4. GOLD LOGIC (Aggregations)
# We calculate the Average Speed & Total Pings per vehicle, per minute.
gold_df = silver_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("vehicle_id")
    ) \
    .agg(
        avg("speed").alias("avg_speed"),
        count("*").alias("total_pings")
    ) \
    .select("window.start", "window.end", "vehicle_id", "avg_speed", "total_pings")

# 5. Write Stream (To Gold Files)
# 'append' mode with aggregations only outputs when the window is CLOSED (watermark passed)
# query = gold_df.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "./datalake/gold/vehicle_analytics") \
#     .option("checkpointLocation", "./datalake/checkpoints/gold_analytics") \
#     .start()

# print("Gold Stream active... Waiting for windows to close to write files.")
# query.awaitTermination()


# 5. Write Stream (DEBUG MODE: To Console)
# We use 'update' mode so you see results IMMEDIATELY, even before the window closes.
print("Gold Stream active... Printing directly to console for debugging.")

query = gold_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()