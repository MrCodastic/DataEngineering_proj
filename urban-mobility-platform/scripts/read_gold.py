from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("InspectGold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n--- GOLD LAYER (Business Metrics) ---")

# Check if folder exists first to avoid crashing
import os
if not os.path.exists("./datalake/gold/vehicle_analytics"):
    print("Gold data hasn't landed yet! (Waiting for window to close...)")
else:
    df = spark.read.parquet("./datalake/gold/vehicle_analytics")
    
    # Sort by latest window
    df.orderBy(col("start").desc()).show(10, truncate=False)