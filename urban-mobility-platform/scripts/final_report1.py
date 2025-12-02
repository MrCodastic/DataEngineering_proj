import duckdb
import os

print("--- 🚓 TRAFFIC ENFORCEMENT REPORT 🚓 ---")

# Define paths relative to the 'urban-mobility-platform' folder
silver_path = 'datalake/silver/gps_data/*.parquet'
bronze_users_path = 'datalake/bronze/users/*.parquet'

# The SQL Query (DuckDB can query Parquet files directly!)
query = f"""
WITH live_traffic AS (
    -- 1. Get the latest position/speed from the Silver Lake
    SELECT 
        vehicle_id, 
        speed,
        event_time
    FROM read_parquet('{silver_path}')
    -- Window Function: Keep only the absolute latest ping per vehicle
    QUALIFY row_number() OVER (PARTITION BY vehicle_id ORDER BY event_time DESC) = 1
),
drivers AS (
    -- 2. Get the Driver Names from the Airflow dump (Bronze Lake)
    SELECT user_id, name, address 
    FROM read_parquet('{bronze_users_path}')
)

-- 3. The Hybrid Join
SELECT 
    t.vehicle_id,
    d.name as driver_name,
    t.speed as current_speed_kmh,
    d.address,
    t.event_time
FROM live_traffic t
-- Fake Join Logic to map random Vehicles to Users (V-101 -> User 1, etc.)
JOIN drivers d ON CAST(SUBSTR(t.vehicle_id, 3) AS INT) % 5 + 1 = d.user_id
WHERE t.speed > 80 -- Only show people breaking the law!
ORDER BY t.speed DESC;
"""

try:
    # Execute the query
    df = duckdb.query(query).df()
    
    if df.empty:
        print("\n✅ No speeding vehicles detected at this moment.")
    else:
        print(f"\n🚨 FOUND {len(df)} SPEEDING VEHICLES 🚨")
        print(df.to_string(index=False))

except Exception as e:
    print(f"\n❌ Error: {e}")
    print("Troubleshooting:")
    print("1. Are you running this from the 'urban-mobility-platform' folder?")
    print("2. Did the Airflow DAG create the user parquet file?")
    print("3. Is the Spark job creating silver parquet files?")