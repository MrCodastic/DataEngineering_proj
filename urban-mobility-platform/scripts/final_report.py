import duckdb
import os

print("--- 🚓 TRAFFIC REPORT (ALL VEHICLES) 🚓 ---")

silver_path = 'datalake/silver/gps_data/*.parquet'
bronze_users_path = 'datalake/bronze/users/*.parquet'

query = f"""
WITH live_traffic AS (
    SELECT 
        vehicle_id, 
        speed,
        event_time
    FROM read_parquet('{silver_path}')
    QUALIFY row_number() OVER (PARTITION BY vehicle_id ORDER BY event_time DESC) = 1
),
drivers AS (
    SELECT user_id, name, address 
    FROM read_parquet('{bronze_users_path}')
)

SELECT 
    t.vehicle_id,
    d.name as driver_name,
    t.speed as current_speed_kmh,
    d.address,
    t.event_time
FROM live_traffic t
JOIN drivers d ON CAST(SUBSTR(t.vehicle_id, 3) AS INT) % 5 + 1 = d.user_id
-- WHERE clause removed to show all drivers
ORDER BY t.event_time DESC;
"""

try:
    df = duckdb.query(query).df()
    print(df.to_string(index=False))
except Exception as e:
    print(f"Error: {e}")
