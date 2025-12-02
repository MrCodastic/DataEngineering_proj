import streamlit as st
import pandas as pd
import time
import os

# 1. Page Config
st.set_page_config(
    page_title="Urban Mobility Control Center",
    layout="wide",
    page_icon="🚗"
)

st.title("🚗 Real-Time Vehicle Telemetry")

# 2. Define Data Loader
# We use caching but clear it regularly to force a reload
def load_data():
    try:
        # Read the Silver Parquet Folder using Pandas + PyArrow
        # This reads all partitioned files in the directory
        df = pd.read_parquet("./datalake/silver/gps_data")
        
        # Sort by time so we get the latest movements
        df = df.sort_values(by="event_time", ascending=False)
        return df
    except Exception as e:
        return pd.DataFrame()

# 3. Auto-Refresh Logic
placeholder = st.empty()

# 4. Main Loop
while True:
    df = load_data()
    
    with placeholder.container():
        if df.empty:
            st.warning("Waiting for data to land in Silver Layer...")
        else:
            # Create two columns
            kpi1, kpi2, kpi3 = st.columns(3)
            
            # KPI Metrics
            latest_speed = df.iloc[0]['speed']
            active_vehicles = df['vehicle_id'].nunique()
            total_records = len(df)
            
            kpi1.metric(label="Active Vehicles", value=active_vehicles)
            kpi2.metric(label="Latest Recorded Speed", value=f"{latest_speed} km/h")
            kpi3.metric(label="Total Data Points", value=total_records)

            # --- MAP VISUALIZATION ---
            st.subheader("📍 Live Fleet Position")
            # Streamlit map looks for 'lat'/'lon' columns, so we rename
            map_data = df.rename(columns={"latitude": "lat", "longitude": "lon"})
            # Show only the most recent location for each vehicle (Current State)
            latest_positions = map_data.drop_duplicates(subset=['vehicle_id'], keep='first')
            st.map(latest_positions)

            # --- RAW DATA ---
            with st.expander("View Raw Telemetry Feed"):
                st.dataframe(df.head(10))

    # Wait 2 seconds before refreshing
    time.sleep(2)