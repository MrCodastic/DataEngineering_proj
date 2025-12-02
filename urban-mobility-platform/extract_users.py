from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os

# --- CONFIG ---
PROJECT_ROOT = "/workspaces/BankingDataEngineering/urban-mobility-platform"
OUTPUT_PATH = f"{PROJECT_ROOT}/datalake/bronze/users"

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "mobility_db",
    "user": "user",
    "password": "password"
}

def extract_postgres_data():
    try:
        print("--- Connecting to Postgres ---")
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Extract Users
        df = pd.read_sql("SELECT * FROM users", conn)
        print(f"--- Extracted {len(df)} records ---")
        
        # Save to Data Lake
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        filename = f"users_extract_{datetime.now().strftime('%Y%m%d')}.parquet"
        full_path = f"{OUTPUT_PATH}/{filename}"
        
        df.to_parquet(full_path, index=False)
        print(f"--- Saved to {full_path} ---")
        conn.close()
    except Exception as e:
        print(f"ERROR: {e}")
        raise e

# --- DAG ---
default_args = { 'owner': 'data_engineer', 'retries': 0 }

with DAG(
    dag_id='ingest_users_postgres_to_bronze',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule='@daily', # The Fix for Airflow 3.0
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id='extract_users',
        python_callable=extract_postgres_data
    )

