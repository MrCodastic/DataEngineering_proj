# 🏁 Start-to-Finish Run Guide

### Phase 1: Setup
1.  **Install Requirements:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Start Infrastructure (Kafka, Postgres):**
    ```bash
    docker-compose up -d
    ```

### Phase 2: The "World" Generator
3.  **Start the Data Generator** (Keep this terminal open!):
    ```bash
    python scripts/data_generator.py
    ```

### Phase 3: The Streaming Pipeline
4.  **Start Bronze Ingestion** (Kafka -> Parquet):
    ```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_jobs/stream_to_bronze.py
    ```
5.  **Start Silver Cleaning** (Bronze -> Silver):
    *Open a new terminal.*
    ```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_jobs/stream_bronze_to_silver.py
    ```

### Phase 4: The Batch Layer (Airflow)
6.  **Initialize Airflow:**
    ```bash
    export AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=true
    airflow standalone
    ```
7.  **Login:** Go to Port 8080 (User: `admin`, Password: in terminal logs).
8.  **Trigger DAG:** Unpause and trigger `ingest_users_postgres_to_bronze`.

### Phase 5: Visualization & Reporting
9.  **Live Dashboard:**
    ```bash
    streamlit run dashboard.py
    ```
10. **Final Hybrid Report (The "Speeding Ticket" Logic):**
    ```bash
    python scripts/final_report.py
    ```

### Phase 6: Cleanup
11. **Stop Everything:**
    ```bash
    docker-compose down
    # Press Ctrl+C in all python terminals
    ```