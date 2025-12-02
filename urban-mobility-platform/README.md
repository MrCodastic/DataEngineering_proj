# BankingDataEngineering# 🏙️ Urban Mobility Data Platform

## 📖 Project Overview
An End-to-End Data Engineering project simulating a Smart City traffic system. This platform ingests real-time GPS data from vehicles (Streaming) and combines it with driver registration data (Batch) to identify speeding violations in real-time.

It implements the **Lambda Architecture** (Speed Layer + Batch Layer) and the **Medallion Architecture** (Bronze/Silver/Gold) using a modern tech stack.

## 🏗️ Architecture
* **Source:** Python Faker scripts generating users and live GPS coordinates.
* **Ingestion:** Apache Kafka & Zookeeper (Dockerized).
* **Batch Layer:** Postgres DB & Apache Airflow (Nightly extraction).
* **Speed Layer:** Apache Spark Structured Streaming.
* **Storage:** Parquet Data Lake (Bronze/Silver/Gold).
* **Serving:** Streamlit (Live Dashboard) & DuckDB (SQL Analytics).

## 🛠️ Tech Stack
* **Language:** Python 3.12
* **Containerization:** Docker & Docker Compose
* **Streaming:** Kafka, Spark Structured Streaming
* **Orchestration:** Apache Airflow
* **Database:** PostgreSQL
* **Analysis:** DuckDB, Pandas

## 📂 Data Flow (The Medallion Model)
1.  **Bronze:** Raw JSON dumps from Kafka and raw SQL dumps from Postgres.
2.  **Silver:** Cleaned data with proper timestamps, types, and deduplication.
3.  **Gold:** Aggregated metrics (Average Speed per Minute) & Enriched Reports.

## 🚀 How to Run
(See RUN_INSTRUCTIONS.md for step-by-step commands)