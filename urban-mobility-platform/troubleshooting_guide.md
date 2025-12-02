# 🔧 Troubleshooting & Error Log
A collection of errors encountered during development and their solutions.

## 1. Spark Version Mismatch (NoSuchMethodError)
* **Error:** `java.lang.NoSuchMethodError: ... scala.Predef$.wrapRefArray`
* **Cause:** We ran `pyspark` (latest version 3.5/4.0) but used connectors for Spark 3.3/3.4.
* **Solution:** Downgrade PySpark to match the standard connectors.
    ```bash
    pip uninstall pyspark
    pip install pyspark==3.5.0
    ```

## 2. Docker Spark Image Not Found
* **Error:** `manifest for bitnami/spark:latest not found`
* **Cause:** Bitnami removed the `latest` tag, or regional registry issues with rolling tags.
* **Solution:** We pivoted to running Spark **Locally** (via `pip install pyspark`) instead of in Docker, which simplified development.

## 3. Airflow Login "Invalid URL"
* **Error:** `Invalid or unsafe next URL` or 400 Bad Request on Login.
* **Cause:** GitHub Codespaces runs on a proxy URL (`github.dev`), confusing Airflow's security settings.
* **Solution:** Export proxy variables before starting Airflow:
    ```bash
    export AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=true
    # Also clear the browser URL bar of any ?next=... parameters
    ```

## 4. Spark Job Cancelled / OOM
* **Error:** `Job aborted... SparkContext was shut down` or `RejectedExecutionException`.
* **Cause:** The Python process was killed (likely Out Of Memory) or corrupted checkpoint state.
* **Solution:**
    1. Delete the checkpoint folder: `rm -rf datalake/checkpoints/silver_gps`
    2. Restart the job.

## 5. IndentationError in Stream
* **Error:** `IndentationError: unexpected indent` inside the Spark chain.
* **Cause:** Placing `# comments` inside a multi-line Python command (`\`) breaks the interpreter.
* **Solution:** Move comments outside the chained command block.

## 6. DuckDB Module Not Found
* **Error:** `ModuleNotFoundError: No module named 'duckdb'`
* **Cause:** Library not installed.
* **Solution:** `pip install duckdb`