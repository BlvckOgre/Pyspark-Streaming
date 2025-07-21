# 🚀 PySpark Structured Streaming in Databricks

This project demonstrates an end-to-end implementation of **PySpark Structured Streaming** using **Databricks**. It covers concepts from JSON ingestion to advanced topics like **stateful processing**, **watermarking**, **custom triggers**, **output modes**, and **batch-level operations**. The streaming workload is built atop a **managed volume** in the Databricks workspace using the latest Delta and Spark capabilities.

---

## 📁 Project Structure

```
📆 streaming-project/
🗂️ notebooks/
│   🔍 01_stream_schema_and_volume_setup.py
│   🔍 02_ingest_json_stream.py
│   🔍 03_stateless_stateful_transforms.py
│   🔍 04_output_modes_and_sink.py
│   🔍 05_watermarking_and_late_data.py
│   🔍 06_for_each_batch_processing.py
│   🔍 07_archiving_and_triggering.py
🗂️ resources/
│   🔍 sample_json_files/
README.md
```

---

## 🧠 Concepts Covered

### 1. Evolution of Structured Streaming

- Initially, Spark Streaming was DStream-based (RDD-like).
- Structured Streaming provides a high-level, declarative API (DataFrame/Dataset) with end-to-end fault tolerance.
- Supports continuous ingestion, exactly-once semantics, and integration with Delta Lake.

### 2. Ingesting JSON Streaming Data

- We use `readStream` to ingest JSON files continuously from a **managed volume**.
- Schema inference or explicitly defined schema helps ensure stability.

### 3. Stateless vs Stateful Transformations

- **Stateless:** `select`, `filter`, `withColumn`, `dropDuplicates`.
- **Stateful:** `groupByKey`, `mapGroupsWithState`, `flatMapGroupsWithState`, useful for sessionization or deduplication across batches.

### 4. Caching Streaming Data

- Use `persist()` judiciously with streaming DataFrames (mainly for debugging or reusing transformed streams).

### 5. Mastering Output Modes

- **Append:** Only new rows.
- **Update:** Updated rows only.
- **Complete:** All rows for aggregations.
- We demonstrate all modes with sinks like `console`, `memory`, and `Delta`.

### 6. Archiving Source Files

- JSON files ingested from the volume are **archived/moved** after successful processing.
- Use `dbutils.fs.mv()` in a foreachBatch for cleanup.

### 7. Triggers and Processing Modes

- **Available triggers:**
  - `Trigger.ProcessingTime("10 seconds")`
  - `Trigger.Once()`
  - `Trigger.Continuous("1 second")` (experimental)
- Triggers define micro-batch frequency or continuous mode.

### 8. Watermarking for Late Data

- Late-arriving data handled using `withWatermark(eventTime, delay)` with aggregations.
- Ensures proper state clean-up and event-time semantics.

### 9. Custom `foreachBatch` Functions

- Handle complex ETL logic inside each batch.
- Useful for dynamic partitioning, auditing, archiving, or external sink writes.

---

## 🏗️ Setup in Databricks

### 🔹 Create a Streaming Schema and Managed Volume

```python
# Create schema for the streaming workspace
spark.sql("CREATE SCHEMA IF NOT EXISTS streaming_workspace")

# Create managed volume inside the schema
spark.sql("""
    CREATE VOLUME IF NOT EXISTS streaming_workspace.json_volume
    COMMENT 'Managed volume for streaming JSON data'
""")
```

### 🔹 Upload JSON Files to Volume

Upload sample JSON files to the volume path:

```bash
# Sample path to volume
Volumes/streaming_workspace/json_volume/
```

You can use the UI or:

```python
dbutils.fs.cp("path/to/local/sample.json", "dbfs:/Volumes/streaming_workspace/json_volume/")
```

---

## 📦 Sample Code Snippet: Reading from JSON Stream

```python
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import *

schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("event_time", TimestampType())

df_stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json("/Volumes/streaming_workspace/json_volume/")
)
```

---

## ✅ Running the Stream

You can run and stop the stream with:

```python
query = df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/stream1") \
    .start("/mnt/output_path")
```

```python
query.stop()
```

---

## 🧪 Testing and Debugging

- Use `.writeStream.format("console")` for real-time debugging.
- Use `.trigger(Trigger.Once())` for development snapshots.

---

## 💾 Archiving Processed Files

In `foreachBatch`, archive files that were processed:

```python
def archive_files(batch_df, batch_id):
    processed_files = batch_df.select(input_file_name()).distinct().collect()
    for row in processed_files:
        src = row[0]
        dest = src.replace("json_volume", "json_volume_archive")
        dbutils.fs.mv(src, dest)

df_stream.writeStream.foreachBatch(archive_files).start()
```

---

## 🔐 Requirements

- Databricks Runtime with Spark 3.3+
- Delta Lake enabled workspace
- PySpark

---

## 📌 Summary

This project helps you build a **robust, scalable streaming system** in Databricks that supports:

- Managed storage
- Schema evolution
- Real-time transformation and enrichment
- Late-arriving event handling
- Custom batch-level processing
- Output optimization

---

## 📬 Contact

For feedback or contributions, reach out via issues or pull requests.

