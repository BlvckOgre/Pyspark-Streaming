# 🚀 PySpark Structured Streaming in Databricks

## Overview

This project demonstrates a real-world implementation of **Spark Structured Streaming** using **Databricks**. It focuses on streaming semi-structured JSON data, transforming it in real time, and persisting it using Delta Lake. The objective is to explore different streaming trigger types, output modes, and apply best practices like archiving and late data handling.
It covers concepts from JSON ingestion to advanced topics like **stateful processing**, **watermarking**, **custom triggers**, **output modes**, and **batch-level operations**. The streaming workload is built atop a **managed volume** in the Databricks workspace using the latest Delta and Spark capabilities.
---

## Environment Setup

* **Platform**: Databricks
* **Schema**: `stream`
* **Volume**: `streaming`
* **Source Directory**: `/Volumes/workspace/stream/streaming/jsonsource/`
* **File Used**: `day1.json` (and later `day2.json` and `day3.json`)

---

## 🔐 Requirements

- Databricks Runtime with Spark 3.3+
- Delta Lake enabled workspace
- PySpark

---

## 📁 Project Structure

```
📆 streaming-project/
🗂️ notebooks/
│   🔍 01_streaming.py
│   🔍 02_output_modes.py
│   🔍 03_foreach_batch.py
│   🔍 04_window.py
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

### Directory Setup

```python
dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/jsonsourcenew")
dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/jsonsourceArchives")
dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/jsonsink")
dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/foreachsink/destination1")
dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/foreachsink/destination2")
dbutils.fs.mkdirs("/Volumes/workspace/stream/streaming/foreachsink/ckeckpointLocation")
```

---

## Streaming Pipeline Steps

### 1. Load and Inspect JSON

* Load multiline hierarchical JSON
* Inspect structure via JSON formatter (e.g., jsonformatter.org)

### 2. Flatten Nested Structure

* Use `explode_outer()` to safely handle arrays
* Select deep nested fields:

```python
df = df.select("order_id", "timestamp", "customer.customer_id", "customer.name", ...)
df = df.withColumn("items", explode_outer("items"))
df = df.withColumn("metadata", explode_outer("metadata"))
```

### 3. Streaming with Schema Inference

```python
spark.conf.set("spark.sql.streaming.schemaInference", True)
```

### 4. Define Custom Schema

Custom schemas are preferred for production:

```python
my_schema = """
order_id STRING,
timestamp STRING,
customer STRUCT<...>,
items ARRAY<STRUCT<...>>,
payment STRUCT<...>,
metadata ARRAY<STRUCT<key: STRING, value: STRING>>
"""
```

### 5. Write Stream to Delta

```python
df.writeStream.format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", ...) \
    .option("path", ...) \
    .start()
```

---

## Trigger Types

* `.trigger(once=True)` - batch-like, reliable
* `.trigger(processingTime="10 seconds")` - micro-batches
* `.trigger(availableNow=True)` - fast full-load
* `.trigger(continuous="10 seconds")` - row-level continuous

## Archiving Source Files

```python
.option("cleanSource", "archive")
.option("sourceArchiveDir", "/Volumes/.../jsonsourceArchives")
```

---

## Output Modes

### Complete Mode

```python
df.groupBy("color").agg(count("*").alias("count"))
  .writeStream.outputMode("complete")
```

### Append Mode with foreachBatch

```python
def myfunc(df, batch_id):
    df.groupBy("color").agg(count("*").alias("count"))
      .write.format("delta")
      .save("/Volumes/.../destination1")
```

---

## Window Functions and Watermarking

```python
df.groupBy("color", window("event_time", "10 seconds"))
  .agg(count(lit(1)).alias("color_count"))
  .writeStream.outputMode("complete")
```

### Late Data Handling

* Use `event_time` not `processing_time`
* Watermark helps trim state and manage memory

---

## 📌 Summary

This project helps you build a **robust, scalable streaming system** in Databricks that supports:

- Managed storage
- Schema evolution
- Real-time transformation and enrichment
- Late-arriving event handling
- Custom batch-level processing
---

## Key Learnings

* Schema inference is good for prototyping, but production should use defined schemas
* Flattening nested JSON with `explode_outer` is crucial
* Triggers offer flexibility depending on data arrival pattern
* Window functions enable activity tracking and time-bound analytics
* Archiving source files boosts stream management and traceability
* `foreachBatch` gives full control over complex streaming logic

---

## Conclusion

This project provided a comprehensive overview of how to structure and manage streaming data pipelines using Spark Structured Streaming. We covered ingestion, transformation, writing, and advanced techniques like windowing and archival—all within Databricks.
