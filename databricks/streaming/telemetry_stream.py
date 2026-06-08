# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming: Patient monitoring telemetry  (Phase 5 — Resume bullet 3)
# MAGIC Spark Structured Streaming over vitals events from `landing/telemetry/` (newline-JSON).
# MAGIC Demonstrates **event-time watermarking** + **windowed aggregations** and writes:
# MAGIC   * `silver.telemetry_vitals`        — raw cleansed events
# MAGIC   * `silver.telemetry_vitals_1min`   — 1-minute rolling aggregates per patient + anomaly flag

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from common import config

# COMMAND ----------
schema = StructType([
    StructField("event_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("heart_rate", IntegerType()),
    StructField("spo2", IntegerType()),
    StructField("systolic_bp", IntegerType()),
    StructField("diastolic_bp", IntegerType()),
    StructField("temperature_c", DoubleType()),
    StructField("event_time", TimestampType()),
])

raw_target = config.fqn(config.SCHEMA_SILVER, "telemetry_vitals")
agg_target = config.fqn(config.SCHEMA_SILVER, "telemetry_vitals_1min")
ckpt_raw = f"{config.CHECKPOINTS}/telemetry_raw"
ckpt_agg = f"{config.CHECKPOINTS}/telemetry_agg"

# COMMAND ----------
# MAGIC %md ## Source stream (file source = no Event Hubs cost)
# COMMAND ----------
events = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)
    .load(config.LANDING_TELEMETRY)
    .withColumn("spo2_anomaly", F.col("spo2") < F.lit(90))
)

# COMMAND ----------
# MAGIC %md ## 1) Persist cleansed raw events
# COMMAND ----------
(events.writeStream
 .format("delta")
 .option("checkpointLocation", ckpt_raw)
 .outputMode("append")
 .trigger(processingTime="10 seconds")
 .toTable(raw_target))

# COMMAND ----------
# MAGIC %md ## 2) Event-time windowed aggregation with watermark
# MAGIC 5-minute watermark drops very-late events; 1-minute tumbling window per patient.
# COMMAND ----------
agg = (
    events
    .withWatermark("event_time", "5 minutes")
    .groupBy(F.window("event_time", "1 minute"), F.col("patient_id"))
    .agg(
        F.avg("heart_rate").alias("avg_heart_rate"),
        F.avg("spo2").alias("avg_spo2"),
        F.min("spo2").alias("min_spo2"),
        F.max("systolic_bp").alias("max_systolic_bp"),
        F.avg("temperature_c").alias("avg_temp_c"),
        F.max(F.col("spo2_anomaly").cast("int")).cast("boolean").alias("had_spo2_anomaly"),
        F.count("*").alias("event_count"),
    )
    .select("patient_id", F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "avg_heart_rate", "avg_spo2", "min_spo2", "max_systolic_bp",
            "avg_temp_c", "had_spo2_anomaly", "event_count")
)

(agg.writeStream
 .format("delta")
 .option("checkpointLocation", ckpt_agg)
 .outputMode("append")
 .trigger(processingTime="10 seconds")
 .toTable(agg_target))

# COMMAND ----------
# MAGIC %md Inspect (run after a minute of streaming):
# MAGIC ```sql
# MAGIC SELECT * FROM rcm.silver.telemetry_vitals_1min WHERE had_spo2_anomaly ORDER BY window_start DESC;
# MAGIC ```
