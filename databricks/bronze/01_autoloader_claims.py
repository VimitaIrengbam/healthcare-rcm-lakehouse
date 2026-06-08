# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Claims ingestion via Auto Loader  (Phase 2b — Resume bullet 2)
# MAGIC Incrementally ingests insurance claims flat files from `landing/claims/` into `bronze/claims/`
# MAGIC as parquet, using Databricks Auto Loader (`cloudFiles`) with schema evolution + checkpointing.
# MAGIC Uses the `availableNow` trigger to run batch-style on a schedule (monthly / twice-monthly).

# COMMAND ----------
from pyspark.sql import functions as F

from common import audit, config

# COMMAND ----------
schema_location = f"{config.CHECKPOINTS}/claims/_schema"
checkpoint_location = f"{config.CHECKPOINTS}/claims/_ckpt"
target_table = config.fqn(config.SCHEMA_BRONZE, "claims")

# COMMAND ----------
with audit.log_load(
    spark, pipeline_name="bronze_claims_autoloader",
    source=config.LANDING_CLAIMS, target=target_table, load_type="incremental",
) as a:

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(config.LANDING_CLAIMS)
        # ingestion metadata for lineage
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )

    query = (
        stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()

    out = spark.table(target_table)
    a["rows_written"] = out.count()

# COMMAND ----------
display(spark.table(target_table).orderBy(F.desc("_ingest_ts")).limit(20))
