# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Encounter fact-source  (Phase 4)
# MAGIC Cleanses encounters, enriches ICD descriptions, and demonstrates **late-arriving data** handling
# MAGIC via watermark + MERGE on the business key. Output `silver.encounter` is a clean conformed table.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import audit, config, dq

# COMMAND ----------
bronze_path = f"{config.BRONZE}/emr/encounter/"
icd_ref = config.fqn(config.SCHEMA_BRONZE, "ref_icd")
target = config.fqn(config.SCHEMA_SILVER, "encounter")
quarantine_path = f"{config.QUARANTINE}/encounter/"

# Late-arrival watermark: ignore records older than the max already-seen start_time minus a grace window.
LATE_GRACE_DAYS = 7

# COMMAND ----------
with audit.log_load(spark, "silver_encounter", bronze_path, target, "incremental_merge") as a:
    raw = spark.read.option("recursiveFileLookup", "true").parquet(bronze_path)
    a["rows_read"] = raw.count()

    rules = [
        dq.not_null("encounter_id"),
        dq.not_null("hospital_id"),
        dq.not_null("patient_id"),
        dq.not_null("start_time"),
    ]
    valid, bad = dq.split_valid_quarantine(raw, rules)
    dq.write_quarantine(bad, quarantine_path)

    # Dedup latest per encounter
    w = Window.partitionBy("encounter_id", "hospital_id").orderBy(F.col("modified_at").desc_nulls_last())
    deduped = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

    # Late-arriving filter: drop records that are older than the established watermark - grace.
    if spark.catalog.tableExists(target):
        wm = spark.table(target).agg(F.max("start_time").alias("m")).collect()[0]["m"]
        if wm is not None:
            cutoff = F.lit(wm) - F.expr(f"INTERVAL {LATE_GRACE_DAYS} DAYS")
            late = deduped.filter(F.col("start_time") < cutoff)
            dq.write_quarantine(late.withColumn("dq_failed_rules", F.array(F.lit("late_arriving")))
                                    .withColumn("dq_quarantined_at", F.current_timestamp()),
                                quarantine_path)
            deduped = deduped.filter(F.col("start_time") >= cutoff)

    # Enrich ICD description
    icd = spark.table(icd_ref).select(F.col("icd_code"), F.col("description").alias("icd_description"))
    enriched = deduped.join(icd, on="icd_code", how="left")

    cdm = enriched.select(
        "encounter_id", "hospital_id", "patient_id", "provider_id", "dept_id",
        "encounter_type", "procedure_code", "icd_code", "icd_description",
        "start_time", "end_time",
    )

    # MERGE (upsert) on business key so late corrections update in place
    cdm.createOrReplaceTempView("enc_updates")
    if not spark.catalog.tableExists(target):
        cdm.write.format("delta").saveAsTable(target)
    else:
        spark.sql(f"""
            MERGE INTO {target} t
            USING enc_updates s
              ON t.encounter_id = s.encounter_id AND t.hospital_id = s.hospital_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    a["rows_written"] = spark.table(target).count()

# COMMAND ----------
display(spark.table(target).limit(20))
