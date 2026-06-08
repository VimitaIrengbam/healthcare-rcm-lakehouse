# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Patient dimension  (Phase 4 — Resume bullets 1 & 4)
# MAGIC Reads bronze EMR patients, applies DQ quarantine + dedup + PII masking, then maintains
# MAGIC `silver.patient` as an SCD Type 2 dimension. Multi-hospital sources conform to one CDM entity.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import audit, config, dq, masking, scd

# COMMAND ----------
bronze_path = f"{config.BRONZE}/emr/patients/"
quarantine_path = f"{config.QUARANTINE}/patient/"
target = config.fqn(config.SCHEMA_SILVER, "patient")

# COMMAND ----------
with audit.log_load(spark, "silver_patient", bronze_path, target, "scd2") as a:
    raw = spark.read.parquet(bronze_path)
    a["rows_read"] = raw.count()

    # 1) Data quality -> quarantine invalid rows
    rules = [
        dq.not_null("patient_id"),
        dq.not_null("hospital_id"),
        dq.in_set("gender", ["M", "F", "male", "female", "O", "U", "other", "unknown"]),
    ]
    valid, bad = dq.split_valid_quarantine(raw, rules)
    dq.write_quarantine(bad, quarantine_path)

    # 2) Deduplicate -> keep latest per (patient_id, hospital_id)
    w = Window.partitionBy("patient_id", "hospital_id").orderBy(F.col("modified_at").desc_nulls_last())
    deduped = (valid.withColumn("_rn", F.row_number().over(w))
                    .filter("_rn = 1").drop("_rn"))

    # 3) PII masking/redaction (defense in depth; UC column masks also applied at query time)
    masked = masking.apply_default_pii(deduped)
    masked = masking.redact_name(masked, "lastname")  # keep firstname for demos, redact last

    # 4) Conform to CDM columns
    cdm = masked.select(
        "patient_id", "hospital_id", "firstname", "lastname",
        F.col("dob").alias("birth_year"), "gender",
        "city", "state", "zip", "ssn",
    )

    # 5) SCD Type 2 upsert
    scd.scd2_merge(
        spark, target, cdm,
        business_keys=["patient_id", "hospital_id"],
        tracked_cols=["firstname", "lastname", "birth_year", "gender", "city", "state", "zip"],
    )
    a["rows_written"] = spark.table(target).filter("is_current = true").count()

# COMMAND ----------
display(spark.table(target).limit(20))
