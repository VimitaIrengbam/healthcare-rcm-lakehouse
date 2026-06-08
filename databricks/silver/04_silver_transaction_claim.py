# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transactions & Claims (CDM)  (Phase 4 — Resume bullet 4)
# MAGIC Conforms EMR transactions (bronze parquet from ADF) and insurance claims (bronze Delta from
# MAGIC Auto Loader) into clean `silver.transaction` and `silver.claim` tables feeding the gold KPIs.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import audit, config, dq

# COMMAND ----------
# MAGIC %md ## Transactions (from EMR via ADF)
# COMMAND ----------
txn_bronze = f"{config.BRONZE}/emr/transactions/"
txn_target = config.fqn(config.SCHEMA_SILVER, "transaction")

with audit.log_load(spark, "silver_transaction", txn_bronze, txn_target, "batch") as a:
    raw = spark.read.parquet(txn_bronze)
    a["rows_read"] = raw.count()
    rules = [
        dq.not_null("txn_id"), dq.not_null("hospital_id"),
        dq.non_negative("amount"), dq.non_negative("paid_amount"),
    ]
    valid, bad = dq.split_valid_quarantine(raw, rules)
    dq.write_quarantine(bad, f"{config.QUARANTINE}/transaction/")
    w = Window.partitionBy("txn_id", "hospital_id").orderBy(F.col("modified_at").desc_nulls_last())
    cdm = (valid.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
           .select("txn_id", "hospital_id", "encounter_id",
                   F.col("amount").alias("charge_amount"),
                   "paid_amount",
                   F.coalesce("adjustment", F.lit(0)).alias("adjustment"),
                   "payer", "status", "txn_date"))
    cdm.write.format("delta").mode("overwrite").saveAsTable(txn_target)
    a["rows_written"] = cdm.count()

# COMMAND ----------
# MAGIC %md ## Claims (from insurance via Auto Loader)
# COMMAND ----------
claims_bronze = config.fqn(config.SCHEMA_BRONZE, "claims")
claims_target = config.fqn(config.SCHEMA_SILVER, "claim")

with audit.log_load(spark, "silver_claim", claims_bronze, claims_target, "batch") as a:
    raw = spark.table(claims_bronze)
    a["rows_read"] = raw.count()
    # cast string columns landed by Auto Loader
    typed = (raw
             .withColumn("billed_amount", F.col("billed_amount").cast("double"))
             .withColumn("allowed_amount", F.col("allowed_amount").cast("double"))
             .withColumn("paid_amount", F.col("paid_amount").cast("double"))
             .withColumn("submission_date", F.to_date("submission_date")))
    rules = [
        dq.not_null("claim_id"), dq.not_null("encounter_id"),
        dq.non_negative("billed_amount"),
        dq.in_set("claim_status", ["paid", "partially_paid", "denied", "pending"]),
    ]
    valid, bad = dq.split_valid_quarantine(typed, rules)
    dq.write_quarantine(bad, f"{config.QUARANTINE}/claim/")
    w = Window.partitionBy("claim_id").orderBy(F.col("_ingest_ts").desc_nulls_last())
    cdm = (valid.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
           .select("claim_id", "encounter_id", "hospital_id", "payer",
                   "billed_amount", "allowed_amount", "paid_amount",
                   F.col("denial_code"),
                   F.col("claim_status"),
                   (F.col("claim_status") == "denied").alias("is_denied"),
                   "submission_date"))
    cdm.write.format("delta").mode("overwrite").saveAsTable(claims_target)
    a["rows_written"] = cdm.count()

# COMMAND ----------
display(spark.table(claims_target).limit(20))
