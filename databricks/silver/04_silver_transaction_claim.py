# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Transactions & Claims (CDM)  (Phase 4 — Resume bullet 4)
# MAGIC Conforms EMR transactions (bronze parquet from ADF) and insurance claims (bronze Delta from
# MAGIC Auto Loader) into clean `silver.transaction` and `silver.claim` tables feeding the gold KPIs.

# COMMAND ----------
from itertools import chain

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from common import audit, config, dq

# ── Late-arrival policy knobs ────────────────────────────────────────────────
# Per-payer timely-filing limits (days from DATE OF SERVICE). Medicare = 12 months is a
# real CMS limit; commercial payers vary (~90-120). Move to a reference table in prod.
TIMELY_FILING_DAYS = {"medicare": 365, "medicaid": 180, "aetna": 120,
                      "cigna": 120, "unitedhealthcare": 90}
TIMELY_FILING_DEFAULT = 90
LATE_FLAG_DAYS = 7  # arrival lag (ingest - submission) beyond this is flagged (observability)

# COMMAND ----------
# MAGIC %md ## Transactions (from EMR via ADF)
# COMMAND ----------
txn_bronze = f"{config.BRONZE}/emr/transactions/"
txn_target = config.fqn(config.SCHEMA_SILVER, "transaction")

with audit.log_load(spark, "silver_transaction", txn_bronze, txn_target, "batch") as a:
    raw = spark.read.option("recursiveFileLookup", "true").parquet(txn_bronze)
    a["rows_read"] = raw.count()
    rules = [
        dq.not_null("txn_id"), dq.not_null("hospital_id"),
        dq.not_null("patient_id"),  # a financial transaction must know its patient
        dq.non_negative("amount"), dq.non_negative("paid_amount"),
    ]
    valid, bad = dq.split_valid_quarantine(raw, rules)
    dq.write_quarantine(bad, f"{config.QUARANTINE}/transaction/")
    w = Window.partitionBy("txn_id", "hospital_id").orderBy(F.col("modified_at").desc_nulls_last())
    cdm = (valid.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
           .select("txn_id", "hospital_id", "encounter_id", "patient_id",
                   F.col("amount").alias("charge_amount"),
                   "paid_amount",
                   F.coalesce("adjustment", F.lit(0)).alias("adjustment"),
                   "amount_type", "payer", "status",
                   "visit_date", "service_date", "txn_date"))
    cdm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(txn_target)
    a["rows_written"] = cdm.count()

# COMMAND ----------
# MAGIC %md ## Claims (from insurance via Auto Loader)
# MAGIC Incremental load keyed off the Auto Loader `_ingest_ts` **watermark** (stored in the audit
# MAGIC framework), deduped on the **primary key** `claim_id`, and upserted via Delta **MERGE** so
# MAGIC claim status changes / late corrections update in place. Plus three late-arrival controls:
# MAGIC   1. measure lateness (filing lag from date-of-service; arrival lag from submission),
# MAGIC   2. quarantine claims past per-payer timely-filing limits,
# MAGIC   3. record reporting periods restated by late claims (for targeted KPI recompute).
# COMMAND ----------
claims_bronze = config.fqn(config.SCHEMA_BRONZE, "claims")
claims_target = config.fqn(config.SCHEMA_SILVER, "claim")
encounter_silver = config.fqn(config.SCHEMA_SILVER, "encounter")
restated_target = config.fqn(config.SCHEMA_AUDIT, "claim_restated_periods")

# Let the MERGE absorb the two new late-arrival columns on existing silver.claim tables.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# WATERMARK: max `_ingest_ts` already loaded successfully (reuses the audit framework's
# watermark_value column). ensure_audit_table so the read works on the very first run.
audit.ensure_audit_table(spark)
last_wm = spark.sql(f"""
    SELECT COALESCE(MAX(to_timestamp(watermark_value)), TIMESTAMP'1900-01-01')
    FROM {audit.AUDIT_TABLE}
    WHERE pipeline_name = 'silver_claim' AND status = 'success'
      AND watermark_value IS NOT NULL
""").first()[0]

bronze = spark.table(claims_bronze)
current_max = bronze.agg(F.max("_ingest_ts")).first()[0]  # freeze upper bound (None if empty)

with audit.log_load(spark, "silver_claim", claims_bronze, claims_target, "incremental") as a:
    if current_max is None:
        a["rows_read"] = 0
        a["rows_written"] = 0
    else:
        a["watermark_value"] = current_max.isoformat()

        # ── WATERMARK window: only claims ingested since the last successful load ──
        incremental = bronze.filter(
            (F.col("_ingest_ts") > F.lit(last_wm)) &
            (F.col("_ingest_ts") <= F.lit(current_max)))
        a["rows_read"] = incremental.count()

        # cast string columns landed by Auto Loader
        typed = (incremental
                 .withColumn("billed_amount", F.col("billed_amount").cast("double"))
                 .withColumn("allowed_amount", F.col("allowed_amount").cast("double"))
                 .withColumn("paid_amount", F.col("paid_amount").cast("double"))
                 .withColumn("submission_date", F.to_date("submission_date")))

        # ── Late-arrival #1: measure lateness ──
        # filing lag is anchored on DATE OF SERVICE (from encounter) vs per-payer limit;
        # arrival lag (ingest - submission) is a separate observability flag.
        svc = (spark.table(encounter_silver)
               .select("encounter_id", "hospital_id",
                       F.to_date("start_time").alias("service_date")))
        filing_map = F.create_map([F.lit(x) for x in chain(*TIMELY_FILING_DAYS.items())])
        scored = (typed.join(svc, on=["encounter_id", "hospital_id"], how="left")
                  .withColumn("service_anchor", F.coalesce("service_date", "submission_date"))
                  .withColumn("timely_filing_limit",
                              F.coalesce(filing_map[F.lower("payer")], F.lit(TIMELY_FILING_DEFAULT)))
                  .withColumn("filing_lag_days", F.datediff("submission_date", "service_anchor"))
                  .withColumn("arrival_lag_days",
                              F.datediff(F.to_date("_ingest_ts"), F.col("submission_date")))
                  .withColumn("is_late_arrival", F.col("arrival_lag_days") > LATE_FLAG_DAYS)
                  .withColumn("is_too_late", F.col("filing_lag_days") > F.col("timely_filing_limit")))

        # ── Late-arrival #2: quarantine claims past timely filing (don't silently absorb) ──
        too_late = scored.filter("is_too_late = true")
        dq.write_quarantine(
            too_late.withColumn("dq_failed_rules", F.array(F.lit("late_arriving_timely_filing"))),
            f"{config.QUARANTINE}/claim_late/")
        on_time = scored.filter("is_too_late = false")

        # standard DQ rules + quarantine
        rules = [
            dq.not_null("claim_id"), dq.not_null("encounter_id"),
            dq.non_negative("billed_amount"),
            dq.in_set("claim_status", ["paid", "partially_paid", "denied", "pending"]),
        ]
        valid, bad = dq.split_valid_quarantine(on_time, rules)
        dq.write_quarantine(bad, f"{config.QUARANTINE}/claim/")

        # dedup on PRIMARY KEY (claim_id); latest _ingest_ts wins
        w = Window.partitionBy("claim_id").orderBy(F.col("_ingest_ts").desc_nulls_last())
        cdm = (valid.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
               .select("claim_id", "encounter_id", "hospital_id", "payer",
                       "billed_amount", "allowed_amount", "paid_amount",
                       F.col("denial_code"),
                       F.col("claim_status"),
                       (F.col("claim_status") == "denied").alias("is_denied"),
                       "submission_date",
                       "arrival_lag_days", "is_late_arrival"))

        # ── PRIMARY KEY MERGE (upsert): status changes / late corrections update in place ──
        if not spark.catalog.tableExists(claims_target):
            cdm.write.format("delta").saveAsTable(claims_target)
        else:
            (DeltaTable.forName(spark, claims_target).alias("t")
             .merge(cdm.alias("s"), "t.claim_id = s.claim_id")
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        a["rows_written"] = cdm.count()

        # ── Late-arrival #3: record reporting periods restated by late claims ──
        restated = (cdm.filter("is_late_arrival = true")
                    .select(F.date_format("submission_date", "yyyy-MM").alias("affected_month"),
                            "hospital_id")
                    .distinct()
                    .withColumn("detected_at", F.current_timestamp()))
        if restated.head(1):
            restated.write.format("delta").mode("append").saveAsTable(restated_target)

# COMMAND ----------
display(spark.table(claims_target).orderBy(F.desc("arrival_lag_days")).limit(20))
