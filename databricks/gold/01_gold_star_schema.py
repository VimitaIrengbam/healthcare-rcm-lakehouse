# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: RCM star schema  (Phase 6)
# MAGIC Builds conformed dimensions (from silver SCD2 current rows) and facts that the KPI notebook
# MAGIC consumes: `gold.dim_date`, `gold.dim_patient`, `gold.dim_provider`, `gold.dim_department`,
# MAGIC `gold.fact_claim`, `gold.fact_transaction`.

# COMMAND ----------
from pyspark.sql import functions as F

from common import audit, config

S = config.SCHEMA_SILVER
G = config.SCHEMA_GOLD

# COMMAND ----------
# MAGIC %md ## Date dimension
# COMMAND ----------
with audit.log_load(spark, "gold_dim_date", "generated", config.fqn(G, "dim_date"), "full"):
    dim_date = (spark.sql("SELECT explode(sequence(to_date('2023-01-01'), to_date('2026-12-31'), interval 1 day)) AS date"))
    dim_date = (dim_date
                .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
                .withColumn("year", F.year("date"))
                .withColumn("month", F.month("date"))
                .withColumn("day", F.dayofmonth("date"))
                .withColumn("quarter", F.quarter("date")))
    dim_date.write.format("delta").mode("overwrite").saveAsTable(config.fqn(G, "dim_date"))

# COMMAND ----------
# MAGIC %md ## Conformed dimensions (current SCD2 rows)
# COMMAND ----------
def current(table):
    return spark.table(config.fqn(S, table)).filter("is_current = true")

with audit.log_load(spark, "gold_dims", "silver", config.fqn(G, "dim_*"), "full"):
    current("patient").write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "dim_patient"))
    current("provider").write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "dim_provider"))
    current("department").write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "dim_department"))

# COMMAND ----------
# MAGIC %md ## Facts (joined to department via encounter for dept-level KPIs)
# COMMAND ----------
with audit.log_load(spark, "gold_facts", "silver", config.fqn(G, "fact_*"), "full"):
    enc = spark.table(config.fqn(S, "encounter")).select(
        "encounter_id", "hospital_id", "dept_id", "provider_id", "patient_id")

    # fact_claim enriched with department (denial-rate-by-department needs dept)
    claim = spark.table(config.fqn(S, "claim"))
    fact_claim = (claim.join(enc, on=["encounter_id", "hospital_id"], how="left")
                  .withColumn("submission_date_key", F.date_format("submission_date", "yyyyMMdd").cast("int")))
    fact_claim.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "fact_claim"))

    # fact_transaction (charges/payments/adjustments for A/R + NCR)
    txn = spark.table(config.fqn(S, "transaction"))
    fact_txn = (txn.join(enc.drop("patient_id", "provider_id"), on=["encounter_id", "hospital_id"], how="left")
                .withColumn("txn_date_key", F.date_format("txn_date", "yyyyMMdd").cast("int"))
                .withColumn("outstanding_ar", F.col("charge_amount") - F.col("paid_amount") - F.col("adjustment")))
    fact_txn.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "fact_transaction"))

# COMMAND ----------
display(spark.table(config.fqn(G, "fact_transaction")).limit(20))
