# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: RCM KPIs  (Phase 6 — Resume bullet 4)
# MAGIC Computes the finance KPIs that power AR monitoring, persisted as Delta tables/views:
# MAGIC   * `gold.kpi_days_in_ar`          — Days in Accounts Receivable (monthly, per hospital)
# MAGIC   * `gold.kpi_net_collection_rate` — Net Collection Rate (monthly, per hospital)
# MAGIC   * `gold.kpi_denial_rate_by_dept` — Denial Rate by department
# MAGIC
# MAGIC Formulas:
# MAGIC   Days in A/R       = Total A/R / (Net charges over period / days in period)
# MAGIC   Net Collection %  = Payments / (Charges - Contractual Adjustments)
# MAGIC   Denial Rate       = Denied claims / Total claims

# COMMAND ----------
from pyspark.sql import functions as F

from common import audit, config, kpis

G = config.SCHEMA_GOLD

# COMMAND ----------
# MAGIC %md ## Days in A/R (monthly per hospital)
# COMMAND ----------
with audit.log_load(spark, "kpi_days_in_ar", config.fqn(G, "fact_transaction"),
                    config.fqn(G, "kpi_days_in_ar"), "full"):
    txn = spark.table(config.fqn(G, "fact_transaction"))
    kpis.days_in_ar(txn).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "kpi_days_in_ar"))

# COMMAND ----------
# MAGIC %md ## Net Collection Rate (monthly per hospital)
# COMMAND ----------
with audit.log_load(spark, "kpi_ncr", config.fqn(G, "fact_transaction"),
                    config.fqn(G, "kpi_net_collection_rate"), "full"):
    txn = spark.table(config.fqn(G, "fact_transaction"))
    kpis.net_collection_rate(txn).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "kpi_net_collection_rate"))

# COMMAND ----------
# MAGIC %md ## Denial Rate by department
# COMMAND ----------
with audit.log_load(spark, "kpi_denial_rate", config.fqn(G, "fact_claim"),
                    config.fqn(G, "kpi_denial_rate_by_dept"), "full"):
    claim = spark.table(config.fqn(G, "fact_claim"))
    dept = spark.table(config.fqn(G, "dim_department")).select(
        "dept_id", "hospital_id", F.col("name").alias("department_name"))
    denial = kpis.denial_rate_by_dept(claim).join(dept, on=["dept_id", "hospital_id"], how="left")
    denial.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(config.fqn(G, "kpi_denial_rate_by_dept"))

# COMMAND ----------
display(spark.table(config.fqn(G, "kpi_denial_rate_by_dept")).orderBy(F.desc("denial_rate")))
