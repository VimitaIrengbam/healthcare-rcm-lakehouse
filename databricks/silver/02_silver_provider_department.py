# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Provider & Department dimensions  (Phase 4)
# MAGIC Conforms providers (enriched with NPI reference) and departments into SCD2 dimensions.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import audit, config, dq, scd

# COMMAND ----------
# MAGIC %md ## Department dimension
# COMMAND ----------
dept_bronze = f"{config.BRONZE}/emr/department/"
dept_target = config.fqn(config.SCHEMA_SILVER, "department")

with audit.log_load(spark, "silver_department", dept_bronze, dept_target, "scd2") as a:
    raw = spark.read.parquet(dept_bronze)
    a["rows_read"] = raw.count()
    valid, bad = dq.split_valid_quarantine(raw, [dq.not_null("dept_id"), dq.not_null("hospital_id")])
    dq.write_quarantine(bad, f"{config.QUARANTINE}/department/")
    w = Window.partitionBy("dept_id", "hospital_id").orderBy(F.col("modified_at").desc_nulls_last())
    deduped = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
    cdm = deduped.select("dept_id", "hospital_id", "name")
    scd.scd2_merge(spark, dept_target, cdm, ["dept_id", "hospital_id"], ["name"])
    a["rows_written"] = spark.table(dept_target).filter("is_current = true").count()

# COMMAND ----------
# MAGIC %md ## Provider dimension (enriched with NPI reference)
# COMMAND ----------
prov_bronze = f"{config.BRONZE}/emr/providers/"
prov_target = config.fqn(config.SCHEMA_SILVER, "provider")
npi_ref = config.fqn(config.SCHEMA_BRONZE, "ref_npi")

with audit.log_load(spark, "silver_provider", prov_bronze, prov_target, "scd2") as a:
    raw = spark.read.parquet(prov_bronze)
    a["rows_read"] = raw.count()
    valid, bad = dq.split_valid_quarantine(raw, [dq.not_null("provider_id"), dq.not_null("hospital_id")])
    dq.write_quarantine(bad, f"{config.QUARANTINE}/provider/")
    w = Window.partitionBy("provider_id", "hospital_id").orderBy(F.col("modified_at").desc_nulls_last())
    deduped = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")

    # enrich with NPI registry specialty (left join; keeps providers without a match)
    ref = spark.table(npi_ref).select(F.col("npi"), F.col("specialty").alias("npi_specialty"))
    enriched = deduped.join(ref, on="npi", how="left")

    cdm = enriched.select(
        "provider_id", "hospital_id", "npi", "name",
        F.coalesce("specialty", "npi_specialty").alias("specialty"), "dept_id",
    )
    scd.scd2_merge(spark, prov_target, cdm, ["provider_id", "hospital_id"],
                   ["npi", "name", "specialty", "dept_id"])
    a["rows_written"] = spark.table(prov_target).filter("is_current = true").count()

# COMMAND ----------
display(spark.table(prov_target).limit(20))
