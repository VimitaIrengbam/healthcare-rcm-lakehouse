# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: targeted denial-rate recompute (late-arriving claims)
# MAGIC Reads `audit.claim_restated_periods` (written by `silver/04` when late claims land),
# MAGIC finds the affected hospitals, recomputes ONLY their `(hospital_id, dept_id)` rows from
# MAGIC `gold.fact_claim`, and MERGEs them into `gold.kpi_denial_rate_by_dept`. This keeps the KPI
# MAGIC correct after late claims **without a full rebuild** of all KPIs (`02_gold_kpis.py`).
# MAGIC
# MAGIC Scope note: `denial_rate_by_dept` is all-time per `(hospital, dept)` — it has no month
# MAGIC grain — so a late claim from any month shifts that hospital's per-dept rate. The recompute
# MAGIC is therefore scoped to the affected HOSPITAL; the month in `claim_restated_periods` drives
# MAGIC only the watermark (process restated periods detected since the last successful recompute).

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import audit, config, kpis

G = config.SCHEMA_GOLD
restated_table = config.fqn(config.SCHEMA_AUDIT, "claim_restated_periods")
fact_claim = config.fqn(G, "fact_claim")
kpi_target = config.fqn(G, "kpi_denial_rate_by_dept")

# COMMAND ----------
# Nothing to do until the dependencies exist (full gold build + at least one late claim).
if not (spark.catalog.tableExists(restated_table)
        and spark.catalog.tableExists(fact_claim)
        and spark.catalog.tableExists(kpi_target)):
    dbutils.notebook.exit("skip: restated/fact_claim/kpi table not present yet")

# COMMAND ----------
# WATERMARK: only restated periods detected since the last successful recompute.
audit.ensure_audit_table(spark)
last_wm = spark.sql(f"""
    SELECT COALESCE(MAX(to_timestamp(watermark_value)), TIMESTAMP'1900-01-01')
    FROM {audit.AUDIT_TABLE}
    WHERE pipeline_name = 'gold_kpi_denial_recompute' AND status = 'success'
      AND watermark_value IS NOT NULL
""").first()[0]

new_restated = spark.table(restated_table).filter(F.col("detected_at") > F.lit(last_wm))
current_max = new_restated.agg(F.max("detected_at")).first()[0]  # None if nothing new

# COMMAND ----------
with audit.log_load(spark, "gold_kpi_denial_recompute", restated_table, kpi_target,
                    "incremental") as a:
    if current_max is None:
        a["rows_read"] = 0
        a["rows_written"] = 0
    else:
        a["watermark_value"] = current_max.isoformat()

        # affected hospitals = those with a late claim detected since last recompute
        affected = new_restated.select("hospital_id").distinct()
        a["rows_read"] = affected.count()

        # recompute denial rate for ONLY the affected hospitals, from fact_claim
        fc = spark.table(fact_claim).join(affected, on="hospital_id", how="inner")
        dept = spark.table(config.fqn(G, "dim_department")).select(
            "dept_id", "hospital_id", F.col("name").alias("department_name"))
        recomputed = (kpis.denial_rate_by_dept(fc)
                      .join(dept, on=["dept_id", "hospital_id"], how="left"))

        # MERGE recomputed rows into the KPI table (null-safe on dept_id for unmatched depts)
        (DeltaTable.forName(spark, kpi_target).alias("t")
         .merge(recomputed.alias("s"),
                "t.hospital_id = s.hospital_id AND t.dept_id <=> s.dept_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        a["rows_written"] = recomputed.count()

# COMMAND ----------
display(spark.table(kpi_target).orderBy(F.desc("denial_rate")).limit(20))
