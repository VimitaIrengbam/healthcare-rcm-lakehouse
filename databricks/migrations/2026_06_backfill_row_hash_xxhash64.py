# Databricks notebook source
# MAGIC %md
# MAGIC # One-off migration: backfill SCD2 `row_hash` to xxhash64
# MAGIC `scd.scd2_merge` switched its change-detection hash from `sha2(...,256)` to `xxhash64`.
# MAGIC Existing dimension rows still hold the old SHA-256 `row_hash`, so the **first** run after
# MAGIC the switch would see `t.row_hash <> s.row_hash` for every member and spuriously create a
# MAGIC new SCD2 version for each one (a "change" that never happened).
# MAGIC
# MAGIC This notebook recomputes `row_hash` in place using the SAME helper `scd2_merge` uses
# MAGIC (`scd.row_hash_expr`), so after running it the next pipeline run is a clean no-op for
# MAGIC unchanged members.
# MAGIC
# MAGIC **Run once**, before the next silver run. Idempotent — re-running just recomputes the
# MAGIC identical hash, so it is safe to run again.

# COMMAND ----------
from delta.tables import DeltaTable

from common import config, scd

# Each SCD2 dimension and its tracked_cols — MUST match the scd2_merge calls in the
# silver notebooks (01_silver_patient.py, 02_silver_provider_department.py).
DIMENSIONS = [
    (config.fqn(config.SCHEMA_SILVER, "patient"),
     ["firstname", "lastname", "birth_year", "gender", "city", "state", "zip"]),
    (config.fqn(config.SCHEMA_SILVER, "provider"),
     ["npi", "name", "specialty", "dept_id"]),
    (config.fqn(config.SCHEMA_SILVER, "department"),
     ["name"]),
]

# COMMAND ----------
for table, tracked_cols in DIMENSIONS:
    if not spark.catalog.tableExists(table):
        print(f"SKIP (table does not exist yet): {table}")
        continue
    # Recompute row_hash for ALL rows (current + historical) so the column stays internally
    # consistent. Only current rows are compared by scd2_merge, but backfilling everything is
    # cheap and avoids confusion when inspecting history.
    DeltaTable.forName(spark, table).update(set={"row_hash": scd.row_hash_expr(tracked_cols)})
    n = spark.table(table).count()
    print(f"OK backfilled row_hash on {table} ({n} rows)")

print("Done. The next silver run will be a no-op for unchanged members.")
