"""Unit test for SCD Type 2 merge (Phase 8)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "databricks"))

from common import scd  # noqa: E402


def test_scd2_tracks_history(spark):
    table = "default.dim_patient_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    v1 = spark.createDataFrame([("p1", "Smith")], ["patient_id", "lastname"])
    scd.scd2_merge(spark, table, v1, ["patient_id"], ["lastname"])
    cur = spark.table(table).filter("is_current = true")
    assert cur.count() == 1
    assert cur.collect()[0].lastname == "Smith"

    # Change the tracked attribute -> a new current version, old one expired
    v2 = spark.createDataFrame([("p1", "Jones")], ["patient_id", "lastname"])
    scd.scd2_merge(spark, table, v2, ["patient_id"], ["lastname"])

    all_rows = spark.table(table)
    assert all_rows.count() == 2                       # history preserved
    assert all_rows.filter("is_current = true").count() == 1
    assert all_rows.filter("is_current = true").collect()[0].lastname == "Jones"
    expired = all_rows.filter("is_current = false").collect()[0]
    assert expired.lastname == "Smith"
    assert expired.effective_to is not None

    # Re-running with no change is idempotent (still 2 rows)
    scd.scd2_merge(spark, table, v2, ["patient_id"], ["lastname"])
    assert spark.table(table).count() == 2

    spark.sql(f"DROP TABLE IF EXISTS {table}")
