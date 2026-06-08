"""Unit tests for KPI math (Phase 8) — validates the formulas on tiny DataFrames."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "databricks"))

from common import kpis  # noqa: E402


def test_denial_rate_by_dept(spark):
    fact_claim = spark.createDataFrame(
        [
            ("hospital_a", "d1", True),
            ("hospital_a", "d1", False),
            ("hospital_a", "d1", False),
            ("hospital_a", "d1", False),   # d1: 1/4 = 0.25
            ("hospital_a", "d2", True),
            ("hospital_a", "d2", True),    # d2: 2/2 = 1.0
        ],
        ["hospital_id", "dept_id", "is_denied"],
    )
    res = {r.dept_id: r.denial_rate for r in kpis.denial_rate_by_dept(fact_claim).collect()}
    assert res["d1"] == 0.25
    assert res["d2"] == 1.0


def test_net_collection_rate(spark):
    fact_txn = spark.createDataFrame(
        [
            # hospital_a, 2025-01: payments 80, charges 100, adjustments 20 -> 80/(100-20)=1.0
            ("hospital_a", 80.0, 100.0, 20.0, "2025-01-15"),
        ],
        ["hospital_id", "paid_amount", "charge_amount", "adjustment", "txn_date"],
    )
    from pyspark.sql import functions as F
    fact_txn = fact_txn.withColumn("txn_date", F.to_date("txn_date"))
    row = kpis.net_collection_rate(fact_txn).collect()[0]
    assert row.net_collection_rate == 1.0


def test_days_in_ar(spark):
    from pyspark.sql import functions as F
    fact_txn = spark.createDataFrame(
        [
            # one month, two distinct days; total AR=200, net charges=400 -> avg daily=200 -> 200/200=1.0
            ("hospital_a", 100.0, 200.0, 0.0, 100.0, "2025-01-10"),
            ("hospital_a", 100.0, 200.0, 0.0, 100.0, "2025-01-20"),
        ],
        ["hospital_id", "outstanding_ar", "charge_amount", "adjustment", "paid_amount", "txn_date"],
    ).withColumn("txn_date", F.to_date("txn_date"))
    row = kpis.days_in_ar(fact_txn).collect()[0]
    # total_ar=200, net_charges=400, days=2 -> avg_daily=200 -> days_in_ar=1.0
    assert row.days_in_ar == 1.0
