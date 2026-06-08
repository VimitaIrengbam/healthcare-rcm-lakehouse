"""Unit tests for the data-quality split (Phase 8)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "databricks"))

from common import dq  # noqa: E402


def test_split_valid_quarantine(spark):
    df = spark.createDataFrame(
        [
            ("p1", "M", 100.0),   # valid
            (None, "F", 50.0),    # null key -> quarantine
            ("p3", "X", 10.0),    # bad gender -> quarantine
            ("p4", "F", -5.0),    # negative -> quarantine
        ],
        ["patient_id", "gender", "amount"],
    )
    rules = [
        dq.not_null("patient_id"),
        dq.in_set("gender", ["M", "F"]),
        dq.non_negative("amount"),
    ]
    valid, bad = dq.split_valid_quarantine(df, rules)
    assert valid.count() == 1
    assert bad.count() == 3
    # quarantine rows record which rules failed
    failed = {r.patient_id: r.dq_failed_rules for r in bad.collect()}
    assert "gender_in_set" in failed["p3"]
    assert "amount_non_negative" in failed["p4"]


def test_no_rules_returns_all_valid(spark):
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    valid, bad = dq.split_valid_quarantine(df, [])
    assert valid.count() == 2
    assert bad.count() == 0
