"""Unit tests for PII masking helpers (Phase 8)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "databricks"))

from common import masking  # noqa: E402


def test_redact_ssn_keeps_last_four(spark):
    df = spark.createDataFrame([("123-45-6789",), (None,)], ["ssn"])
    out = {r.ssn for r in masking.redact_ssn(df).collect()}
    assert "XXX-XX-6789" in out
    assert None in out


def test_hash_col_is_deterministic_and_irreversible(spark):
    df = spark.createDataFrame([("123-45-6789",)], ["ssn"])
    h1 = masking.hash_col(df, "ssn").collect()[0].ssn
    h2 = masking.hash_col(df, "ssn").collect()[0].ssn
    assert h1 == h2
    assert h1 != "123-45-6789"
    assert len(h1) == 64  # sha-256 hex


def test_apply_default_pii_generalizes_dob(spark):
    df = spark.createDataFrame([("1980-05-01", "1 Main St", "123-45-6789")],
                               ["dob", "address", "ssn"])
    row = masking.apply_default_pii(df).collect()[0]
    assert row.dob == "1980"
    assert row.address == "[REDACTED]"
    assert row.ssn == "XXX-XX-6789"
