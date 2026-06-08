"""
PII masking & redaction helpers (Phase 4 / governance).

Two complementary mechanisms:
  1. Column-level transforms applied at write time (hashing/redaction) — defense in depth.
  2. Unity Catalog column-mask functions (SQL, in masking_policies.sql) — enforced at query time
     based on the querying user's group.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def hash_col(df: DataFrame, col: str, salt: str = "rcm") -> DataFrame:
    """Irreversibly hash a PII column (e.g. ssn) with SHA-256 + salt."""
    return df.withColumn(col, F.sha2(F.concat(F.lit(salt), F.col(col).cast("string")), 256))


def redact_ssn(df: DataFrame, col: str = "ssn") -> DataFrame:
    """Keep only last 4: 123-45-6789 -> XXX-XX-6789."""
    return df.withColumn(
        col,
        F.when(F.col(col).isNull(), None).otherwise(
            F.concat(F.lit("XXX-XX-"), F.substring(F.regexp_replace(F.col(col), "[^0-9]", ""), -4, 4))
        ),
    )


def redact_name(df: DataFrame, col: str) -> DataFrame:
    """Keep first initial only: 'Robert' -> 'R.'"""
    return df.withColumn(
        col,
        F.when(F.col(col).isNull(), None).otherwise(F.concat(F.substring(F.col(col), 1, 1), F.lit("."))),
    )


def redact_address(df: DataFrame, col: str = "address") -> DataFrame:
    return df.withColumn(col, F.when(F.col(col).isNull(), None).otherwise(F.lit("[REDACTED]")))


def generalize_dob(df: DataFrame, col: str = "dob") -> DataFrame:
    """Reduce DOB to birth year only (date generalization)."""
    return df.withColumn(col, F.year(F.col(col)).cast("string"))


def apply_default_pii(df: DataFrame) -> DataFrame:
    """Apply the standard patient PII treatment used in silver."""
    out = df
    if "ssn" in df.columns:
        out = redact_ssn(out, "ssn")
    if "address" in df.columns:
        out = redact_address(out, "address")
    if "dob" in df.columns:
        out = generalize_dob(out, "dob")
    return out
