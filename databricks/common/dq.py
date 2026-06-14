"""
Data-quality helpers (Phase 4).

Splits a DataFrame into VALID and QUARANTINE rows based on declarative rules, so silver
notebooks stay readable. A "rule" is (name, boolean Column expression that must be TRUE to pass).
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass
class Rule:
    name: str
    expr: Column  # must evaluate TRUE for a row to be considered valid


def split_valid_quarantine(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    """
    Returns (valid_df, quarantine_df).
    quarantine_df gains a `dq_failed_rules` array column and `dq_quarantined_at` timestamp.
    """
    if not rules:
        return df, df.limit(0).withColumn("dq_failed_rules", F.array().cast("array<string>")) \
                              .withColumn("dq_quarantined_at", F.current_timestamp())

    # Build a column listing which rules failed for each row.
    # NOTE: use array_compact (not array_remove(arr, None)) — array_remove(arr, NULL)
    # returns NULL in Spark, which would null out every row's failed-rules array.
    failed = F.array_compact(
        F.array(*[F.when(~r.expr, F.lit(r.name)) for r in rules])
    )
    tagged = df.withColumn("dq_failed_rules", failed)

    valid = tagged.filter(F.size("dq_failed_rules") == 0).drop("dq_failed_rules")
    quarantine = (
        tagged.filter(F.size("dq_failed_rules") > 0)
        .withColumn("dq_quarantined_at", F.current_timestamp())
    )
    return valid, quarantine


def not_null(col: str) -> Rule:
    return Rule(f"{col}_not_null", F.col(col).isNotNull())


def positive(col: str) -> Rule:
    return Rule(f"{col}_positive", F.col(col) > 0)


def non_negative(col: str) -> Rule:
    return Rule(f"{col}_non_negative", F.col(col) >= 0)


def in_set(col: str, allowed: list[str]) -> Rule:
    return Rule(f"{col}_in_set", F.col(col).isin(allowed))


def matches(col: str, pattern: str, name: str | None = None) -> Rule:
    return Rule(name or f"{col}_format", F.col(col).rlike(pattern))


def write_quarantine(quarantine_df: DataFrame, path: str) -> int:
    """Append quarantined rows to a parquet location; returns count."""
    n = quarantine_df.count()
    if n:
        quarantine_df.write.mode("append").parquet(path)
    return n
