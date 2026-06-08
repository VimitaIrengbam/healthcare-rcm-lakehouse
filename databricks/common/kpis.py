"""
Pure KPI transformations (Phase 6) — kept as importable functions so they are unit-testable
locally (tests/test_kpis.py) and reused by databricks/gold/02_gold_kpis.py.

Each function takes a fact DataFrame and returns a KPI DataFrame; no I/O, no globals.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def days_in_ar(fact_transaction: DataFrame) -> DataFrame:
    """Days in A/R = total A/R / average daily net charges, per hospital per month."""
    return (
        fact_transaction
        .withColumn("month", F.date_format("txn_date", "yyyy-MM"))
        .groupBy("hospital_id", "month")
        .agg(
            F.sum("outstanding_ar").alias("total_ar"),
            (F.sum("charge_amount") - F.sum("adjustment")).alias("net_charges"),
            F.countDistinct("txn_date").alias("days_with_activity"),
        )
        .withColumn("avg_daily_net_charges",
                    F.col("net_charges") / F.greatest(F.col("days_with_activity"), F.lit(1)))
        .withColumn("days_in_ar",
                    F.round(F.col("total_ar") /
                            F.when(F.col("avg_daily_net_charges") == 0, F.lit(None))
                             .otherwise(F.col("avg_daily_net_charges")), 1))
    )


def net_collection_rate(fact_transaction: DataFrame) -> DataFrame:
    """NCR = payments / (charges - contractual adjustments), per hospital per month."""
    return (
        fact_transaction
        .withColumn("month", F.date_format("txn_date", "yyyy-MM"))
        .groupBy("hospital_id", "month")
        .agg(F.sum("paid_amount").alias("payments"),
             F.sum("charge_amount").alias("charges"),
             F.sum("adjustment").alias("adjustments"))
        .withColumn("net_collection_rate",
                    F.round(F.col("payments") /
                            F.when((F.col("charges") - F.col("adjustments")) == 0, F.lit(None))
                             .otherwise(F.col("charges") - F.col("adjustments")), 4))
    )


def denial_rate_by_dept(fact_claim: DataFrame) -> DataFrame:
    """Denial Rate = denied claims / total claims, grouped by hospital + department."""
    return (
        fact_claim
        .groupBy("hospital_id", "dept_id")
        .agg(F.count("*").alias("total_claims"),
             F.sum(F.col("is_denied").cast("int")).alias("denied_claims"))
        .withColumn("denial_rate",
                    F.round(F.col("denied_claims") / F.greatest(F.col("total_claims"), F.lit(1)), 4))
    )
