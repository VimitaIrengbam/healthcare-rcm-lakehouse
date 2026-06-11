"""
SCD Type 2 helper using Delta Lake MERGE (Phase 4).

Maintains history on dimension tables with effective_from / effective_to / is_current and a
surrogate key. Idempotent: re-running with unchanged source is a no-op.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def row_hash_expr(tracked_cols: list[str]) -> Column:
    """
    SCD2 change-detection fingerprint over the tracked attributes.

    xxhash64 (non-cryptographic) is used purely for change detection — far cheaper than
    sha2 and the collision risk at dimension scale is negligible. A collision would only
    ever cause a missed change, never corruption.

    Exposed as a helper so one-off migrations/backfills compute the EXACT same hash as
    scd2_merge — otherwise a backfilled row_hash wouldn't match and every member would be
    spuriously re-versioned on the next run.
    """
    return F.xxhash64(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]))


def scd2_merge(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
    business_keys: list[str],
    tracked_cols: list[str],
) -> None:
    """
    Upsert source_df into target_table as SCD2.

    Creates the table on first run. `business_keys` identify a dimension member;
    `tracked_cols` are the attributes whose change triggers a new version.
    """
    from delta.tables import DeltaTable

    src = (
        source_df
        .withColumn("row_hash", row_hash_expr(tracked_cols))
        .withColumn("effective_from", F.current_timestamp())
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if not spark.catalog.tableExists(target_table):
        (src.withColumn("dim_key", F.expr("uuid()"))
            .write.format("delta").saveAsTable(target_table))
        return

    tgt = DeltaTable.forName(spark, target_table)
    key_match = " AND ".join([f"t.{k} = s.{k}" for k in business_keys])

    # 1) Expire current rows whose tracked attributes changed
    (tgt.alias("t")
        .merge(src.alias("s"), f"{key_match} AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "effective_to": "current_timestamp()"},
        )
        .execute())

    # 2) Insert new versions (changed) and brand-new members
    new_versions = src.alias("s").join(
        tgt.toDF().filter("is_current = true").alias("t"),
        on=business_keys,
        how="left_anti",
    )
    (new_versions
        .withColumn("dim_key", F.expr("uuid()"))
        .write.format("delta").mode("append").saveAsTable(target_table))
