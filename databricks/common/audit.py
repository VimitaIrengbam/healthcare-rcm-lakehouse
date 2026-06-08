"""
Audit framework (Phase 3).

Records one row per pipeline load into the audit Delta table (rcm.audit.pipeline_log).
Databricks notebooks call log_load(); ADF can write equivalent rows via a Lookup/stored-proc.
"""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (LongType, StringType, StructField, StructType,
                               TimestampType)

from . import config

AUDIT_TABLE = config.fqn(config.SCHEMA_AUDIT, "pipeline_log")

# Explicit schema so rows with NULL fields (watermark_value, error_msg, etc.) don't break
# Spark type inference (CANNOT_DETERMINE_TYPE). Field order = column order used in _write_row.
AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("pipeline_name", StringType()),
    StructField("source", StringType()),
    StructField("target", StringType()),
    StructField("load_type", StringType()),
    StructField("rows_read", LongType()),
    StructField("rows_written", LongType()),
    StructField("watermark_value", StringType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("status", StringType()),
    StructField("error_msg", StringType()),
])
_AUDIT_COLS = [f.name for f in AUDIT_SCHEMA.fields]

DDL = f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    run_id          STRING,
    pipeline_name   STRING,
    source          STRING,
    target          STRING,
    load_type       STRING,
    rows_read       BIGINT,
    rows_written    BIGINT,
    watermark_value STRING,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    status          STRING,
    error_msg       STRING
) USING DELTA
"""


def ensure_audit_table(spark: SparkSession) -> None:
    spark.sql(DDL)


def _write_row(spark: SparkSession, row: dict) -> None:
    values = tuple(row.get(c) for c in _AUDIT_COLS)
    df = spark.createDataFrame([values], schema=AUDIT_SCHEMA)
    df.write.mode("append").saveAsTable(AUDIT_TABLE)


@contextmanager
def log_load(spark: SparkSession, pipeline_name: str, source: str, target: str,
             load_type: str = "batch", watermark_value: str | None = None):
    """
    Context manager that records start/end + status of a load.

    Usage:
        with audit.log_load(spark, "bronze_claims", "landing/claims", "bronze.claims") as a:
            ...
            a["rows_read"] = df.count()
            a["rows_written"] = out.count()
    """
    ensure_audit_table(spark)
    state = {
        "run_id": uuid.uuid4().hex,
        "pipeline_name": pipeline_name,
        "source": source,
        "target": target,
        "load_type": load_type,
        "rows_read": None,
        "rows_written": None,
        "watermark_value": watermark_value,
        "start_time": datetime.now(timezone.utc),
        "end_time": None,
        "status": "running",
        "error_msg": None,
    }
    try:
        yield state
        state["status"] = "success"
    except Exception as e:  # noqa: BLE001
        state["status"] = "failed"
        state["error_msg"] = str(e)[:2000]
        raise
    finally:
        state["end_time"] = datetime.now(timezone.utc)
        _write_row(spark, state)
