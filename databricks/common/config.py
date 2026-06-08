"""
Shared configuration for Databricks notebooks/jobs.

Centralizes catalog/schema names and ADLS paths so notebooks stay environment-agnostic.
Override STORAGE_ACCOUNT via a Databricks job/cluster env var or widget if needed.
"""
from __future__ import annotations

import os

CATALOG = os.environ.get("RCM_CATALOG", "rcm")
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_AUDIT = "audit"

STORAGE_ACCOUNT = os.environ.get("RCM_STORAGE_ACCOUNT", "strcmdemo70648c")

# abfss container roots
def _abfss(container: str) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

LANDING = _abfss("landing")
BRONZE = _abfss("bronze")
SILVER = _abfss("silver")
GOLD = _abfss("gold")
QUARANTINE = _abfss("quarantine")
CHECKPOINTS = _abfss("checkpoints")

# common landing/bronze paths
LANDING_CLAIMS = f"{LANDING}/claims/"
LANDING_TELEMETRY = f"{LANDING}/telemetry/"
BRONZE_CLAIMS = f"{BRONZE}/claims/"
BRONZE_REF_NPI = f"{BRONZE}/reference/npi/"
BRONZE_REF_ICD = f"{BRONZE}/reference/icd/"


def fqn(schema: str, table: str) -> str:
    """Fully-qualified Unity Catalog table name."""
    return f"{CATALOG}.{schema}.{table}"
