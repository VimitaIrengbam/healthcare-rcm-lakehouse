# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: NPI & ICD reference data via public API  (Phase 2c)
# MAGIC Fetches provider identifiers (NPI Registry API) and ICD-10 diagnosis codes (Clinical Tables API),
# MAGIC normalizes the JSON, and writes parquet to `bronze/reference/{npi,icd}/`.
# MAGIC These enrich providers/encounters in silver (Phase 4).

# COMMAND ----------
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from common import audit, config

# Explicit schemas — the public APIs return columns that can be entirely null in a small
# sample (e.g. credential), which breaks Spark's type inference (CANNOT_DETERMINE_TYPE).
NPI_SCHEMA = StructType([
    StructField("npi", StringType()), StructField("first_name", StringType()),
    StructField("last_name", StringType()), StructField("credential", StringType()),
    StructField("specialty", StringType()), StructField("state", StringType()),
])
ICD_SCHEMA = StructType([
    StructField("icd_code", StringType()), StructField("description", StringType()),
    StructField("search_term", StringType()),
])

# COMMAND ----------
# MAGIC %md ## NPI Registry — https://npiregistry.cms.hhs.gov/api/
# COMMAND ----------
def fetch_npi(states=("MA", "CA"), limit=200):
    rows = []
    for st in states:
        resp = requests.get(
            "https://npiregistry.cms.hhs.gov/api/",
            params={"version": "2.1", "state": st, "limit": limit, "enumeration_type": "NPI-1"},
            timeout=30,
        )
        resp.raise_for_status()
        for r in resp.json().get("results", []):
            basic = r.get("basic", {})
            taxonomies = r.get("taxonomies", [{}])
            rows.append({
                "npi": str(r.get("number")),
                "first_name": basic.get("first_name"),
                "last_name": basic.get("last_name"),
                "credential": basic.get("credential"),
                "specialty": taxonomies[0].get("desc") if taxonomies else None,
                "state": st,
            })
    return rows

with audit.log_load(spark, "bronze_ref_npi", "npiregistry_api",
                    config.fqn(config.SCHEMA_BRONZE, "ref_npi"), "full") as a:
    npi_rows = fetch_npi()
    npi_df = spark.createDataFrame(npi_rows, schema=NPI_SCHEMA).withColumn("_ingest_ts", F.current_timestamp())
    npi_df.write.mode("overwrite").parquet(config.BRONZE_REF_NPI)
    npi_df.write.mode("overwrite").saveAsTable(config.fqn(config.SCHEMA_BRONZE, "ref_npi"))
    a["rows_written"] = npi_df.count()

# COMMAND ----------
# MAGIC %md ## ICD-10 — https://clinicaltables.nlm.nih.gov/api/icd10cm/v3/search
# COMMAND ----------
def fetch_icd(terms=("diabetes", "hypertension", "sepsis", "fracture", "pneumonia"), max_each=200):
    rows = []
    for term in terms:
        resp = requests.get(
            "https://clinicaltables.nlm.nih.gov/api/icd10cm/v3/search",
            params={"sf": "code,name", "terms": term, "maxList": max_each},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        for code, name in data[3]:
            rows.append({"icd_code": code, "description": name, "search_term": term})
    return rows

with audit.log_load(spark, "bronze_ref_icd", "clinicaltables_api",
                    config.fqn(config.SCHEMA_BRONZE, "ref_icd"), "full") as a:
    icd_rows = fetch_icd()
    icd_df = (spark.createDataFrame(icd_rows, schema=ICD_SCHEMA)
              .dropDuplicates(["icd_code"])
              .withColumn("_ingest_ts", F.current_timestamp()))
    icd_df.write.mode("overwrite").parquet(config.BRONZE_REF_ICD)
    icd_df.write.mode("overwrite").saveAsTable(config.fqn(config.SCHEMA_BRONZE, "ref_icd"))
    a["rows_written"] = icd_df.count()

# COMMAND ----------
display(spark.table(config.fqn(config.SCHEMA_BRONZE, "ref_icd")).limit(20))
