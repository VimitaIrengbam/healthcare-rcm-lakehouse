# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: NPI & ICD reference data via public API  (Phase 2c)
# MAGIC Fetches provider identifiers (NPI Registry API) and ICD-10-CM diagnosis codes (NLM Clinical Tables API),
# MAGIC normalizes the JSON, and writes parquet to `bronze/reference/{npi,icd}/`.
# MAGIC These enrich providers/encounters in silver (Phase 4).
# MAGIC
# MAGIC ICD source note: we use the **NLM Clinical Tables ICD-10-CM** API (US billing codes), not the
# MAGIC WHO `id.who.int` API (ICD-11 / international). ICD-10-CM is what US payers adjudicate against, so
# MAGIC it is the correct source for RCM claim/diagnosis joins. It is a public GET (no auth/OAuth2).

# COMMAND ----------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    StructField("icd_code", StringType()), StructField("icd_code_type", StringType()),
    StructField("description", StringType()), StructField("search_term", StringType()),
])

# Shared HTTP session with retry/backoff so transient network failures, gateway errors
# and throttling (429) don't fail the whole load. respect_retry_after_header honors the
# server's Retry-After on 429s. Used by both fetch_npi and fetch_icd.
def _http_session():
    retry = Retry(
        total=5,
        backoff_factor=1,  # waits 1s, 2s, 4s, 8s, 16s between attempts
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

SESSION = _http_session()

# COMMAND ----------
# MAGIC %md ## NPI Registry — https://npiregistry.cms.hhs.gov/api/
# MAGIC The API caps `limit` at 200 and `skip` at 1000, so a single state query can return at most
# MAGIC 1200 records. We paginate via `skip` up to that ceiling; for fuller coverage, narrow each query
# MAGIC (city/postal/taxonomy) so sub-queries stay under the cap.
# COMMAND ----------
def fetch_npi(states=("MA", "CA", "NY", "TX", "FL", "IL", "PA", "OH"), limit=200, max_skip=1000):
    rows = []
    for st in states:
        skip = 0
        while True:
            resp = SESSION.get(
                "https://npiregistry.cms.hhs.gov/api/",
                params={"version": "2.1", "state": st, "limit": limit,
                        "skip": skip, "enumeration_type": "NPI-1"},
                timeout=30,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            for r in results:
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
            # stop on a short/empty page or when the next skip would exceed the API ceiling
            if len(results) < limit or skip + limit > max_skip:
                break
            skip += limit
    return rows

with audit.log_load(spark, "bronze_ref_npi", "npiregistry_api",
                    config.fqn(config.SCHEMA_BRONZE, "ref_npi"), "full") as a:
    npi_rows = fetch_npi()
    npi_df = (spark.createDataFrame(npi_rows, schema=NPI_SCHEMA)
              .dropDuplicates(["npi"])  # same NPI can appear across paged sub-queries
              .withColumn("_ingest_ts", F.current_timestamp()))
    npi_df.write.mode("overwrite").parquet(config.BRONZE_REF_NPI)
    npi_df.write.mode("overwrite").saveAsTable(config.fqn(config.SCHEMA_BRONZE, "ref_npi"))
    a["rows_written"] = npi_df.count()

# COMMAND ----------
# MAGIC %md ## ICD-10-CM — https://clinicaltables.nlm.nih.gov/api/icd10cm/v3/search
# MAGIC `icd_code_type` is a constant for this endpoint (ICD-10-CM), so we stamp it as a literal.
# MAGIC `maxList` is raised toward the API max (500) for fuller per-term coverage. For the *complete*
# MAGIC ~70k ICD-10-CM set, prefer bulk-loading the official CMS annual release file over keyword search.
# COMMAND ----------
def fetch_icd(terms=("diabetes", "hypertension", "sepsis", "fracture", "pneumonia"), max_each=500):
    rows = []
    for term in terms:
        resp = SESSION.get(
            "https://clinicaltables.nlm.nih.gov/api/icd10cm/v3/search",
            # sf = fields searched; df = fields returned in the data array (data[3])
            params={"sf": "code,name", "df": "code,name", "terms": term, "maxList": max_each},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        for code, name in data[3]:
            rows.append({
                "icd_code": code,
                "icd_code_type": "ICD-10-CM",
                "description": name,
                "search_term": term,
            })
    return rows

with audit.log_load(spark, "bronze_ref_icd", "clinicaltables_api",
                    config.fqn(config.SCHEMA_BRONZE, "ref_icd"), "full") as a:
    icd_rows = fetch_icd()
    icd_df = (spark.createDataFrame(icd_rows, schema=ICD_SCHEMA)
              .dropDuplicates(["icd_code"])  # same code can match multiple search terms
              .withColumn("_ingest_ts", F.current_timestamp()))
    icd_df.write.mode("overwrite").parquet(config.BRONZE_REF_ICD)
    icd_df.write.mode("overwrite").saveAsTable(config.fqn(config.SCHEMA_BRONZE, "ref_icd"))
    a["rows_written"] = icd_df.count()

# COMMAND ----------
display(spark.table(config.fqn(config.SCHEMA_BRONZE, "ref_icd")).limit(20))
