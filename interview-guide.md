# Interview Guide — Healthcare RCM Lakehouse

How to present this project in a data-engineering interview: a 30-second pitch, the
step-by-step flow from ingestion to KPI, cross-cutting talking points, challenges,
and a mock Q&A with the most likely follow-up questions.

Repo: https://github.com/VimitaIrengbam/healthcare-rcm-lakehouse

---

## 1. The 30-second opening ("walk me through your project")

> "I built an end-to-end **Revenue Cycle Management (RCM) lakehouse on Azure** using the
> **medallion architecture**. It ingests a hospital group's financial and clinical data from
> three different source types, cleanses and standardizes it through bronze → silver → gold
> layers on **Databricks/PySpark with Delta Lake**, and produces finance KPIs — Days in A/R,
> Net Collection Rate, and Denial Rate by department. Ingestion is **metadata-driven in Azure
> Data Factory**, it's **governed through Unity Catalog**, version-controlled with CI/CD, and
> provisioned with Infrastructure-as-Code."

That hits every keyword: medallion, Delta, PySpark, ADF, metadata-driven, Unity Catalog,
governance, CI/CD, IaC.

---

## 2. Architecture in one line

`Sources → Landing → Bronze (Parquet) → Silver (Delta) → Gold (Delta) → KPIs`
— orchestrated by ADF + Databricks, governed by Unity Catalog, secrets in Key Vault.

📊 **Architecture flow diagram:** [`docs/architecture.md`](docs/architecture.md) (renders on GitHub).

---

## 3. Step-by-step: ingestion → KPI

**Step 1 — Sources (three ingestion patterns on purpose):**
- **EMR** (Patients, Providers, Department, Encounter, Transactions) in **Azure SQL** — system of
  record for **two hospitals**.
- **Claims** as monthly **flat files** from the insurer.
- **Reference data** (NPI provider IDs, ICD diagnosis codes) from **public APIs**.

**Step 2 — Bronze ingestion, metadata-driven (ADF):** one pipeline driven by `configs.csv`
(`database, datasource, tablename, loadtype, watermark, is_active, targetpath`).
**Lookup → Filter active → ForEach** copies SQL → bronze as **Parquet, partitioned per hospital**
(`bronze/emr/<table>/<hospital>/`). Onboarding a new table = **one CSV row**.

**Step 3 — Archive + incremental:**
- **Get Metadata** checks if the bronze folder has files; if so, **archive** to
  `archive/<table>/<hospital>/yyyy/MM/dd/`.
- **Full** → clear bronze + reload all (clean snapshot). **Incremental** → read **last watermark
  from the audit log**, copy only new rows (`watermark > last AND <= current max`), and **append**.

**Step 4 — Audit logging:** every load writes to `dbo.audit_log` (source, table, load type,
watermark used, rows, status) — this is how incremental remembers state and gives lineage.

**Step 5 — Claims + reference into bronze (Databricks):** **Auto Loader** incrementally ingests
claim files with checkpointing (no double-loads); a notebook pulls NPI/ICD from the APIs.

**Step 6 — Silver: clean, govern, conform (PySpark/Delta):**
- **DQ quarantine** — bad rows routed aside, not into silver.
- **Deduplication** — latest record per natural key.
- **PII masking** — names/SSN/address/DOB redacted/hashed; Unity Catalog column masks at query time.
- **Common Data Model** — both hospitals conformed into one schema, joined to NPI/ICD.
- **SCD Type 2** on dimensions via **Delta MERGE** (`effective_from/to`, `is_current`).
- **Surrogate key** — `patient_sk` (deterministic hash of natural key).

**Step 7 — Gold: star schema + KPIs:** conformed **facts** (claims, transactions) + **dimensions**
(patient, provider, department, date), then:
- **Days in A/R** = Total A/R ÷ avg daily net charges.
- **Net Collection Rate** = Payments ÷ (Charges − Adjustments).
- **Denial Rate by dept** = Denied ÷ Total claims.

**Step 8 — Orchestration:** a master ADF pipeline runs it **end-to-end in one trigger** —
EMR→bronze, then Databricks notebooks bronze→silver→gold via Notebook activities.

---

## 4. Cross-cutting talking points

- **Governance:** Unity Catalog (catalog/schemas, column masking, lineage), Key Vault for secrets,
  audit trail, quarantine.
- **Storage design:** silver/gold are **Delta tables physically in ADLS containers** (UC storage
  credential + external locations) — queryable by name *and* accessible as files, with time-travel.
- **DevOps:** Git + GitHub, **CI/CD** (pytest on DQ/masking/SCD2/KPI logic), **three IaC styles**
  (PowerShell+CLI, Bicep, Terraform).
- **Cost engineering:** serverless SQL auto-pause + strict 10-min cluster auto-termination.

---

## 5. Challenges solved (keep 2–3 ready)

1. **Incremental data-loss bug:** archive-before-load deleted bronze on *every* run; an incremental
   with no new rows wrote an empty file and wiped the table. **Fix:** gate delete to full loads;
   incremental appends.
2. **Silent Spark bug:** a DQ helper used `array_remove(arr, NULL)` which returns NULL in Spark, so
   every row was dropped while the job still "succeeded." **Fix:** `array_compact`.
3. **Cloud realities:** new subscription had unregistered providers, Key Vault defaulted to RBAC
   (blocked secret access), East US was capacity-blocked for SQL. **Fix:** auto-register providers,
   switch KV to access-policy model, deploy SQL in Central US; made provisioning idempotent.

---

## 6. Mock Q&A — likely follow-up questions

### Architecture & design

**Q1. Why a lakehouse / medallion instead of a traditional data warehouse?**
A lakehouse gives the cheap, scalable storage of a data lake plus the reliability of a warehouse
(ACID, schema, time-travel) via Delta Lake. Medallion layering isolates responsibilities — bronze
preserves the immutable raw copy, silver makes data trustworthy, gold serves business metrics — so
issues are easy to localize and everything is rebuildable from raw.

**Q2. Why is bronze Parquet but silver/gold Delta?**
Bronze is a raw landing copy — Parquet is fine and slightly cheaper. From silver on, I need ACID
MERGE (for SCD2 and upserts), schema enforcement/evolution, and time-travel for auditing changes —
that's Delta. Delta is just Parquet + a transaction log.

**Q3. Why metadata-driven ingestion instead of one pipeline per table?**
Scalability and maintainability. A single parameterized pipeline reads a config table and loops over
entries, so onboarding a new source is a config change (one row), not new code. It also centralizes
load logic (archive, watermark, audit) in one place.

**Q4. Walk me through your incremental loading / watermarking.**
Each incremental table has a watermark column (e.g., `start_time`, `txn_date`). Before loading I read
the last successful watermark from the audit log, read the current max from the source, then copy
`watermark > last AND <= currentMax` and append. After success I write the new watermark to the audit
log. Using "last successful" makes it resilient to failed runs.

**Q5. Full vs incremental — how do you decide, and how do you avoid duplicates/data loss?**
Small slowly-changing tables (Patients, Providers, Department) are full loads — archive then replace
for a clean snapshot. Large fast-growing tables (Encounter, Transactions) are incremental — append
only new rows. The key fix: the "delete bronze" step runs **only for full loads**; incremental never
deletes, so history is preserved.

### Spark / Delta internals

**Q6. What is SCD Type 2 and how did you implement it?**
SCD2 keeps history: when a tracked attribute changes you don't overwrite — you close the current row
(`effective_to`, `is_current=false`) and insert a new current version. I implemented it with a Delta
`MERGE`: match on the business key, when the row-hash differs expire the old version, then insert new
versions for changed/new members. Each version gets a surrogate `dim_key`.

**Q7. What's a surrogate key and why use one?**
A system-generated key that uniquely identifies a row independent of the source's natural/business
key. I used `xxhash64(patient_id || hospital_id)` for `patient_sk` — deterministic and stable. It
decouples the warehouse from messy/duplicate source IDs and gives fact tables a clean join key.

**Q8. How do you handle data quality?**
Declarative rules (not-null keys, valid code sets, non-negative amounts). Rows are split into valid vs
quarantine; failures go to a quarantine path with the failed-rule names recorded, so silver stays
clean and bad data is auditable rather than dropped silently.

**Q9. How does Auto Loader work and why use it for claims?**
Auto Loader (`cloudFiles`) incrementally discovers and ingests new files from a folder, tracking what
it has processed via a checkpoint, with schema inference/evolution. It's ideal for the monthly claim
drops — idempotent, no double-processing, scales to many files without listing the whole directory.

**Q10. How do you handle schema evolution / schema drift?**
Auto Loader handles new columns in source files. For Delta tables I enable schema auto-merge where
needed (e.g., when I added `patient_sk`). I also recreate tables when a structural change warrants a
clean rebuild, since everything is regenerable from bronze.

**Q11. How does Structured Streaming handle late/out-of-order data?**
With event-time **watermarks**: the stream tracks the max event time and accepts late events within a
threshold (e.g., 5 minutes), dropping anything older so windowed aggregations can finalize. I read
vitals, window by event-time, compute rolling stats, and flag anomalies.

### Governance, security, ops

**Q12. This is healthcare data — how did you handle compliance/PII?**
Defense in depth: write-time redaction/hashing of names, SSN, address, DOB in silver; Unity Catalog
column masks that reveal raw values only to an authorized group at query time; all secrets in Key
Vault; a full audit trail; and quarantine for bad data. No real PHI — all synthetic.

**Q13. What does Unity Catalog give you here?**
Centralized governance: catalogs/schemas, fine-grained access control, column masking, and lineage.
It also lets analysts query by table name while data physically lives in governed external locations
(my silver/gold ADLS containers via a storage credential + external locations).

**Q14. How did you make silver/gold data land in your own ADLS containers as Delta?**
By default managed tables go to UC's default storage. I created a Databricks **Access Connector**
(managed identity), granted it Storage Blob Data Contributor, created a UC **storage credential** and
**external locations** for the silver/gold containers, then set those schemas' **MANAGED LOCATION** to
the containers. Now managed-table Delta files live there and stay queryable by name.

**Q15. How is this orchestrated and scheduled?**
A master ADF pipeline chains an Execute-Pipeline (EMR→bronze) with sequential Databricks Notebook
activities (bronze→silver→gold). It can run on a schedule trigger; I left scheduling off by default to
avoid spending trial credits unattended.

**Q16. How did you control cost?**
Serverless Azure SQL with auto-pause (60-min, the Azure minimum), Databricks clusters with strict
10-min idle auto-termination (the platform minimum), single-node small clusters, file-source streaming
instead of Event Hubs, and small synthetic datasets. Compute — the main cost driver — shuts itself off.

**Q17. What does your CI/CD and testing look like?**
Code is in Git/GitHub. GitHub Actions runs lint + **pytest** on the pure logic (DQ rules, masking,
SCD2, KPI math) on every push — these run locally without Azure. On main it can deploy the Databricks
asset bundle. Infra is reproducible via PowerShell+CLI, Bicep, and Terraform.

**Q18. How would you scale or productionize this further?**
Switch synthetic sources to real connectors; move PII handling to tokenization/Unity Catalog row+
column policies; add Delta Live Tables or Databricks Workflows for managed orchestration with
retries/alerts; add data-quality monitoring (e.g., expectations) and freshness SLAs; partition/Z-order
large fact tables; and add environment promotion (dev/test/prod) through the IaC + CI/CD.

---

## 7. KPI cheat-sheet (be ready to define each)

- **Days in A/R** = Total Accounts Receivable ÷ (Net charges over period ÷ days). *How fast we
  collect; lower is better.*
- **Net Collection Rate** = Payments ÷ (Charges − Contractual Adjustments). *How much of what we're
  owed we actually collect; closer to 100% is better.*
- **Denial Rate by department** = Denied claims ÷ Total claims, by department. *Where claims fail;
  pinpoints process problems.*
