# Phase-by-phase plan & runbook

This mirrors the approved project plan and adds the concrete files + operational steps per phase.
The build is **config/metadata-driven** so future changes are additive (new config rows, new notebooks).

| Bullet | Capability | Where |
|---|---|---|
| 1 | Governed cloud pipelines (Databricks/PySpark, compliance) | Phase 0, 4, 7 |
| 2 | Auto Loader incremental batch ingestion | Phase 2 |
| 3 | Spark Structured Streaming (patient monitoring) | Phase 5 |
| 4 | Python/SQL cleanse–validate–transform | Phase 4, 6 |

---

## Phase 0 — Foundations & provisioning
- Files: `infra/provision.ps1`, `infra/teardown.ps1`, `config/env.json`.
- Run `./infra/provision.ps1` (creates RG, ADLS+containers, Azure SQL serverless, Databricks, ADF, Key Vault, budget).
- **Declarative alternative (Bicep):** `infra/main.bicep` (+ `infra/main.bicepparam`) declares the same core
  resources. Deploy into a fresh RG with a new `suffix`:
  `az deployment group create -g <rg> -f infra/main.bicep -p infra/main.bicepparam sqlAdminPassword='<pwd>'`.
  Imperative-only steps (provider registration, secret values, RBAC, budget) stay in `provision.ps1`.
- **Declarative alternative (Terraform):** `infra/terraform/` (`main.tf`, `variables.tf`, `outputs.tf`).
  `cd infra/terraform && terraform init && terraform apply -var="sql_admin_password=<pwd>"`.
- **Manual after provisioning:**
  1. Enable Unity Catalog; create catalog `rcm` + schemas `bronze/silver/gold/audit`.
  2. Create a Databricks **secret scope** `rcm-secrets` backed by Key Vault `kv-rcm-demo`.
  3. Add Key Vault secret `adls-account-key` (used by the ADF linked service).
  4. Upload `config/ingestion_metadata.csv` to the ADLS `metadata` container.

## Phase 1 — Synthetic data
- Files: `sql/01_create_emr_tables.sql`, `data_generation/*`.
- Run `sql/01_*` against Azure SQL, generate Synthea data (`run_synthea.md`), then
  `load_synthea_to_azuresql.py`, `generate_claims.py`, and (for Phase 5) `telemetry_simulator.py`.

## Phase 2 — Bronze ingestion
- 2a EMR via ADF metadata-driven pipeline: `adf/pipeline/pl_ingest_emr_metadata.json` (+ datasets/linked services).
  Driven by `config/ingestion_metadata.csv`
  (`database,datasource,tablename,loadtype,watermark,is_active,targetpath,watermark_value`).
  `watermark` = the incremental column name (e.g. `start_time`); `watermark_value` = last loaded value
  (empty ⇒ first/full load). **Add a table = add a CSV row** (and re-upload the CSV to the `metadata` container).
- 2b Claims via Auto Loader: `databricks/bronze/01_autoloader_claims.py`.
- 2c NPI/ICD via API: `databricks/bronze/02_api_reference_npi_icd.py`.

## Phase 3 — Audit
- ADF side: `sql/02_audit_stored_proc.sql` (`dbo.sp_audit_log`, called by the pipeline).
- Databricks side: `databricks/common/audit.py` → Delta table `rcm.audit.pipeline_log` (used by every notebook via `with audit.log_load(...)`).

## Silver/Gold storage in ADLS containers (Unity Catalog external locations)
So silver/gold Delta files physically live in the `silver`/`gold` ADLS containers (not UC default storage):
1. Create a **Databricks Access Connector** (`ac-rcm-demo`, system-assigned MI).
2. Grant it **Storage Blob Data Contributor** on the storage account.
3. UC **storage credential** (`rcm_storage_cred`) → the access connector.
4. UC **external locations** `rcm_silver`/`rcm_gold` over `abfss://silver|gold@<acct>.dfs.core.windows.net/`.
5. Repoint schemas: `CREATE SCHEMA dbw_rcm_demo.silver MANAGED LOCATION 'abfss://silver@.../'` (same for gold).
   Now `saveAsTable` managed tables store their Delta under those containers; analysts query by name
   (`silver.patient`, `gold.kpi_*`) and the files are in ADLS.

## Phase 4 — Silver (cleanse / govern / CDM / SCD2)
- Files: `databricks/silver/0{1..4}_*.py`, helpers `common/{dq,masking,scd}.py`, `silver/masking_policies.sql`.
- Quarantine (`dq.split_valid_quarantine`), dedup, PII redaction (`masking`), late-arriving handling
  (encounter), CDM conform, SCD2 (`scd.scd2_merge`) on patient/provider/department.
- Run `masking_policies.sql` once to attach Unity Catalog column masks.

## Phase 5 — Structured Streaming telemetry
- Files: `data_generation/telemetry_simulator.py`, `databricks/streaming/telemetry_stream.py`.
- Event-time watermark (5 min) + 1-min windowed aggregates + SpO2 anomaly flag → `silver.telemetry_vitals*`.

## Phase 6 — Gold KPIs
- Files: `databricks/gold/01_gold_star_schema.py`, `02_gold_kpis.py`, helper `common/kpis.py`.
- KPIs: `kpi_days_in_ar`, `kpi_net_collection_rate`, `kpi_denial_rate_by_dept`.
- **Add a KPI = add a function in `kpis.py` + a cell in `02_gold_kpis.py`.**

## Phase 7 — Orchestration & CI/CD
- Files: `databricks/databricks.yml` (Asset Bundle: batch job DAG + streaming job), `.github/workflows/ci.yml`.
- `databricks bundle deploy -t dev` deploys the chained job; CI runs ruff + pytest then deploys on main.
- See [DEPLOY.md](DEPLOY.md) for the full deploy + run sequence (CLI/auth setup, wheel build,
  the one-time SCD2 `row_hash` migration, and post-deploy verification).

## Phase 8 — Verification
- Local: `pip install -r requirements-dev.txt && pytest` (no Azure needed).
- End-to-end: see README Quick start + the verification checklist in the project plan.

---

## Secrets / config you must set (not committed)
| Secret (Key Vault `kv-rcm-demo`) | Used by |
|---|---|
| `sql-admin-password` | provision.ps1 (generated), ADF SQL linked service, loader |
| `adls-account-key` | ADF ADLS linked service |
| Databricks secret scope `rcm-secrets` | notebooks accessing ADLS/Key Vault |

Env vars for local scripts: `SQL_SERVER`, `SQL_DB`, `SQL_USER`, `SQL_PASSWORD`,
and for notebooks `RCM_CATALOG`, `RCM_STORAGE_ACCOUNT`.
