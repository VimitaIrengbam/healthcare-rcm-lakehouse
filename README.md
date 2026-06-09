# Healthcare RCM Lakehouse

An end-to-end **Revenue Cycle Management (RCM)** data engineering project on Azure, built with the
**medallion architecture** (`landing → bronze → silver → gold`) on Databricks + Delta Lake.

It tracks the hospital financial lifecycle (scheduling → care → billing → payment) and surfaces finance
KPIs: **Days in A/R**, **Net Collection Rate (NCR)**, and **Denial Rate by department**.

> Portfolio/demo project. All data is **synthetic** (Synthea + a telemetry simulator). **No real PHI.**

---

## Architecture

> 📊 Full rendered flow diagram: [`docs/architecture.md`](docs/architecture.md)

```
                 ┌─────────────── SOURCES ───────────────┐
 Azure SQL (EMR) ─┤ ADF metadata-driven Copy (config CSV) ├─┐
 Claims flat file ─┤ Databricks Auto Loader (incremental)  ├─┤
 NPI / ICD API   ─┤ Python API ingest                     ├─┤
 Vitals telemetry─┤ Spark Structured Streaming            ├─┤
                 └────────────────────────────────────────┘ │
                                                             ▼
   landing (flat) ──► bronze (parquet) ──► silver (Delta) ──► gold (Delta) ──► BI / KPIs
                                            │                  │
                                       quarantine          star schema
                                       PII masking         Days in A/R, NCR,
                                       SCD2 / CDM          Denial Rate
```

Every load is recorded in an **audit Delta table**; governance is enforced through **Unity Catalog**
(catalogs/schemas, column masks, access control).

---

## Repository layout

| Path | Purpose |
|---|---|
| `infra/` | Azure provisioning (`provision.ps1`) and teardown (`teardown.ps1`) |
| `config/` | `env.json` (resource names) + `ingestion_metadata.csv` (metadata-driven EMR ingest) |
| `data_generation/` | Synthea guide, claims generator, telemetry simulator, Azure SQL loader |
| `sql/` | Azure SQL DDL/seed for the EMR source system |
| `adf/` | Azure Data Factory pipeline / dataset / linked-service JSON |
| `databricks/bronze/` | Auto Loader (claims) + API (NPI/ICD) ingestion |
| `databricks/silver/` | Cleansing, quarantine, dedup, masking, CDM, SCD2 |
| `databricks/streaming/` | Structured Streaming telemetry job |
| `databricks/gold/` | KPI / star-schema notebooks |
| `databricks/common/` | Shared PySpark utils (audit, DQ, masking) |
| `tests/` | `pytest` unit tests (run locally, no Azure needed) |
| `.github/workflows/` | CI/CD |

---

## Quick start

1. **Provision** (Azure CLI logged in):
   ```powershell
   ./infra/provision.ps1
   ```
2. **Generate data:** follow `data_generation/run_synthea.md`, then:
   ```powershell
   python data_generation/generate_claims.py
   python data_generation/load_synthea_to_azuresql.py
   ```
3. **Run pipelines** (Databricks): import `databricks/` notebooks, run bronze → silver → gold.
4. **Stream telemetry:**
   ```powershell
   python data_generation/telemetry_simulator.py
   ```
   then run `databricks/streaming/telemetry_stream.py`.
5. **Tear down to stop spend:**
   ```powershell
   ./infra/teardown.ps1
   ```

See [`docs/PHASES.md`](docs/PHASES.md) for the full phase-by-phase plan.

---

## Cost guardrails (Azure trial: 30 days / $200)

- Databricks clusters use **strict 10-min idle auto-termination** (the platform minimum); single/small nodes.
- Azure SQL is **Serverless** with auto-pause at 60 min idle (Azure's minimum).
- Streaming defaults to a **file source** (no Event Hubs cost).
- Keep synthetic data small (hundreds–few thousand rows).
- **Run `teardown.ps1` after every work session**; a budget alert is set as a backstop.
