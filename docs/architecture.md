# Architecture Flow — Healthcare RCM Lakehouse

End-to-end data flow from sources to KPIs. The diagram below renders automatically on GitHub
(Mermaid). An ASCII version follows for any viewer that doesn't render Mermaid.

## Diagram (Mermaid)

```mermaid
flowchart LR
    %% ---------------- SOURCES ----------------
    subgraph SRC["1 - Sources"]
        direction TB
        SQL[("Azure SQL - EMR<br/>2 hospitals<br/>Patients/Providers/Dept/<br/>Encounter/Transactions")]
        CLM["Claims flat files<br/>monthly CSV"]
        API["NPI and ICD<br/>public APIs"]
        TEL["Vitals telemetry<br/>simulated stream"]
    end

    %% ---------------- INGESTION / ORCHESTRATION ----------------
    subgraph ADF["2 - Azure Data Factory (metadata-driven)"]
        direction TB
        CFG["configs.csv<br/>database, datasource, table,<br/>loadtype, watermark, target"]
        PIPE["Lookup -> Filter active -> ForEach<br/>Get Metadata, Archive, Copy"]
        AUD[("audit_log<br/>watermark + lineage")]
        CFG --> PIPE
        PIPE --> AUD
    end

    %% ---------------- STORAGE (MEDALLION) ----------------
    subgraph LAKE["3 - ADLS Gen2 (medallion containers)"]
        direction TB
        LAND["landing<br/>raw files"]
        BRZ["bronze - Parquet<br/>per hospital"]
        SLV["silver - Delta<br/>clean / CDM / SCD2"]
        GLD["gold - Delta<br/>star schema + KPIs"]
        QUAR["quarantine<br/>bad rows"]
        ARC["archive<br/>yyyy/MM/dd backups"]
    end

    %% ---------------- DATABRICKS COMPUTE ----------------
    subgraph DBX["4 - Azure Databricks (Spark / Delta)"]
        direction TB
        AL["Auto Loader<br/>incremental + checkpoints"]
        STR["Structured Streaming<br/>event-time watermark"]
        SILVERNB["Silver transforms<br/>DQ quarantine, dedup,<br/>PII masking, CDM,<br/>SCD2, surrogate key"]
        GOLDNB["Gold<br/>star schema + KPI logic"]
    end

    %% ---------------- GOVERNANCE ----------------
    subgraph GOV["Governance and Security"]
        direction TB
        UC["Unity Catalog<br/>catalog/schemas, masking, lineage"]
        KV["Key Vault<br/>secrets"]
    end

    %% ---------------- CONSUMERS ----------------
    BI["Data Analysts / BI<br/>Days in A/R, Net Collection Rate,<br/>Denial Rate by department"]

    %% ---------------- FLOWS ----------------
    SQL -->|ADF copy| PIPE
    PIPE -->|"full: replace / incr: append"| BRZ
    PIPE -->|backup existing| ARC
    CFG -. read from .-> LAND

    CLM --> LAND
    TEL --> LAND
    LAND --> AL --> BRZ
    LAND --> STR --> SLV
    API --> SILVERNB

    BRZ --> SILVERNB --> SLV
    SILVERNB -->|failed rows| QUAR
    SLV --> GOLDNB --> GLD
    AUD -. last watermark .-> PIPE

    GLD --> BI

    %% ---------------- CROSS-CUTTING ----------------
    KV -. secrets .-> ADF
    KV -. secrets .-> DBX
    UC -. governs .-> SLV
    UC -. governs .-> GLD
    DBX -. read/write .-> LAKE

    classDef src fill:#e8f0fe,stroke:#4285f4,color:#111;
    classDef adf fill:#fef7e0,stroke:#f9ab00,color:#111;
    classDef lake fill:#e6f4ea,stroke:#34a853,color:#111;
    classDef dbx fill:#fce8e6,stroke:#ea4335,color:#111;
    classDef gov fill:#f3e8fd,stroke:#a142f4,color:#111;
    classDef out fill:#fff3cd,stroke:#d39e00,color:#111;
    class SQL,CLM,API,TEL src;
    class CFG,PIPE,AUD adf;
    class LAND,BRZ,SLV,GLD,QUAR,ARC lake;
    class AL,STR,SILVERNB,GOLDNB dbx;
    class UC,KV gov;
    class BI out;
```

## Flow in words

1. **Sources** → Azure SQL (EMR, 2 hospitals), monthly **claims** files, **NPI/ICD** APIs, vitals telemetry.
2. **ADF** reads `configs.csv` and, per hospital/table: checks the bronze folder, **archives** existing
   data (date-partitioned), then **full-replaces or incrementally appends** to **bronze** — recording each
   load (and the watermark) in **audit_log**.
3. **Databricks** brings claims in via **Auto Loader** and reference data via API into bronze, then
   transforms bronze → **silver** (DQ quarantine, dedup, PII masking, common data model, SCD2, surrogate
   key) and silver → **gold** (star schema + KPIs). A streaming job handles telemetry with event-time
   watermarks.
4. **Governance** is cross-cutting: **Unity Catalog** governs silver/gold (schemas, column masking,
   lineage) and **Key Vault** holds all secrets.
5. **Consumers** (analysts/BI) read the **gold** KPIs: Days in A/R, Net Collection Rate, Denial Rate by
   department.

## ASCII fallback

```
 SOURCES                 INGESTION (ADF)             STORAGE (ADLS medallion)        COMPUTE (Databricks)        CONSUMERS
 -------                 ---------------             ------------------------        --------------------        ---------
 Azure SQL (EMR) ─copy─► configs.csv ─► Lookup/      landing ─► bronze (Parquet) ─► Silver transforms ─► silver
   2 hospitals            ForEach: GetMeta,            ▲           │ per hospital      DQ/dedup/mask/             (Delta)
 Claims files ──────────► Archive, Copy ──────────────┘           │                   CDM/SCD2/SK        │
 NPI/ICD APIs ─────────────────────────────────────────────────► bronze ◄─ AutoLoader/API               ▼
 Telemetry ─► landing ─► Structured Streaming ──────────────────► silver                       gold (Delta) ─► Analysts/BI
                          │                                        │   ▲                         star + KPIs       Days in A/R
                          ▼                                   quarantine│                                          NCR
                       audit_log ◄─ watermark ───────────────────────┘                                            Denial Rate
                       (lineage)                          archive (yyyy/MM/dd)

 Cross-cutting:  Key Vault (secrets) ──► ADF + Databricks      Unity Catalog (governance, masking, lineage) ──► silver + gold
```

## Legend
- **Cylinders/blue** = sources. **Yellow** = ADF ingestion/orchestration. **Green** = ADLS medallion
  containers. **Red** = Databricks compute. **Purple** = governance/security. **Gold** = consumers.
- Solid arrows = data movement. Dotted arrows = control/governance (secrets, watermark reads, access).
