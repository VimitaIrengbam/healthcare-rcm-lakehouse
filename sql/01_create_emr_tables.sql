-- =============================================================================
-- EMR source system schema (Azure SQL Database)  -- Phase 1
-- Models multiple hospitals via the `hospital_id` / datasource column.
-- These tables are the SOURCE that ADF copies into bronze (Phase 2).
-- =============================================================================

IF OBJECT_ID('dbo.Transactions', 'U') IS NOT NULL DROP TABLE dbo.Transactions;
IF OBJECT_ID('dbo.Encounter', 'U')    IS NOT NULL DROP TABLE dbo.Encounter;
IF OBJECT_ID('dbo.Providers', 'U')    IS NOT NULL DROP TABLE dbo.Providers;
IF OBJECT_ID('dbo.Department', 'U')   IS NOT NULL DROP TABLE dbo.Department;
IF OBJECT_ID('dbo.Patients', 'U')     IS NOT NULL DROP TABLE dbo.Patients;
GO

CREATE TABLE dbo.Patients (
    patient_id     VARCHAR(50)   NOT NULL,
    hospital_id    VARCHAR(20)   NOT NULL,        -- datasource discriminator
    firstname      NVARCHAR(100) NULL,
    lastname       NVARCHAR(100) NULL,
    dob            DATE          NULL,
    gender         VARCHAR(10)   NULL,
    address        NVARCHAR(255) NULL,
    city           NVARCHAR(100) NULL,
    state          VARCHAR(10)   NULL,
    zip            VARCHAR(10)   NULL,
    ssn            VARCHAR(20)   NULL,            -- PII; masked in silver
    modified_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Patients PRIMARY KEY (patient_id, hospital_id)
);
GO

CREATE TABLE dbo.Department (
    dept_id        VARCHAR(50)   NOT NULL,
    hospital_id    VARCHAR(20)   NOT NULL,
    name           NVARCHAR(150) NULL,
    modified_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Department PRIMARY KEY (dept_id, hospital_id)
);
GO

CREATE TABLE dbo.Providers (
    provider_id    VARCHAR(50)   NOT NULL,
    hospital_id    VARCHAR(20)   NOT NULL,
    npi            VARCHAR(20)    NULL,           -- joined to NPI reference in silver
    name           NVARCHAR(150) NULL,
    specialty      NVARCHAR(150) NULL,
    dept_id        VARCHAR(50)   NULL,
    modified_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Providers PRIMARY KEY (provider_id, hospital_id)
);
GO

CREATE TABLE dbo.Encounter (
    encounter_id   VARCHAR(50)   NOT NULL,
    hospital_id    VARCHAR(20)   NOT NULL,
    patient_id     VARCHAR(50)   NULL,
    provider_id    VARCHAR(50)   NULL,
    dept_id        VARCHAR(50)   NULL,
    encounter_type NVARCHAR(100) NULL,            -- inpatient/outpatient/ER ...
    procedure_code VARCHAR(20)   NULL,            -- CPT/HCPCS
    icd_code       VARCHAR(20)   NULL,            -- ICD-10; joined to ICD ref in silver
    start_time     DATETIME2     NULL,            -- incremental watermark
    end_time       DATETIME2     NULL,
    modified_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Encounter PRIMARY KEY (encounter_id, hospital_id)
);
GO

CREATE TABLE dbo.Transactions (
    txn_id         VARCHAR(50)   NOT NULL,
    hospital_id    VARCHAR(20)   NOT NULL,
    encounter_id   VARCHAR(50)   NULL,
    patient_id     VARCHAR(50)   NULL,            -- WHO the charge/payment is for (the guarantor)
    amount         DECIMAL(12,2) NULL,            -- charged amount
    paid_amount    DECIMAL(12,2) NULL,
    adjustment     DECIMAL(12,2) NULL,            -- contractual adjustment (for NCR)
    amount_type    VARCHAR(20)   NULL,            -- Insurance / Co-pay / Self-pay
    payer          NVARCHAR(100) NULL,
    status         VARCHAR(30)   NULL,            -- billed/paid/denied/...
    visit_date     DATE          NULL,            -- date of the patient visit
    service_date   DATE          NULL,            -- date the billable service was rendered
    txn_date       DATE          NULL,            -- financial POST date (incremental watermark)
    modified_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Transactions PRIMARY KEY (txn_id, hospital_id)
);
GO

-- Helpful indexes for incremental watermark reads
CREATE INDEX IX_Encounter_start    ON dbo.Encounter(start_time);
CREATE INDEX IX_Transactions_date  ON dbo.Transactions(txn_date);
GO
