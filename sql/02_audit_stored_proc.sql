-- =============================================================================
-- ADF-side audit logging (Phase 3)
-- ADF Copy activities call dbo.sp_audit_log at start/end of each table load.
-- (Databricks loads log to the Delta audit table via common/audit.py — both feed
--  the same observability story; this one captures the ADF bronze ingestion.)
-- =============================================================================

IF OBJECT_ID('dbo.audit_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.audit_log (
        audit_id        BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_name   NVARCHAR(200),
        source          NVARCHAR(300),
        target          NVARCHAR(300),
        load_type       NVARCHAR(50),
        rows_written    BIGINT NULL,
        status          NVARCHAR(50),
        logged_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

CREATE OR ALTER PROCEDURE dbo.sp_audit_log
    @pipeline_name NVARCHAR(200),
    @source        NVARCHAR(300),
    @target        NVARCHAR(300),
    @load_type     NVARCHAR(50),
    @status        NVARCHAR(50),
    @rows_written  BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.audit_log (pipeline_name, source, target, load_type, rows_written, status)
    VALUES (@pipeline_name, @source, @target, @load_type, @rows_written, @status);
END
GO
