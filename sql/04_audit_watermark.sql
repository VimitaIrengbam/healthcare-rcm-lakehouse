-- =============================================================================
-- Enhanced audit/watermark logging for the metadata-driven pipeline (v2).
-- Adds per-table/per-hospital watermark tracking to dbo.audit_log so INCREMENTAL
-- loads can read the last successful watermark and write the new one.
-- Idempotent: safe to re-run.
-- =============================================================================

IF COL_LENGTH('dbo.audit_log', 'datasource') IS NULL
    ALTER TABLE dbo.audit_log ADD datasource NVARCHAR(50) NULL;
GO
IF COL_LENGTH('dbo.audit_log', 'tablename') IS NULL
    ALTER TABLE dbo.audit_log ADD tablename NVARCHAR(100) NULL;
GO
IF COL_LENGTH('dbo.audit_log', 'watermark_column') IS NULL
    ALTER TABLE dbo.audit_log ADD watermark_column NVARCHAR(100) NULL;
GO
IF COL_LENGTH('dbo.audit_log', 'last_watermark_value') IS NULL
    ALTER TABLE dbo.audit_log ADD last_watermark_value NVARCHAR(50) NULL;
GO
IF COL_LENGTH('dbo.audit_log', 'rows_read') IS NULL
    ALTER TABLE dbo.audit_log ADD rows_read BIGINT NULL;
GO

-- Insert one audit row per (datasource, tablename) load, recording the watermark used.
CREATE OR ALTER PROCEDURE dbo.sp_log_load
    @pipeline_name        NVARCHAR(200),
    @datasource           NVARCHAR(50),
    @tablename            NVARCHAR(100),
    @load_type            NVARCHAR(20),
    @watermark_column     NVARCHAR(100) = NULL,
    @last_watermark_value NVARCHAR(50)  = NULL,
    @rows_written         BIGINT        = NULL,
    @status               NVARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.audit_log
        (pipeline_name, datasource, tablename, load_type, watermark_column,
         last_watermark_value, rows_written, status)
    VALUES
        (@pipeline_name, @datasource, @tablename, @load_type, @watermark_column,
         @last_watermark_value, @rows_written, @status);
END
GO

-- Helper (optional): read the last successful watermark for a table/hospital.
-- (The ADF pipeline reads this inline via a Lookup, but the function documents intent.)
CREATE OR ALTER FUNCTION dbo.fn_last_watermark(@datasource NVARCHAR(50), @tablename NVARCHAR(100))
RETURNS NVARCHAR(50)
AS
BEGIN
    RETURN (
        SELECT ISNULL(MAX(last_watermark_value), '1900-01-01T00:00:00')
        FROM dbo.audit_log
        WHERE status = 'success' AND datasource = @datasource AND tablename = @tablename
    );
END
GO
