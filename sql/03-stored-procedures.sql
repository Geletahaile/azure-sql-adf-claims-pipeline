/* ============================================================================
   03-stored-procedures.sql
   
   Project:  Healthcare Claims ETL Pipeline (Azure SQL + ADF)
   Purpose:  Create the 5 stored procedures that drive the ETL pipeline.
   Author:   Geleta Hamda
   Database: Azure SQL Database (sqldb-geletaedw-dev)
   
   Procedures created:
       meta.sp_LogPipelineStart  - Insert pipeline-run row, return new PipelineID
       meta.sp_LogPipelineEnd    - Close out pipeline-run row with final status
       stg.sp_ExtractClaim       - src.Claim -> raw.Claim (typing + AlternateKey)
       stg.sp_StageClaim         - raw.Claim -> stg.Claim, errors -> hold.Claim
       stg.sp_LoadClaim          - stg.Claim -> dw.Claim (SCD1 with batched DML)
   
   Design pattern: split Extract/Stage/Load mirrors the modernized EDW pattern
   from Vaya Health. Each procedure has a single responsibility, can be re-run
   independently, and is idempotent (safe to re-execute on the same source).
   
   Prerequisite: 01-schemas-and-tables.sql and 02-meta-orchestration.sql 
                 must be executed first.
============================================================================ */

USE [sqldb-geletaedw-dev];
GO

-- ============================================================================
-- meta.sp_LogPipelineStart
-- Insert a pipeline run row with StartTime=now and Status='Running'.
-- Returns the new identity PipelineID for downstream procs to stamp records 
-- with for lineage.
-- ============================================================================
CREATE OR ALTER PROCEDURE [meta].[sp_LogPipelineStart]
    @PipelineName VARCHAR(150),
    @GroupName    VARCHAR(50) = NULL,
    @PipelineID   INT         = 0 OUTPUT
AS
/* =============================================================================
SAMPLE CALL:
    DECLARE @pid INT;
    EXEC meta.sp_LogPipelineStart 
        @PipelineName = 'ClaimDomain_FullRefresh', 
        @GroupName    = 'CLAIMS',
        @PipelineID   = @pid OUTPUT;
============================================================================= */
BEGIN
    SET NOCOUNT ON;

    INSERT INTO meta.PipelineLog (PipelineName, GroupName, StartTime, Status)
    VALUES (@PipelineName, @GroupName, SYSUTCDATETIME(), 'Running');

    SET @PipelineID = CAST(SCOPE_IDENTITY() AS INT);
    SELECT @PipelineID AS PipelineID;
END;
GO


-- ============================================================================
-- meta.sp_LogPipelineEnd
-- Close out a pipeline run row. Called once at the end of every pipeline run
-- (success or failure paths in ADF) to populate EndTime, final Status, row 
-- counts, and any error context for the audit trail.
-- ============================================================================
CREATE OR ALTER PROCEDURE [meta].[sp_LogPipelineEnd]
    @PipelineID    INT,
    @Status        VARCHAR(20)   = 'Succeeded',
    @RowsProcessed INT           = NULL,
    @ErrorActivity VARCHAR(255)  = NULL,
    @ErrorMessage  NVARCHAR(MAX) = NULL
AS
/* =============================================================================
SAMPLE CALL:
    EXEC meta.sp_LogPipelineEnd 
        @PipelineID = @pid, @Status = 'Succeeded', @RowsProcessed = 1000;
============================================================================= */
BEGIN
    SET NOCOUNT ON;

    UPDATE meta.PipelineLog
    SET EndTime       = SYSUTCDATETIME(),
        Status        = @Status,
        RowsProcessed = @RowsProcessed,
        ErrorActivity = @ErrorActivity,
        ErrorMessage  = @ErrorMessage
    WHERE PipelineID = @PipelineID;
END;
GO


-- ============================================================================
-- stg.sp_ExtractClaim
-- Extract phase: src.Claim -> raw.Claim
--   1. Drops the raw index (for faster bulk insert)
--   2. Truncates raw.Claim
--   3. Inserts from src.Claim with:
--      - Type conversion via TRY_CAST (date, decimal)
--      - AlternateKey computed as SHA-256 hash of (SourceSystem ~ ClaimID)
--      - SourceSystem stamp for multi-source disambiguation
--      - SELECT DISTINCT to dedupe upstream duplicates
--      - @LastUpdated incremental filter (NULL = full refresh)
--      - Filters NULL ClaimID rows (cannot generate a stable key)
--   4. Recreates the index
-- ============================================================================
CREATE OR ALTER PROCEDURE [stg].[sp_ExtractClaim]
    @LastUpdated  DATETIME    = NULL,           -- NULL = full refresh; else filter on src.LoadDate
    @SourceSystem VARCHAR(50) = 'CSV-IMPORT',
    @Extracted    INT         = 0 OUTPUT
AS
/* =============================================================================
SAMPLE CALL:
    DECLARE @Extracted INT;
    EXEC stg.sp_ExtractClaim 
        @LastUpdated  = NULL,
        @SourceSystem = 'CSV-IMPORT',
        @Extracted    = @Extracted OUTPUT;
============================================================================= */
BEGIN
    SET NOCOUNT ON;

    DROP INDEX IF EXISTS IX_raw_Claim_ClaimID ON raw.Claim;
    TRUNCATE TABLE raw.Claim;

    INSERT INTO raw.Claim
    (
        AlternateKey, SourceSystem, ClaimID, MemberID, ProviderID,
        ServiceFromDate, ServiceToDate, PaidAmount, BilledAmount,
        ClaimStatus, AdjudicationDate, LoadDate
    )
    SELECT DISTINCT
           HASHBYTES('SHA2_256', 
               CAST(CONCAT_WS('~', @SourceSystem, s.ClaimID) AS NVARCHAR(75))
           ) AS AlternateKey,
           @SourceSystem,
           s.ClaimID,
           s.MemberID,
           s.ProviderID,
           TRY_CAST(s.ServiceFromDate  AS DATE),
           TRY_CAST(s.ServiceToDate    AS DATE),
           TRY_CAST(s.PaidAmount       AS DECIMAL(18,2)),
           TRY_CAST(s.BilledAmount     AS DECIMAL(18,2)),
           s.ClaimStatus,
           TRY_CAST(s.AdjudicationDate AS DATE),
           SYSUTCDATETIME()
    FROM src.Claim s
    WHERE s.ClaimID IS NOT NULL
      AND (@LastUpdated IS NULL OR s.LoadDate >= @LastUpdated);

    SET @Extracted = @@ROWCOUNT;

    CREATE INDEX IX_raw_Claim_ClaimID ON raw.Claim (ClaimID);
    SELECT @Extracted AS Extracted;
END;
GO


-- ============================================================================
-- stg.sp_StageClaim
-- Stage phase: raw.Claim -> stg.Claim, errors routed to hold.Claim
--   1. Truncate stg.Claim
--   2. Self-healing hold reprocessing:
--      - Delete hold rows that already exist in raw (avoid duplicates)
--      - Re-inject remaining hold rows into raw (allowing retry next run)
--   3. Copy raw -> stg (type-stable, no further conversion needed)
--   4. Compute RowHash via MD5 over business columns for change detection
--   5. Validation framework:
--      - Dynamic SQL builds a WHERE clause flagging IsError=1 for rows with
--        NULL in any required column (declared in #Claim_columns temp table)
--      - Cursor iterates required columns, building per-row HoldReason text
--   6. Push error rows to hold.Claim (TRUNCATE first; INNER JOIN raw->stg
--      on AlternateKey to bring typed-source values along with HoldReason)
-- ============================================================================
CREATE OR ALTER PROCEDURE [stg].[sp_StageClaim]
    @PipelineID INT = NULL,
    @Staged     INT = 0 OUTPUT,
    @Hold       INT = 0 OUTPUT
AS
/* =============================================================================
SAMPLE CALL:
    DECLARE @s INT, @h INT;
    EXEC stg.sp_StageClaim 
        @PipelineID = 1, 
        @Staged     = @s OUTPUT, 
        @Hold       = @h OUTPUT;
============================================================================= */
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    SET @Staged = 0;
    SET @Hold   = 0;

    -- Truncate stg
    TRUNCATE TABLE stg.Claim;

    -- Remove records from hold that already exist in raw
    DELETE e
    FROM hold.Claim e
    WHERE EXISTS
    (
        SELECT 1 FROM raw.Claim r WHERE r.AlternateKey = e.AlternateKey
    );

    -- Re-inject remaining hold records into raw for reprocessing
    INSERT INTO raw.Claim
    (
        AlternateKey, SourceSystem, ClaimID, MemberID, ProviderID,
        ServiceFromDate, ServiceToDate, PaidAmount, BilledAmount,
        ClaimStatus, AdjudicationDate, LoadDate
    )
    SELECT h.AlternateKey, h.SourceSystem, h.ClaimID, h.MemberID, h.ProviderID,
           h.ServiceFromDate, h.ServiceToDate, h.PaidAmount, h.BilledAmount,
           h.ClaimStatus, h.AdjudicationDate, SYSUTCDATETIME()
    FROM hold.Claim h
    WHERE NOT EXISTS
    (
        SELECT 1 FROM raw.Claim r WHERE r.AlternateKey = h.AlternateKey
    );

    -- Map raw -> stg (raw is already typed; no further conversion needed)
    INSERT INTO stg.Claim
    (
        AlternateKey, SourceSystem, ClaimID, MemberID, ProviderID,
        ServiceFromDate, ServiceToDate, PaidAmount, BilledAmount,
        ClaimStatus, AdjudicationDate, LoadDate, PipelineID, IsError
    )
    SELECT r.AlternateKey,
           r.SourceSystem,
           r.ClaimID,
           r.MemberID,
           r.ProviderID,
           r.ServiceFromDate,
           r.ServiceToDate,
           r.PaidAmount,
           r.BilledAmount,
           r.ClaimStatus,
           r.AdjudicationDate,
           SYSUTCDATETIME(),
           @PipelineID,
           0
    FROM raw.Claim r;

    -- RowHash (MD5) for change detection
    UPDATE stg.Claim 
    SET RowHash = HASHBYTES('MD5', CONCAT_WS('~',
        [ClaimID],[MemberID],[ProviderID],
        [ServiceFromDate],[ServiceToDate],
        [PaidAmount],[BilledAmount],
        [ClaimStatus],[AdjudicationDate],[SourceSystem]
    ));

    -- Validation framework: flag stg rows where any required column is NULL.
    -- Required-column list is data-driven (in #Claim_columns) so adding a 
    -- new required field is a one-line change, not a proc rewrite.
    DROP TABLE IF EXISTS #Claim_columns;
    SELECT column_name
    INTO #Claim_columns
    FROM (VALUES ('MemberID'), ('ProviderID')) v(column_name);

    SET @SQL = N'UPDATE c SET IsError = 1 FROM stg.Claim c WHERE (';
    SET @SQL = @SQL + (SELECT STRING_AGG(column_name, ' IS NULL OR ') FROM #Claim_columns);
    SET @SQL = @SQL + N' IS NULL);';
    EXECUTE sp_executesql @SQL;

    -- Build HoldReason column-by-column via cursor (e.g. 'MemberID; ProviderID; ')
    DECLARE @ColumnName VARCHAR(250);
    DECLARE ColumnCursor CURSOR FOR SELECT COLUMN_NAME FROM #Claim_columns;
    OPEN ColumnCursor;
    FETCH NEXT FROM ColumnCursor INTO @ColumnName;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @SQL = 'UPDATE stg.Claim 
                    SET HoldReason = CONCAT(ISNULL(HoldReason,''''),'''+@ColumnName+'; '')
                    WHERE IsError = 1 AND '+@ColumnName+' IS NULL';
        EXECUTE sp_executesql @SQL;
        FETCH NEXT FROM ColumnCursor INTO @ColumnName;
    END;
    CLOSE ColumnCursor;
    DEALLOCATE ColumnCursor;

    -- Push error rows into hold (TRUNCATE first; pull typed values from raw,
    -- HoldReason from stg, joined on AlternateKey)
    TRUNCATE TABLE hold.Claim;

    INSERT INTO hold.Claim
    (
        AlternateKey, SourceSystem, ClaimID, MemberID, ProviderID,
        ServiceFromDate, ServiceToDate, PaidAmount, BilledAmount,
        ClaimStatus, AdjudicationDate, InsertDate, HoldReason
    )
    SELECT r.AlternateKey, r.SourceSystem, r.ClaimID, r.MemberID, r.ProviderID,
           r.ServiceFromDate, r.ServiceToDate, r.PaidAmount, r.BilledAmount,
           r.ClaimStatus, r.AdjudicationDate, SYSUTCDATETIME(), s.HoldReason
    FROM raw.Claim r
    INNER JOIN stg.Claim s ON s.AlternateKey = r.AlternateKey
    WHERE s.IsError = 1;

    SET @Hold   = ISNULL(@@ROWCOUNT, 0);
    SET @Staged = (SELECT COUNT(*) FROM stg.Claim WHERE IsError = 0);

    SELECT @Staged AS Staged, @Hold AS [Hold];
END;
GO


-- ============================================================================
-- stg.sp_LoadClaim
-- Load phase: stg.Claim -> dw.Claim (SCD1 with change detection)
--   1. Compute DataChangeCode by AlternateKey LEFT JOIN to dw.Claim:
--      1 = new (RowHash IS NULL in dw - never seen before)
--      2 = changed (RowHash differs - update needed)
--      0 = unchanged (no-op, saves write I/O)
--   2. Batched INSERT for new rows (DataChangeCode = 1):
--      - Materialize to #newClaim with identity column for batch control
--      - WHILE loop processing 150,000 rows per batch
--      - BEGIN/COMMIT TRANSACTION + CHECKPOINT per batch to manage log
--   3. Set-based UPDATE for changed rows (DataChangeCode = 2):
--      - All business columns refreshed (SCD1 - no historical preservation)
--      - LastUpdatedDate, PipelineID, RowHash also updated
-- ============================================================================
CREATE OR ALTER PROCEDURE [stg].[sp_LoadClaim]
    @PipelineID INT = NULL,
    @Inserted   INT = 0 OUTPUT,
    @Updated    INT = 0 OUTPUT
AS
/* =============================================================================
SAMPLE CALL:
    DECLARE @i INT, @u INT;
    EXEC stg.sp_LoadClaim 
        @PipelineID = 1, 
        @Inserted   = @i OUTPUT, 
        @Updated    = @u OUTPUT;
============================================================================= */
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch       INT = 150000;
    DECLARE @rn_control  INT;
    DECLARE @lastCount   INT = 1;

    SET @Inserted = 0;
    SET @Updated  = 0;

    -- 1. Compute DataChangeCode
    UPDATE s
    SET DataChangeCode = CASE
                            WHEN e.RowHash IS NULL THEN 1               -- new
                            WHEN s.RowHash <> e.RowHash THEN 2          -- changed
                            ELSE 0                                      -- unchanged
                        END
    FROM stg.Claim s
    LEFT JOIN dw.Claim e ON e.AlternateKey = s.AlternateKey
    WHERE s.IsError = 0;

    -- 2. Batched INSERT for new rows
    DROP TABLE IF EXISTS #newClaim;
    SELECT *
    INTO #newClaim
    FROM stg.Claim
    WHERE DataChangeCode = 1 AND IsError = 0;

    ALTER TABLE #newClaim ADD New_ID INT IDENTITY(1, 1);

    SET @rn_control = (SELECT MIN(New_ID) FROM #newClaim);

    WHILE @lastCount > 0
    BEGIN
        BEGIN TRANSACTION LoadClaim;

        INSERT INTO dw.Claim
        (
            AlternateKey, SourceSystem, ClaimID, MemberID, ProviderID,
            ServiceFromDate, ServiceToDate, PaidAmount, BilledAmount,
            ClaimStatus, AdjudicationDate,
            IsCurrent, EffectiveStartDate, EffectiveEndDate,
            LastUpdatedDate, PipelineID, RowHash
        )
        SELECT AlternateKey,
               SourceSystem,
               ClaimID,
               MemberID,
               ProviderID,
               ServiceFromDate,
               ServiceToDate,
               PaidAmount,
               BilledAmount,
               ClaimStatus,
               AdjudicationDate,
               1,
               SYSUTCDATETIME(),
               NULL,
               SYSUTCDATETIME(),
               @PipelineID,
               RowHash
        FROM #newClaim
        WHERE New_ID >= @rn_control AND New_ID < (@rn_control + @batch);

        SET @lastCount  = @@ROWCOUNT;
        SET @Inserted   = @Inserted + @lastCount;
        SET @rn_control = @rn_control + @batch;

        COMMIT TRANSACTION LoadClaim;

        CHECKPOINT;
    END;

    -- 3. UPDATE existing rows (SCD1 - full business-column refresh)
    UPDATE e
    SET SourceSystem      = s.SourceSystem,
        ClaimID           = s.ClaimID,
        MemberID          = s.MemberID,
        ProviderID        = s.ProviderID,
        ServiceFromDate   = s.ServiceFromDate,
        ServiceToDate     = s.ServiceToDate,
        PaidAmount        = s.PaidAmount,
        BilledAmount      = s.BilledAmount,
        ClaimStatus       = s.ClaimStatus,
        AdjudicationDate  = s.AdjudicationDate,
        LastUpdatedDate   = SYSUTCDATETIME(),
        PipelineID        = @PipelineID,
        e.RowHash         = s.RowHash
    FROM dw.Claim e
    INNER JOIN stg.Claim s ON s.AlternateKey = e.AlternateKey
    WHERE s.DataChangeCode = 2 AND s.IsError = 0;

    SET @Updated = ISNULL(@@ROWCOUNT, 0);

    SELECT @Inserted AS Inserted, @Updated AS Updated;
END;
GO


-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT SCHEMA_NAME(schema_id) AS [schema], name, create_date, modify_date
FROM sys.procedures
WHERE name IN ('sp_LogPipelineStart','sp_LogPipelineEnd',
               'sp_ExtractClaim','sp_StageClaim','sp_LoadClaim')
ORDER BY [schema], name;
-- Expect 5 rows
GO
