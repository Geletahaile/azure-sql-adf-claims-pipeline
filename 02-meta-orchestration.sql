/* ============================================================================
   02-meta-orchestration.sql
   
   Project:  Healthcare Claims ETL Pipeline (Azure SQL + ADF)
   Purpose:  Create the meta schema tables that drive metadata-driven ADF
             pipeline orchestration and pipeline-run audit logging.
   Author:   Geleta Hamda
   Database: Azure SQL Database (sqldb-geletaedw-dev)
   
   Tables created:
       meta.TableGroup        - Logical groupings of related entities
       meta.SourceTableLoad   - Per-table orchestration config (source, target, procs)
       meta.PipelineLog       - Run audit (start/end times, status, row counts, errors)
   
   These tables are queried by the ADF pipeline (pl_load_claims_edw) to drive
   the ForEach loop that processes each registered entity. Adding a new entity
   requires only one INSERT into SourceTableLoad + creating the corresponding
   Extract/Stage/Load procs - no pipeline code changes.
   
   Prerequisite: 01-schemas-and-tables.sql must be executed first (meta schema)
============================================================================ */

USE [sqldb-geletaedw-dev];
GO

-- ============================================================================
-- SECTION 1: meta.TableGroup
-- Logical groupings of related entities (e.g. "CLAIMS" group might contain
-- Claim, ClaimDiagnosis, ClaimAdjustment in production). ADF pipelines accept
-- a GroupName parameter and process all entities in that group via ForEach.
-- ============================================================================
CREATE TABLE [meta].[TableGroup] (
    [GroupID]          int          IDENTITY(1,1) NOT NULL,
    [GroupName]        varchar(50)  NOT NULL,
    [GroupDescription] varchar(255) NULL,
    [IsActive]         bit          NOT NULL 
        CONSTRAINT DF_meta_TableGroup_IsActive DEFAULT ((1)),
    [CreatedDate]      datetime2(0) NOT NULL 
        CONSTRAINT DF_meta_TableGroup_CreatedDate DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_meta_TableGroup       PRIMARY KEY CLUSTERED ([GroupID]),
    CONSTRAINT UQ_meta_TableGroup_Name  UNIQUE NONCLUSTERED ([GroupName])
);
GO

-- ============================================================================
-- SECTION 2: meta.SourceTableLoad
-- Per-table orchestration config. Each row describes one entity:
--   - Source file location and name in blob storage (FolderPath/FileName)
--   - Target schema and table names in the warehouse
--   - The three stored procedure names (Extract/Stage/Load) the pipeline runs
--
-- The ADF pipeline reads this via the GetTablesForGroup Lookup activity and
-- iterates each row using a ForEach activity. Inside the loop, expressions
-- like @{item().ExtractStoredProc} drive Stored Procedure activities
-- dynamically. To add a new entity, INSERT a row here - no pipeline edits.
-- ============================================================================
CREATE TABLE [meta].[SourceTableLoad] (
    [SourceTableLoadID] int          IDENTITY(1,1) NOT NULL,
    [GroupID]           int          NOT NULL,
    [LoadOrder]         int          NOT NULL,
    [SourceTableName]   varchar(100) NOT NULL,
    [TargetSchemaName]  varchar(20)  NOT NULL,
    [TargetTableName]   varchar(100) NOT NULL,
    [SourceFolderPath]  varchar(255) NOT NULL,
    [SourceFileName]    varchar(255) NOT NULL,
    [ExtractStoredProc] varchar(150) NOT NULL,  -- e.g. stg.sp_ExtractClaim
    [StageStoredProc]   varchar(150) NOT NULL,  -- e.g. stg.sp_StageClaim
    [LoadStoredProc]    varchar(150) NOT NULL,  -- e.g. stg.sp_LoadClaim
    [IsActive]          bit          NOT NULL 
        CONSTRAINT DF_meta_SourceTableLoad_IsActive DEFAULT ((1)),
    [CreatedDate]       datetime2(0) NOT NULL 
        CONSTRAINT DF_meta_SourceTableLoad_CreatedDate DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_meta_SourceTableLoad PRIMARY KEY CLUSTERED ([SourceTableLoadID]),
    CONSTRAINT FK_meta_SourceTableLoad_TableGroup 
        FOREIGN KEY ([GroupID]) REFERENCES [meta].[TableGroup]([GroupID])
);
GO

-- ============================================================================
-- SECTION 3: meta.PipelineLog
-- Run audit. Every pipeline execution writes a row here via sp_LogPipelineStart
-- (at the start) and sp_LogPipelineEnd (at the end, success or failure).
-- Used to build dashboards, monitor SLA compliance, and trace data lineage 
-- (PipelineID stamped on stg.Claim and dw.Claim during each run).
-- ============================================================================
CREATE TABLE [meta].[PipelineLog] (
    [PipelineID]    int           IDENTITY(1,1) NOT NULL,
    [PipelineName]  varchar(150)  NOT NULL,
    [GroupName]     varchar(50)   NULL,
    [StartTime]     datetime2(0)  NOT NULL 
        CONSTRAINT DF_meta_PipelineLog_StartTime DEFAULT (SYSUTCDATETIME()),
    [EndTime]       datetime2(0)  NULL,
    [Status]        varchar(20)   NOT NULL 
        CONSTRAINT DF_meta_PipelineLog_Status DEFAULT ('Running'),
    [ErrorActivity] varchar(255)  NULL,
    [ErrorMessage]  nvarchar(max) NULL,
    [RowsProcessed] int           NULL,
    CONSTRAINT PK_meta_PipelineLog PRIMARY KEY CLUSTERED ([PipelineID])
);
GO

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT s.name AS [schema], t.name AS [table],
       (SELECT COUNT(*) FROM sys.columns WHERE object_id = t.object_id) AS col_count
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = 'meta'
ORDER BY t.name;
/* Expected column counts:
   meta.PipelineLog       9
   meta.SourceTableLoad  13
   meta.TableGroup        5
*/
GO
