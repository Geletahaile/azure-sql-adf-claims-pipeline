/* ============================================================================
   sql/01-schemas-and-tables.sql
   
   Project:  Healthcare Claims ETL Pipeline (Azure SQL + ADF)
   Purpose:  Create the 6 schemas and the Claim table across all 5 ETL layers.
   Author:   Geleta Hamda
   Database: Azure SQL Database (sqldb-geletaedw-dev)
   
   Architecture: 5-layer EDW pattern + meta orchestration
       src   -> CSV landing zone (all varchar)
       raw   -> typed, AlternateKey added
       stg   -> validation results, RowHash, DataChangeCode
       hold  -> error quarantine mirroring raw
       dw    -> final warehouse (surrogate keys, SCD1)
       meta  -> pipeline orchestration & audit
============================================================================ */

USE [sqldb-geletaedw-dev];
GO

-- ============================================================================
-- SECTION 1: Create Schemas
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'src')   EXEC('CREATE SCHEMA src   AUTHORIZATION dbo;');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')   EXEC('CREATE SCHEMA raw   AUTHORIZATION dbo;');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')   EXEC('CREATE SCHEMA stg   AUTHORIZATION dbo;');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'hold')  EXEC('CREATE SCHEMA hold  AUTHORIZATION dbo;');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')    EXEC('CREATE SCHEMA dw    AUTHORIZATION dbo;');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta')  EXEC('CREATE SCHEMA meta  AUTHORIZATION dbo;');
GO

-- ============================================================================
-- SECTION 2: src.Claim
-- CSV landing zone. All varchar to preserve source fidelity and absorb dirty
-- data without typing errors during ADF Copy. Equivalent to integration-team
-- staging (dwSource.hsp.* in production EDW patterns).
-- ============================================================================
CREATE TABLE [src].[Claim] (
    [ClaimID]           varchar(20)  NULL,
    [MemberID]          varchar(20)  NULL,
    [ProviderID]        varchar(20)  NULL,
    [ServiceFromDate]   varchar(20)  NULL,
    [ServiceToDate]     varchar(20)  NULL,
    [PaidAmount]        varchar(20)  NULL,
    [BilledAmount]      varchar(20)  NULL,
    [ClaimStatus]       varchar(20)  NULL,
    [AdjudicationDate]  varchar(20)  NULL,
    [SourceSystem]      varchar(50)  NULL,
    [LoadDate]          datetime2(0) NOT NULL 
        CONSTRAINT DF_src_Claim_LoadDate DEFAULT (SYSUTCDATETIME())
);
GO

-- ============================================================================
-- SECTION 3: raw.Claim
-- Post-Extract destination. Typed columns, AlternateKey for downstream joins,
-- SourceSystem stamp for multi-source disambiguation. AlternateKey is a SHA-256
-- hash of (SourceSystem ~ ClaimID), enabling deterministic joins independent
-- of natural-key formatting variations.
-- ============================================================================
CREATE TABLE [raw].[Claim] (
    [AlternateKey]      varbinary(32) NOT NULL,
    [SourceSystem]      varchar(50)   NULL,
    [ClaimID]           varchar(20)   NOT NULL,
    [MemberID]          varchar(20)   NULL,     -- nullable so validation can flag downstream
    [ProviderID]        varchar(20)   NULL,     -- nullable so validation can flag downstream
    [ServiceFromDate]   date          NULL,
    [ServiceToDate]     date          NULL,
    [PaidAmount]        decimal(18,2) NULL,
    [BilledAmount]      decimal(18,2) NULL,
    [ClaimStatus]       varchar(20)   NULL,
    [AdjudicationDate]  date          NULL,
    [LoadDate]          datetime2(0)  NOT NULL 
        CONSTRAINT DF_raw_Claim_LoadDate DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_raw_Claim_AlternateKey UNIQUE NONCLUSTERED ([AlternateKey])
);
GO

CREATE INDEX IX_raw_Claim_ClaimID ON [raw].[Claim] ([ClaimID]);
GO

-- ============================================================================
-- SECTION 4: stg.Claim
-- Validation layer. Adds IsError (flag), HoldReason (specific column failures),
-- DataChangeCode (1=new, 2=changed, 0=unchanged), RowHash (MD5 fingerprint
-- of business columns for change detection), and PipelineID for lineage.
-- ============================================================================
CREATE TABLE [stg].[Claim] (
    [AlternateKey]      varbinary(32) NOT NULL,
    [SourceSystem]      varchar(50)   NULL,
    [ClaimID]           varchar(20)   NOT NULL,
    [MemberID]          varchar(20)   NULL,
    [ProviderID]        varchar(20)   NULL,
    [ServiceFromDate]   date          NULL,
    [ServiceToDate]     date          NULL,
    [PaidAmount]        decimal(18,2) NULL,
    [BilledAmount]      decimal(18,2) NULL,
    [ClaimStatus]       varchar(20)   NULL,
    [AdjudicationDate]  date          NULL,
    [LoadDate]          datetime2(0)  NOT NULL,
    [PipelineID]        int           NULL,
    [IsError]           bit           NOT NULL 
        CONSTRAINT DF_stg_Claim_IsError DEFAULT ((0)),
    [HoldReason]        varchar(max)  NULL,
    [DataChangeCode]    int           NULL,
    [RowHash]           varbinary(16) NULL,
    CONSTRAINT UQ_stg_Claim_AlternateKey UNIQUE NONCLUSTERED ([AlternateKey])
);
GO

-- ============================================================================
-- SECTION 5: hold.Claim
-- Error quarantine sidecar mirroring raw.Claim structure (typed + AlternateKey).
-- Records flagged IsError=1 in stg are pushed here with HoldReason text.
-- On the next pipeline run, sp_StageClaim re-injects held rows into raw for
-- self-healing reprocessing (in case upstream has fixed the missing data).
-- ============================================================================
CREATE TABLE [hold].[Claim] (
    [AlternateKey]      varbinary(32) NOT NULL,
    [SourceSystem]      varchar(50)   NULL,
    [ClaimID]           varchar(20)   NULL,
    [MemberID]          varchar(20)   NULL,
    [ProviderID]        varchar(20)   NULL,
    [ServiceFromDate]   date          NULL,
    [ServiceToDate]     date          NULL,
    [PaidAmount]        decimal(18,2) NULL,
    [BilledAmount]      decimal(18,2) NULL,
    [ClaimStatus]       varchar(20)   NULL,
    [AdjudicationDate]  date          NULL,
    [InsertDate]        datetime2(0)  NOT NULL 
        CONSTRAINT DF_hold_Claim_InsertDate DEFAULT (SYSUTCDATETIME()),
    [HoldReason]        varchar(max)  NULL
);
GO

-- ============================================================================
-- SECTION 6: dw.Claim
-- Final warehouse table. Surrogate key (ClaimSK) + AlternateKey enables both
-- efficient surrogate-key joins for downstream facts and deterministic
-- AlternateKey-based change detection from stg. SCD1 with EffectiveStart/End
-- columns ready for future SCD2 extension.
-- ============================================================================
CREATE TABLE [dw].[Claim] (
    [ClaimSK]            int           IDENTITY(1,1) NOT NULL,
    [AlternateKey]       varbinary(32) NOT NULL,
    [SourceSystem]       varchar(50)   NULL,
    [ClaimID]            varchar(20)   NOT NULL,
    [MemberID]           varchar(20)   NOT NULL,         -- required at warehouse level (post-validation)
    [ProviderID]         varchar(20)   NOT NULL,         -- required at warehouse level (post-validation)
    [ServiceFromDate]    date          NULL,
    [ServiceToDate]      date          NULL,
    [PaidAmount]         decimal(18,2) NULL,
    [BilledAmount]       decimal(18,2) NULL,
    [ClaimStatus]        varchar(20)   NULL,
    [AdjudicationDate]   date          NULL,
    [IsCurrent]          bit           NOT NULL 
        CONSTRAINT DF_dw_Claim_IsCurrent DEFAULT ((1)),
    [EffectiveStartDate] datetime2(0)  NOT NULL 
        CONSTRAINT DF_dw_Claim_EffectiveStartDate DEFAULT (SYSUTCDATETIME()),
    [EffectiveEndDate]   datetime2(0)  NULL,
    [LastUpdatedDate]    datetime2(0)  NOT NULL 
        CONSTRAINT DF_dw_Claim_LastUpdatedDate DEFAULT (SYSUTCDATETIME()),
    [PipelineID]         int           NULL,
    [RowHash]            varbinary(16) NULL,
    CONSTRAINT PK_dw_Claim PRIMARY KEY CLUSTERED ([ClaimSK]),
    CONSTRAINT UQ_dw_Claim_AlternateKey UNIQUE NONCLUSTERED ([AlternateKey]),
    CONSTRAINT UQ_dw_Claim_ClaimID      UNIQUE NONCLUSTERED ([ClaimID])
);
GO

CREATE INDEX IX_dw_Claim_MemberID ON [dw].[Claim] ([MemberID]);
GO

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT name FROM sys.schemas
WHERE name IN ('src','raw','stg','hold','dw','meta')
ORDER BY name;
-- Expect 6 rows: dw, hold, meta, raw, src, stg

SELECT s.name AS [schema], t.name AS [table],
       (SELECT COUNT(*) FROM sys.columns WHERE object_id = t.object_id) AS col_count
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name IN ('src','raw','stg','hold','dw')
ORDER BY s.name, t.name;
/* Expected column counts:
   dw.Claim      18
   hold.Claim    13
   raw.Claim     12
   src.Claim     11
   stg.Claim     17
*/
GO
