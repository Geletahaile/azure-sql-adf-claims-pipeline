/* ============================================================================
   04-seed-data.sql
   
   Project:  Healthcare Claims ETL Pipeline (Azure SQL + ADF)
   Purpose:  Seed the meta tables with the CLAIMS group and Claim entity 
             configuration that the ADF pipeline reads at runtime.
   Author:   Geleta Hamda
   Database: Azure SQL Database (sqldb-geletaedw-dev)
   
   What this script does:
       1. Registers the CLAIMS group in meta.TableGroup
       2. Registers the Claim entity in meta.SourceTableLoad, pointing at:
          - Source blob path: raw/claim/claim.csv
          - The three stored procedures: stg.sp_ExtractClaim, 
            stg.sp_StageClaim, stg.sp_LoadClaim
   
   To add a new entity (e.g. ClaimDiagnosis) later:
       - Create src/raw/stg/hold/dw tables for the new entity
       - Create three corresponding procs (sp_ExtractX, sp_StageX, sp_LoadX)
       - INSERT a new row into meta.SourceTableLoad for the entity
       - No ADF pipeline changes required - the ForEach picks it up automatically
   
   Prerequisite: Tables (01) and procs (03) must exist before this runs.
============================================================================ */

USE [sqldb-geletaedw-dev];
GO

-- ============================================================================
-- SECTION 1: Register the CLAIMS group
-- ============================================================================
INSERT INTO [meta].[TableGroup] (GroupName, GroupDescription)
VALUES ('CLAIMS', 'Healthcare claims ETL - CSV to dw.Claim');
GO

-- ============================================================================
-- SECTION 2: Register the Claim entity under the CLAIMS group
-- ============================================================================
DECLARE @gid INT = (SELECT GroupID FROM meta.TableGroup WHERE GroupName = 'CLAIMS');

INSERT INTO [meta].[SourceTableLoad] (
    GroupID, 
    LoadOrder, 
    SourceTableName, 
    TargetSchemaName, 
    TargetTableName,
    SourceFolderPath, 
    SourceFileName, 
    ExtractStoredProc, 
    StageStoredProc, 
    LoadStoredProc
)
VALUES (
    @gid, 
    1,                              -- LoadOrder (1st in group)
    'Claim',                        -- SourceTableName (used in src.{name}, dynamic in ADF)
    'dw',                           -- TargetSchemaName
    'Claim',                        -- TargetTableName
    'raw/claim/',                   -- SourceFolderPath (in landing/ blob container)
    'claim.csv',                    -- SourceFileName
    'stg.sp_ExtractClaim',          -- ExtractStoredProc (driven by ForEach in ADF)
    'stg.sp_StageClaim',            -- StageStoredProc
    'stg.sp_LoadClaim'              -- LoadStoredProc
);
GO

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT GroupID, GroupName, GroupDescription, IsActive, CreatedDate
FROM meta.TableGroup;
-- Expect 1 row: GroupName = 'CLAIMS'

SELECT 
    GroupID, LoadOrder, SourceTableName, TargetSchemaName, TargetTableName,
    SourceFolderPath, SourceFileName, 
    ExtractStoredProc, StageStoredProc, LoadStoredProc,
    IsActive
FROM meta.SourceTableLoad;
-- Expect 1 row: SourceTableName = 'Claim', all three procs populated
GO
