# Healthcare Claims ETL Pipeline — Azure SQL Database + Azure Data Factory

> An end-to-end, production-pattern healthcare claims data warehouse pipeline built on Azure, demonstrating advanced SQL Server data engineering and modern cloud orchestration working together.

[![SQL Server](https://img.shields.io/badge/SQL%20Server-T--SQL-CC2927?logo=microsoftsqlserver&logoColor=white)](https://learn.microsoft.com/en-us/sql/t-sql/language-reference)
[![Azure SQL](https://img.shields.io/badge/Azure-SQL%20Database-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/azure-sql/database)
[![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/data-factory)
[![ADLS Gen2](https://img.shields.io/badge/Azure-Data%20Lake%20Gen2-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/storage/data-lake-storage)

---

## Project Overview

This project rebuilds a real production data engineering pattern from a healthcare claims Enterprise Data Warehouse (EDW) — the kind used to process Medicaid claims, adjudicate payments, and feed downstream reporting at scale — and deploys it on Azure cloud infrastructure as a fully working, observable, metadata-driven ETL pipeline.

It was built to demonstrate two skill domains side by side:

- **SQL Server / T-SQL expertise**: 5-layer warehouse architecture, split stored procedures (Extract / Stage / Load), AlternateKey-based change detection, RowHash content comparison, dynamic SQL validation framework, batched DML, self-healing hold-table reprocessing.
- **Azure cloud expertise**: Azure SQL Database, Azure Data Factory metadata-driven orchestration with ForEach iteration, Azure Data Lake Storage Gen2, linked services, parameterized datasets, success/failure path handling, pipeline audit logging.

Every pattern in this repo is from real production practice (CARC/RARC adjustment codes, HIPAA-compliant claims processing, SOX-shaped audit trails) — not a tutorial reimagining of "ETL Hello World."

---

## Results at a Glance

A single end-to-end pipeline run against a realistic claims dataset:

| Metric | Value |
|---|---|
| Records processed | **27,742 healthcare claims** |
| End-to-end duration | **~99 seconds** |
| Layers traversed | CSV → src → raw → stg → dw (5 layers) |
| Validation captures | 1 row routed to `hold` for missing required field |
| Activities executed | 11 (all succeeded) |
| Audit rows written | `meta.PipelineLog` with start/end, duration, status |
| Failure path tested | ✅ — forced file-not-found error logged correctly |

---

## Architecture

### 5-Layer Data Flow

```
┌──────────────┐   ┌────────────┐   ┌─────────────┐   ┌──────────────┐   ┌──────────────┐
│  CSV (ADLS)  │──▶│   src.*    │──▶│   raw.*     │──▶│    stg.*     │──▶│    dw.*      │
│              │   │            │   │             │   │              │   │              │
│ Landing zone │   │ Typed-as-  │   │ Typed +     │   │ Validated +  │   │ Final SCD1   │
│ in blob      │   │ varchar    │   │ AlternateKey│   │ RowHash +    │   │ warehouse +  │
│ storage      │   │ mirror     │   │ + Source-   │   │ Data-        │   │ surrogate    │
│              │   │ of CSV     │   │ System tag  │   │ ChangeCode   │   │ keys         │
└──────────────┘   └────────────┘   └─────┬───────┘   └──────────────┘   └──────────────┘
                                          │                  ▲
                                          │                  │
                                          ▼                  │
                                   ┌────────────┐            │
                                   │   hold.*   │────────────┘
                                   │            │  Self-healing
                                   │ Quarantine │  reprocess on
                                   │ for errors │  next run
                                   └────────────┘
```

### Schema Design

| Schema | Purpose | Key Columns Added |
|---|---|---|
| `src` | Raw CSV landing, all `VARCHAR` to absorb dirty data | (none — mirrors CSV) |
| `raw` | Typed conversion with provenance | `AlternateKey`, `SourceSystem` |
| `stg` | Validation results + change detection signals | `IsError`, `HoldReason`, `DataChangeCode`, `RowHash`, `PipelineID` |
| `hold` | Error quarantine — sidecar table mirroring `raw` | `HoldReason`, `InsertDate` |
| `dw` | Final warehouse with surrogate keys | `ClaimSK` (identity), `IsCurrent`, `EffectiveStart/End`, `RowHash` |
| `meta` | Pipeline orchestration & audit | `TableGroup`, `SourceTableLoad`, `PipelineLog` |

### Azure Components

```
┌────────────────────────────────────────────────────────────────────┐
│  Resource Group: rg-geletaedw-dev                                  │
│                                                                    │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │  ADLS Gen2       │    │  Azure Data      │    │  Azure SQL   │  │
│  │  stgeletaedwdev  │───▶│  Factory         │───▶│  Database    │  │
│  │                  │    │  adf-geletaedw   │    │  sqldb-      │  │
│  │  landing/raw/    │    │                  │    │  geletaedw   │  │
│  │  claim/          │    │  pl_load_claims  │    │              │  │
│  │  └─ claim.csv    │    │  _edw            │    │  src→raw→stg │  │
│  └──────────────────┘    └──────────────────┘    │  →hold→dw    │  │
│                                                  │  +meta       │  │
│                                                  └──────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

---

## Key Technical Patterns

### 1. AlternateKey — deterministic business-key hashing

Every record carries a `VARBINARY(32)` AlternateKey computed from the source system and natural business key. This decouples joins from raw business-column comparisons, prevents collision on natural-key renames, and gives change-detection a single stable handle:

```sql
HASHBYTES('SHA2_256', 
    CAST(CONCAT_WS('~', @SourceSystem, ClaimID) AS NVARCHAR(75))
) AS AlternateKey
```

All `JOIN`s between layers happen on AlternateKey, never on the raw business columns. This makes the load procs resilient to source-side data quirks (e.g., trailing whitespace, case differences) being normalized differently across runs.

### 2. RowHash — content fingerprinting for change detection

A separate `MD5` hash over all material columns detects when an existing record's *contents* have changed even if its AlternateKey stayed the same:

```sql
HASHBYTES('MD5', CONCAT_WS('~', 
    MemberID, ProviderID, ServiceFromDate, ServiceToDate, 
    PaidAmount, BilledAmount, ClaimStatus, AdjudicationDate
)) AS RowHash
```

Combined with AlternateKey, this produces `DataChangeCode` in `stg`:
- `1` = new record (insert into `dw`)
- `2` = existing record with changed RowHash (update `dw`)
- `0` = unchanged (no-op — saves write I/O)

### 3. Split Extract / Stage / Load procedures

Each entity has three stored procedures, mirroring the modernized Vaya pattern:

| Proc | Role |
|---|---|
| `stg.sp_ExtractClaim` | Pulls from `src`, types-cast with `TRY_CAST`, computes AlternateKey, drops/recreates index, writes to `raw` |
| `stg.sp_StageClaim` | Reprocesses `hold` back into `raw` (self-healing), computes RowHash, runs validation, computes DataChangeCode, populates `stg` and `hold` |
| `stg.sp_LoadClaim` | Drives final warehouse — batched inserts for new rows, set-based updates for changed rows, idempotent re-runs |

Splitting these allows independent rerun of any phase, isolated error recovery, and clear ownership boundaries for each transformation responsibility.

### 4. Self-healing hold-table reprocessing

When `stg.sp_StageClaim` runs, it first checks `hold` for records that were previously quarantined. If their AlternateKeys aren't in the current `raw`, they're reinjected so validation can be retried (perhaps the upstream system has since fixed the missing field). This automated retry pattern eliminates the manual "go reprocess yesterday's errors" toil from operations.

### 5. Dynamic SQL validation framework

The list of required columns per entity is data-driven, not hard-coded into the proc:

```sql
DECLARE @requiredCols TABLE (column_name SYSNAME);
INSERT @requiredCols VALUES ('MemberID'), ('ProviderID');

-- Build a dynamic WHERE clause to flag rows with NULL in any required column,
-- then mark IsError = 1 + populate HoldReason via a cursor
```

Adding a new required field is a one-line change — not a proc rewrite.

### 6. Metadata-driven ADF orchestration

The ADF pipeline is **completely generic**. It accepts a `GroupName` parameter and iterates over whatever `meta.SourceTableLoad` returns for that group:

```
GetPipelineID (Lookup)
   ↓
SetPipelineID (Set Variable)
   ↓
GetGroupID (Lookup) → SetGroupID
   ↓
GetTablesForGroup (Lookup — returns rows from meta.SourceTableLoad)
   ↓
ForEach over each row:
   CopyCsvToSrc → ExtractToRaw → StageRawToStg → LoadStgToDw
   ↓
   ┌─ Success ─▶ LogPipelineSuccess
   └─ Failure ─▶ LogPipelineFailure
```

Adding a second entity (e.g., `Member`, `ClaimDiagnosis`) is purely a data change in `meta.SourceTableLoad` + dropping a new CSV — no pipeline code edits required.

### 7. Pipeline audit logging — success and failure paths

Every run, whether successful or failed, writes a row to `meta.PipelineLog` with start/end time, duration, status, and (on failure) which activity failed and why. The success and failure paths are wired separately in ADF, ensuring no "stuck Running" zombie rows.

---

## What This Project Demonstrates

### SQL Server / T-SQL Skills

- Multi-layer ETL architecture (5 schemas, clear responsibility boundaries)
- Advanced T-SQL: `HASHBYTES`, `CONCAT_WS`, `TRY_CAST`, dynamic SQL, cursors for procedural error handling, batched DML inside `WHILE` loops
- Transaction control (`BEGIN TRANSACTION` / `COMMIT` / `CHECKPOINT`) for large batch inserts
- SCD1 with surrogate keys and effective dating columns ready for SCD2 extension
- Idempotent stored procedures — safe to rerun without duplicating data
- Defensive coding patterns (`IF OBJECT_ID IS NOT NULL DROP`, `CREATE OR ALTER`)
- Schema-level isolation (`src`, `raw`, `stg`, `hold`, `dw`, `meta`)

### Azure Cloud Skills

- **Azure SQL Database** provisioning, configuration, security
- **Azure Data Factory** pipeline design with Lookup, Set Variable, ForEach, Copy data, Stored Procedure activities
- **ADF expressions**: `@pipeline()`, `@activity()`, `@variables()`, `@item()`, `@int()`, `@activity('X').Error.Message`
- **Linked services & parameterized datasets** for source-agnostic Copy operations
- **ADLS Gen2** integration with hierarchical folder layout (`landing/raw/<entity>/`)
- **Failure-path branching** with red/green path arrows and audit logging
- **Resource group organization** across SQL DB, ADF, Storage, and Key Vault

### Data Engineering Discipline

- Healthcare domain modeling (claims, adjudication, providers, members)
- Production-shaped error handling (quarantine + retry, not "throw and forget")
- Audit and observability built in from day one
- Metadata-driven configuration for scalability

---

## Repository Structure

```
azure-sql-adf-claims-pipeline/
├── README.md                              ← this file
├── architecture/
│   ├── pipeline-architecture.png          ← top-level Azure diagram
│   └── data-flow-5-layers.png             ← src→raw→stg→hold→dw flow
├── sql/
│   ├── 01-schemas-and-tables.sql          ← DDL for all 6 schemas
│   ├── 02-meta-orchestration.sql          ← TableGroup, SourceTableLoad, PipelineLog
│   ├── 03-stored-procedures.sql           ← all sp_Log*, sp_Extract*, sp_Stage*, sp_Load*
│   └── 04-seed-data.sql                   ← meta seeding for the CLAIMS group
├── adf/
│   └── pl_load_claims_edw.json            ← exported ADF pipeline definition
├── data/
│   └── claim.csv                          ← sample data file (small version)
└── docs/
    ├── design-decisions.md                ← why 5 layers, why split procs, etc.
    ├── run-results.md                     ← screenshots + numbers from the actual run
    └── operations-guide.md                ← how to deploy, schedule, and troubleshoot
```

---

## How to Reproduce

### Prerequisites

- Azure subscription
- Azure SQL Database (Basic tier or higher)
- Azure Data Factory instance
- Azure Data Lake Storage Gen2 account with a `landing` container
- SQL Server Management Studio (SSMS) or Azure Data Studio

### Deploy

1. **Provision resources** (resource group, SQL DB, ADF, ADLS Gen2)
2. **Run SQL deployment** in order:
   - `sql/01-schemas-and-tables.sql`
   - `sql/02-meta-orchestration.sql`
   - `sql/03-stored-procedures.sql`
   - `sql/04-seed-data.sql`
3. **Upload sample data** to `landing/raw/claim/claim.csv`
4. **Import the ADF pipeline** from `adf/pl_load_claims_edw.json`
5. **Create linked services**: one for Azure SQL DB, one for ADLS Gen2
6. **Publish all** in ADF, then **Debug** the pipeline

### Verify

```sql
-- Layer counts
SELECT 'src' [layer], COUNT(*) rows FROM src.Claim
UNION ALL SELECT 'raw',  COUNT(*) FROM raw.Claim
UNION ALL SELECT 'stg',  COUNT(*) FROM stg.Claim
UNION ALL SELECT 'hold', COUNT(*) FROM hold.Claim
UNION ALL SELECT 'dw',   COUNT(*) FROM dw.Claim;

-- Pipeline audit
SELECT TOP 5 PipelineID, Status, ErrorActivity, 
       StartTime, EndTime, 
       DATEDIFF(SECOND, StartTime, EndTime) AS DurationSec
FROM meta.PipelineLog
ORDER BY PipelineID DESC;
```

---

## Design Decisions

A few choices worth flagging for technical reviewers:

**Why 5 layers and not 3?** The `src` layer (varchar-only) absorbs every kind of dirty data without typing errors interrupting the load — a real protection against upstream surprises. The `raw` layer is the first place types matter, giving a clean boundary for "schema is now enforced." Separating `stg` from `hold` makes error recovery explicit and auditable.

**Why split procs (Extract/Stage/Load) instead of one mega-proc?** Single-responsibility per stored procedure. If a Load fails, you can re-run just Load without re-extracting. If validation logic changes, only Stage is touched. Each proc is independently testable, monitorable, and operable.

**Why AlternateKey + RowHash instead of natural keys?** Real source systems do messy things to natural keys (trailing spaces, case changes, leading zeroes). AlternateKey gives a stable join surface; RowHash gives stable change detection independent of column reordering or new column additions.

**Why metadata-driven orchestration?** The same ADF pipeline handles 1 entity or 100 entities with zero code change. New entity = new row in `meta.SourceTableLoad` + new CSV. This is how warehouses scale — declarative, not imperative.

See `docs/design-decisions.md` for the full breakdown.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute / Storage | Azure SQL Database, Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory (metadata-driven) |
| Language | T-SQL (stored procedures, dynamic SQL, batched DML) |
| Tooling | SQL Server Management Studio, Azure Portal |
| Source Control | Git / GitHub |
| Architecture | EDW (Enterprise Data Warehouse) — 5-layer with SCD1 |

---

## About

Built by **Geleta Hamda** — Senior SQL/ETL Developer with 7+ years architecting enterprise data warehouses and BI solutions for financial services and consumer goods. Owns SOX-compliant ETL pipelines processing 50M+ records nightly across 10+ business domains at Citibank, with a track record of cutting query runtimes through indexing strategy redesign, query refactoring, and execution-plan analysis. Deep expertise in T-SQL, SSIS, dimensional modeling, and performance tuning, complemented by hands-on Azure Data Factory work and active progression toward DP-203 certification. Trusted peer reviewer on ETL design patterns and SCD implementations; delivers self-service Power BI, Tableau, and SSRS reporting consumed across Finance, Risk, and Operations.

Currently pursuing an **M.S. in Business Analytics** at Grand Canyon University.

- 🔗 LinkedIn: [linkedin.com/in/geleta-hamda](https://www.linkedin.com/in/geleta-hamda/)
- 📧 Email: geletahaile7@gmail.com
- 📍 Alexandria, VA (open to remote and DC / NoVA on-site roles)

---

*This project mirrors patterns used in real production EDW systems but uses synthetic data only. No protected health information (PHI) or proprietary code is included.*

