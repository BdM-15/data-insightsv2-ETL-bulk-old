<!--
SYNC IMPACT REPORT - Constitution Update 2025-09-24

Version Change: TEMPLATE → 1.0.0 → 1.1.0 → 1.2.0 → 1.3.0 (Added specific USASpending field requirements and API validation) → 1.4.0 (Added Data Request Structure & Ingestion; confirmed solicitation_identifier) → 1.5.0 (Added Monthly Incremental Delta-only Refresh; clarified date_type for historical vs incremental) → 1.6.0 (Added ETL Processing Guidance and Postgres description cleansing policy) → 1.7.0 (Added Storage-Conscious Processing and archiving options) → 1.8.0 (Added PostgreSQL configuration and config.py policy)

Modified Principles:
- NEW: I. Data Integrity First (NON-NEGOTIABLE) - Government data accuracy and audit trails
- UPDATED: II. Scalable ETL Architecture - Specified 66-70M record scale requirements
- NEW: III. Database-Centric Processing (NON-NEGOTIABLE) - SQL-first for large-scale transformations
- NEW: IV. API Schema Validation and Field Discovery - USASpending field verification (ADDED v1.3.0)
- RENUMBERED: V. Schema Consistency - s1_raw schema structure for USASpending data
- RENUMBERED: VI. Vector-Ready Data Preparation - SQL-based text processing for scale
- RENUMBERED: VII. Environment Isolation and Dependency Management - Python virtual environments
- RENUMBERED: VIII. Observability and Monitoring - Comprehensive logging and monitoring

Added Sections:
- Technology Standards: Enhanced API documentation links, procurement-only focus
- Data Extraction Specifications: Prime award fields, full subaward extraction (ADDED v1.3.0)
- Data Request Structure & Ingestion (ADDED v1.4.0)
- Monthly Incremental Delta-only Refresh (ADDED v1.5.0)
- ETL Processing Guidance and Description Cleansing (ADDED v1.6.0)
- Storage-Conscious Processing (ADDED v1.7.0)
- PostgreSQL Database Configuration and Configuration Module Policy (ADDED v1.8.0)

Templates Requiring Updates:
✅ plan-template.md - Updated (Constitution Check section aligned with data pipeline principles)
✅ spec-template.md - Updated (Added Data Pipeline Requirements section)  
✅ tasks-template.md - Updated (ETL-specific task phases and categories)
⚠ templates/*.md - Should update with field validation and API schema discovery requirements
✅ commands/*.md - Not applicable (no command files found)

Follow-up TODOs: 
- Create API schema discovery task to verify solicitation_identifier field name
- Update templates to include field validation steps
--># Data Insights v2 ETL Bulk Pipeline Constitution

## Core Principles

### I. Data Integrity First (NON-NEGOTIABLE)

All data ingestion from USASpending API must maintain complete fidelity and audit trails. Schema validation MUST occur before insertion into PostgreSQL; Data transformations MUST be reversible and logged; No silent data loss or corruption permitted; Source data lineage tracking required for all records.

**Rationale**: Government procurement data accuracy is critical for compliance and decision-making. Any data corruption undermines the entire analytical pipeline.

### II. Scalable ETL Architecture

ETL processes MUST handle bulk data operations efficiently for 66-70 million record datasets. Batch processing preferred over real-time for large datasets; Incremental updates supported for delta processing; Memory-efficient streaming for large API responses; PostgreSQL connection pooling and transaction management enforced.

**Rationale**: USASpending bulk data contains 66-70 million records. Architecture must scale without performance degradation or memory exhaustion.

### III. Database-Centric Processing (NON-NEGOTIABLE)

All data transformation and cleanup operations MUST be performed in SQL within PostgreSQL for datasets exceeding 1 million records. Python/pandas reserved for small-scale operations, API extraction, and orchestration only; Bulk transformations use SQL queries, stored procedures, or database functions; Memory-intensive operations (joins, aggregations, deduplication) executed server-side; ETL logic documented in version-controlled SQL scripts.

**Rationale**: With 66-70 million records, in-memory processing via pandas would cause memory exhaustion and performance bottlenecks. PostgreSQL's query optimizer and disk-based operations are designed for this scale.

### IV. API Schema Validation and Field Discovery

All USASpending API field names MUST be verified before implementation through schema inspection. Field discovery process required for uncertain field names (e.g., solicitation_identifier); API response schema documented and version-controlled; Field mapping validated against actual API responses; Missing or renamed fields identified and documented before pipeline execution.

**Rationale**: Government APIs may change field names or structure. Pre-validation prevents extraction failures and ensures complete data capture.

### V. Schema Consistency

Database schema `s1_raw` MUST maintain strict structure for tables `usaspending_prime_awards_slimv2` and `usaspending_subawards_v2`. Column definitions match API specification exactly; Data types enforced at database level; Foreign key relationships maintained; Version-controlled schema migrations required.

**Rationale**: Consistent schema enables reliable downstream analytics and ensures data quality across the entire pipeline.

### VI. Vector-Ready Data Preparation

All textual data MUST be prepared for pgvector integration using SQL-based text processing for large datasets. Text fields normalized and cleaned via SQL functions before vectorization; Embedding generation pipeline documented and versioned; Vector indexes optimized for semantic search performance; Metadata preserved for vector-to-source traceability via SQL joins.

**Rationale**: Semantic search capabilities depend on high-quality text processing and efficient vector operations at scale.

### VII. Environment Isolation and Dependency Management

All Python development MUST use virtual environments for dependency isolation. Virtual environment activation required before any development or execution; Requirements files maintained and version-pinned; No global Python package installations for project dependencies; Environment reproducibility ensured across development, staging, and production.

**Rationale**: ETL pipelines require specific library versions for data consistency. Virtual environments prevent version conflicts and ensure reproducible deployments.

### VIII. Observability and Monitoring

Every ETL operation MUST be logged, monitored, and traceable. Structured logging with correlation IDs; Performance metrics for API calls, database operations, and vectorization; Error handling with detailed context; Health checks for all pipeline components.

**Rationale**: Data pipelines are complex systems requiring comprehensive monitoring to ensure reliability and enable rapid troubleshooting.

## Technology Standards

**Programming Language**: Python 3.9+ for API extraction, orchestration, and small-scale operations
**Data Processing**: SQL within PostgreSQL for all transformations exceeding 1M records
**Environment Management**: Virtual environments (venv or conda) MUST be used for dependency isolation
**Database**: PostgreSQL with pgvector extension for vector operations and bulk processing
**Schema**: Database `capture_insights` with schemas `s1_raw` (raw), `s2_interim` (cleansing), and `s3_processed` (analytics); additional `util`/`public` as needed
**API Integration**: USASpending Bulk Award API as primary data source (procurement awards only, no grants)
**API Documentation**: https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md
**Reference Implementation**: Follow patterns established in https://github.com/BdM-15/Data_Insights/tree/main/src/backend/data/data_acquisition
**Vector Operations**: pgvector for semantic search and similarity operations
**Data Formats**: JSON for API responses, normalized relational storage in PostgreSQL
**Dependency Management**: requirements.txt for production, requirements-dev.txt for development dependencies
**SQL Management**: Version-controlled SQL scripts for transformations, stored procedures for complex operations

**Data Directory**: Local archive at `data/downloads/usaspending/{prime|sub}/YYYY/MM/` (configurable via `DOWNLOAD_DIR`); logs at `logs/`

## PostgreSQL Database Configuration (Authoritative)

The following environment variables define how all code connects to PostgreSQL. These defaults are for local development only. In production, values MUST come from a secret store and must not be committed to the repository.

- PG_USER=postgres
- PG_PASSWORD=admin
- PG_HOST=localhost
- PG_PORT=5432
- PG_DBNAME=capture_insights
- PG_SCHEMA=public

Policy and usage:

- Single source of truth: A central module `etl/config.py` SHALL read configuration from environment variables (with optional `.env` for local development) and expose typed, validated settings. No other module reads env directly.
- Connection string: Derived as `postgresql://PG_USER:PG_PASSWORD@PG_HOST:PG_PORT/PG_DBNAME` and never hard-coded.
- Secrets handling: `.env` files MAY be used locally but MUST be .gitignored. In non-local environments, rely on process env or managed secret providers; never print `PG_PASSWORD` in logs.
- Schema usage: All SQL MUST be schema-qualified. The pipeline operates in the `capture_insights` database and uses schemas `s1_raw`, `s2_interim`, `s3_processed` explicitly. `PG_SCHEMA` sets the default search_path for utilities but SHALL NOT replace the explicit use of `s1_raw/s2_interim/s3_processed` in SQL.
- Migrations: If required schemas are missing, migrations will create them idempotently at startup. No destructive changes occur without explicit migration scripts.

Note: This constitution defines configuration names and defaults only. Implementation of `etl/config.py` is deferred to the build phase and will include input validation, defaulting, and helpful error messages for missing critical values.

## Project Structure & Modularity

Design for maintainability and minimal coupling. While configuration details are out of scope for the constitution phase, the following structure and principles are REQUIRED to keep the codebase sustainable and simple:

- Folders (top-level):

  - `data/` — Archived downloads and checksums (read-only after load)
  - `etl/` — Python modules by concern (acquisition/, staging/, sql/, utils/, checks/)
  - `sql/` — Version-controlled SQL (00_s1_raw/, 10_s2_interim/, 20_s3_processed/)
  - `logs/` — Structured logs
  - `docs/` — Runbooks and data contracts

- Python modules (examples):

  - `etl/acquisition/usaspending_bulk.py` — Request, status, download, unzip, header verification
  - `etl/staging/loader.py` — COPY loaders to s1_raw, small helpers only
  - `etl/sql/runner.py` — Applies ordered SQL migrations for s2/s3
  - `etl/utils/io.py` — File ops, checksums, sidecar metadata
  - `etl/checks/schema_guard.py` — Header guards (assert required fields)

- Execution scripts:

  - `scripts/fetch_historical.py` — Drives historical backfill via acquisition + staging
  - `scripts/fetch_incremental.py` — Runs rolling window updates
  - `scripts/run_transforms.py` — Applies s2/s3 SQL

- Contract boundaries:
  - Acquisition returns file paths + metadata only (no DB writes)
  - Staging writes raw rows only (no heavy transforms)
  - Transform SQL does dedup, casts, and joins (SQL-first)

## Simplicity & Code Bloat Guardrails

Keep the solution effective yet simple. The following guardrails are MANDATORY:

- Prefer composition over inheritance; small, single-purpose functions.
- No class frameworks unless necessary; plain functions + modules preferred.
- Centralize retries/backoff once; reuse across requests and polling.
- No pandas for large loads; use COPY; only use DataFrame for tiny diagnostics.
- Avoid duplicate file I/O logic; consolidate in `etl/utils/io.py`.
- Keep configuration access localized (single config module later); pass primitives into functions.
- Fail fast with precise error messages; avoid nested exception layers.
- Document only what’s needed: module docstring + 1-2 line function docstrings.

## Data Extraction Specifications

### Prime Awards Field Requirements (usaspending_prime_awards_slimv2)

**Data Type**: Procurement contracts only (no grants)
**Required Fields**: Exactly 54 fields specified below MUST be extracted and stored
**Field Verification**: All fields confirmed present via API schema discovery (including `solicitation_identifier`).

**TARGET_FIELDS** (authoritative):

```
contract_transaction_unique_key, contract_award_unique_key, action_date_fiscal_year,
action_date, parent_award_id_piid, award_id_piid, modification_number,
federal_action_obligation, total_dollars_obligated, potential_total_value_of_award,
total_outlayed_amount_for_overall_award, period_of_performance_start_date,
period_of_performance_current_end_date, period_of_performance_potential_end_date,
ordering_period_end_date, primary_place_of_performance_city_name,
primary_place_of_performance_state_code, prime_award_base_transaction_description,
transaction_description, naics_code, naics_description, product_or_service_code,
product_or_service_code_description, dod_acquisition_program_description,
parent_award_agency_name, awarding_sub_agency_name, awarding_office_name,
funding_agency_name, funding_sub_agency_name, funding_office_name, recipient_name,
recipient_uei, recipient_parent_name, recipient_parent_uei, solicitation_date,
solicitation_identifier, solicitation_procedures, extent_competed, type_of_set_aside,
fair_opportunity_limited_sources, other_than_full_and_open_competition,
number_of_offers_received, subcontracting_plan, government_furnished_property,
type_of_contract_pricing, action_type, award_type, type_of_idc, idv_type,
undefinitized_action, program_acronym, multi_year_contract,
multiple_or_single_award_idv, usaspending_permalink
```

### Subawards Field Requirements (usaspending_subawards_v2)

**Data Type**: Procurement subawards only (no grants)
**Required Fields**: ALL available fields from USASpending API
**Extraction Strategy**: Full schema extraction to capture complete subaward data structure

## Data Request Structure & Ingestion (NON-NEGOTIABLE)

All USASpending data requests and ingestion flows MUST adhere to the structure below. The Data Insights repo patterns inform this section, but this constitution is authoritative and adapted to this project’s paths and policies.

1. Request Windows and Chunking

- Historical backfill: chunk by date range using `action_date` with `CHUNK_DAYS = 7` by default. Process sequential chunks between configured `HISTORICAL_START_DATE` and `HISTORICAL_END_DATE`.
- Current incremental: rolling window using `CURRENT_DAYS_LOOKBACK = 2` (configurable) ending at now; scheduled daily.
- Date type: Historical uses `date_type = "action_date"`; Incremental uses `date_type = "last_modified_date"` (see Monthly Incremental Delta-only Refresh).

2. Filters (Procurement-only)

- Prime awards: `prime_award_types = ["A","B","C","D","IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E"]`.
- Subawards: `sub_award_types = ["procurement"]`.
- Agencies: request level `{"type": "awarding", "tier": "toptier", "name": "All"}` (no agency restriction by default).

3. Request/Status/Download Protocol

- Endpoint: `https://api.usaspending.gov/api/v2/bulk_download/awards/` via POST.
- Always capture `status_url` and `file_url` from response; poll `status_url` until `status = finished` with `MAX_WAIT_SECONDS` guard and exponential backoff retries (minimum 3 attempts).
- On success, download the ZIP from `file_url`.

4. Archiving and File Handling (Project-adapted)

- Archive directory: `data/downloads/usaspending/{prime|sub}/YYYY/MM/` under the repo root by default (override via `DOWNLOAD_DIR`).
- Preferred: Persist the `.zip` only and extract to a temporary working directory for streaming ingestion (see Storage-Conscious Processing). Optionally persist extracted `.csv` only when `PERSIST_EXTRACTED_CSV=true`.
- Compute and store SHA256 checksums and a small JSON sidecar with metadata: request payload, status_url, file_url, byte sizes, checksum, job id, timestamps.
- Filename convention: `{Scope}_{Type}_{YYYY-MM-DD}_H{HH}M{MM}S{SS}.zip` (use server-provided names when available). Never overwrite existing files; ensure idempotency.
- Retention: keep files for `ARCHIVE_RETENTION_DAYS` (default 90) with a scheduled pruning policy; never delete files for in-flight jobs.

5. Header Verification and Schema Guards

- After download, unzip and parse the CSV header. Assert presence of ALL required prime fields listed above—specifically `solicitation_identifier`, `solicitation_date`, `solicitation_procedures`.
- If any required field is missing or renamed, abort the load, log a high-severity event, and open a remediation task before proceeding.

6. Database Load Pipeline (SQL-first)

- Stage: Load CSVs to `capture_insights.s1_raw` tables (`usaspending_prime_awards_slimv2`, `usaspending_subawards_v2`) using COPY/streaming; store raw text for all columns plus `created_at`, `updated_at`, `fetch_date`.
- Transform: Cast types and deduplicate in `s2_interim` using version-controlled SQL. Deduplication keys and normalization are SQL-only (no large pandas processing).
- Processed: Promote to `s3_processed` for analytics after quality checks.
- Transactions: Use transactional loads; index critical keys (`contract_transaction_unique_key`, `contract_award_unique_key`, `prime_award_unique_key`).

7. Reliability and Retries

- Use exponential backoff (e.g., Tenacity) for POST and status polling; fail-fast with clear, actionable logs on repeated errors.
- Resume support: Progress tables track last successful chunk `start_date`, `end_date`, records count, status, and file paths.

8. Observability and Audit

- Structured logs to `logs/` with correlation ids; log payload hashes (not secrets), response codes, durations, sizes, and record counts.
- Audit trail links each database load to archived files via checksum and file path.

9. Configuration and Secrets

- All operational values driven by config/env: `DOWNLOAD_DIR`, `CHUNK_DAYS`, `CURRENT_DAYS_LOOKBACK`, `MAX_WAIT_SECONDS`, retry counts, DB connection settings.
- No hard-coded local paths from external repos; this project’s defaults govern.

## Monthly Incremental Delta-only Refresh (NON-NEGOTIABLE)

The production pipeline MUST run delta-only monthly refreshes, never full reloads, to capture new and corrected records with minimal cost and risk.

1. Scope and Schedule

- Cadence: Run on the 1st–3rd day of each month and target the prior calendar month by default.
- Scope: Procurement prime awards and procurement subawards only.

2. Change Detection Field

- Use `date_type = "last_modified_date"` for monthly incremental runs so that both new and corrected records are included.
- Historical backfills continue to use `date_type = "action_date"` to reproduce the original event timeline.

3. Watermarking and Overlap

- Maintain a durable watermark table (e.g., `capture_insights.meta_refresh_watermarks`) with columns: `pipeline_name`, `last_modified_to`, `overlap_days`, `updated_at`.
- For each run, compute the window as: `[max(last_modified_to - overlap_days, configured_start) , now()]`.
- Default `overlap_days = 7` to account for late-arriving or re-processed transactions.

4. Request Windows

- Submit monthly windows aligned to calendar boundaries for auditability (e.g., 2025-08-01 → 2025-09-01) with the overlap applied to the lower bound.
- If API limits require smaller chunks, subdivide the monthly window into weekly chunks while preserving the same `[from, to)` bounds semantics.

5. Idempotency and Archiving

- All downloads must be archived under `data/downloads/...` with checksums and a metadata sidecar including the computed window and watermark values.
- Before processing, detect duplicates by checksum and exact window; skip re-processing unless explicitly forced.

6. Load and Upsert Semantics

- Stage all rows to `capture_insights.s1_raw.*` via COPY. Do not attempt merge logic in Python.
- In `s2_interim`, deduplicate by stable business keys:
  - Prime awards: `contract_transaction_unique_key` (transaction grain) and `contract_award_unique_key` (award grain).
  - Keep the latest row by `last_modified_date` for each key. If ties, prefer the row with the most recent `action_date`; as a final tie-breaker, prefer the latest ingestion timestamp.
- In `s3_processed`, ensure downstream tables are updated via SQL MERGE/UPSERT patterns keyed by the same business keys.

7. Subawards Policy

- Use the equivalent API-provided modification field for subawards incremental windows. If the bulk API lacks `last_modified_date` for subawards, use monthly `action_date` windows with the same overlap policy and rely on SQL-side dedupe by business keys.

8. Exceptions (Tightly Controlled)

- Full reloads are PROHIBITED in production. A one-time rehydration may be executed only with architectural approval, a runbook, and pre/post row-count and checksum attestations. Such runs must write to separate job IDs and must not overwrite archives.

9. Watermark Advancement

- Advance the `last_modified_to` watermark only after successful completion of: file archival, COPY into `s1_raw`, and s2/s3 SQL merges. On failure, do not advance and alert.

10. Observability

- Every monthly run must log: computed window, overlap, request payload hash, response sizes, row counts staged, upsert counts, and elapsed times for acquisition, load, and SQL phases.

## ETL Processing Guidance (ADAPTED, SQL-first, SIMPLE)

Use the linked Data Insights repo’s data_processing patterns as guidance, but keep this pipeline simple, SQL-first, and focused on procurement awards. The following layered flow is REQUIRED:

1. s1_raw (Acquisition/Staging)

- Archive raw USASpending downloads and load CSVs via COPY to `capture_insights.s1_raw.usaspending_*` preserving source column names and raw text.
- Do not mutate or cleanse; only add `created_at`, `updated_at`, `fetch_date` metadata.

2. s2_interim (Cleansing & Standardization)

- Implement cleansing using pure SQL in Postgres. Key operations, minimally:
  - Trim and normalize text (UPPER/TRIM where appropriate);
  - Standardize agency names and code formats (e.g., NAICS to 6 chars, strip trailing “.0”);
  - Coerce numeric/text fields safely using regex guards;
  - Derive helpful attributes (fiscal year/quarter) from `action_date`;
  - Apply Description Cleansing Policy (below) to produce a canonical `semantic_description` while preserving originals.
- Output canonicalized tables: `s2_interim.usaspending_prime_awards` and `s2_interim.usaspending_subawards` with the same core columns as raw plus derived fields.

3. s3_processed (Deduplication & Canonicalization)

- Deduplicate and merge updates using business keys in SQL:
  - Prime awards: dedupe by `contract_transaction_unique_key` and maintain latest by `last_modified_date` (then `action_date`, then ingestion timestamp) per Monthly Incremental policy.
  - Subawards: dedupe by `prime_award_unique_key, subaward_number, subaward_action_date, subaward_amount` or equivalent stable keys.
- Canonicalize results into `s3_processed.usaspending_*` tables for analytics. Create only essential indexes; avoid over-indexing.
- Later, create materialized views or filter tables if needed for performance, but do not add complex application-specific tables here.

4. Logging and Idempotency

- Follow the repo’s robust logging pattern but keep it lightweight: module logger + rotating file handler to `logs/`.
- Ensure each step is idempotent and restartable; never delete archives; SQL steps guarded with transactions.

## Storage-Conscious Processing (NON-NEGOTIABLE FOR LIMITED DISK)

We MUST design for <200GB free disk with up to ~75GB new data. The pipeline SHALL minimize peak disk usage and prevent mid-ETL aborts due to storage constraints.

1. Streaming and Ephemeral Extracts

- Archive ZIPs permanently; extract CSVs to an OS temp directory or `data/tmp/` and delete immediately after each chunk load.
- Use COPY FROM STDIN or server-side COPY when feasible; otherwise stream local CSVs chunk-by-chunk with bounded temp space.
- Config: `PERSIST_EXTRACTED_CSV=false` by default; when true, retain for debugging with retention policy.

2. Chunked Staging and CTAS Batches

- Process one API chunk (e.g., 7 days) end-to-end before starting the next to cap temporary growth.
- For s2*interim, use per-chunk CTAS into a small temporary table `s2_interim.tmp_prime_awards*<chunk_id>`; immediately MERGE/UPSERT into the target table and drop the temp table.
- Avoid full table rewrites; use INSERT ... ON CONFLICT (or MERGE) into s2/s3 targets keyed by business keys.

3. UNLOGGED + Minimal Indexing During Load

- For intermediary per-chunk tables, declare UNLOGGED to reduce WAL and disk churn when acceptable (not for critical persistent tables).
- Delay heavy indexes until after a batch is finished, and only on final s3_processed tables. Maintain only primary keys/unique constraints needed for UPSERTs during ingestion.

4. Vacuum/Analyze and Checkpointing

- After each chunk commit, run targeted ANALYZE on affected tables; schedule VACUUM (not FULL) on large tables during low activity.
- Persist a progress record per chunk so the pipeline resumes without repeating already-loaded work.

5. Concurrency Limits and Disk Guardrails

- Limit parallel chunks to prevent disk spikes: `MAX_CONCURRENT_CHUNKS=1..2` based on free space.
- Enforce free-space checks: before extraction/loading, assert `MIN_FREE_GB` (e.g., 40GB). If below threshold, backoff and alert.
- Target peak working set `TARGET_PEAK_GB` (e.g., 100GB) for sum of temp CSV + temp tables + WAL.

6. Partitioning Strategy (Optional, Adopt When Needed)

- If tables grow large, consider range/month partitions on `action_date` in s2/s3 to bound maintenance and accelerate chunk merges.
- Create partitions on-demand per month being processed; detach/archive old partitions without copying whole tables.

7. File Retention and Pruning

- ZIPs: Retain per retention policy (default 90 days). CSVs: Ephemeral by default; when persisted, prune by freshest N months or size quota.

8. Observability

- Log per-chunk sizes (ZIP bytes, extracted CSV bytes, rows loaded), WAL growth estimates, and current free disk.
- Surface alerts when free disk falls below `MIN_FREE_GB` or when temp working set exceeds `TARGET_PEAK_GB`.

### Description Cleansing Policy (PostgreSQL)

Goal: Remove distracting CAR/IGF-like tags and boilerplate markers from description fields while keeping semantic meaning. We will create a Postgres function and apply it in s2_interim to produce `semantic_description` alongside the original fields `prime_award_base_transaction_description` and `transaction_description`.

1. Canonical function (to be placed in version-controlled SQL migrations):

```
-- Schema: capture_insights.util (or capture_insights.public if util schema is not used)
CREATE OR REPLACE FUNCTION capture_insights.util.clean_description(input text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          COALESCE(input, ''),
          -- Remove common IGF/CAR prefixes like 'IGF::CL:IGF', 'IGF::OT:IGF', 'IGF:IGF', etc.
          '(^|\s)(IGF(?:::[A-Z]{1,3}){0,3}:?IGF)\b[:\-\s]*',
          ' ', 'gi'
        ),
        -- Remove bracketed or parenthetical boilerplate tags like [IGF], (IGF), {IGF}
        '(\[\s*IGF\s*\]|\(\s*IGF\s*\)|\{\s*IGF\s*\})',
        ' ', 'gi'
      ),
      -- Collapse multiple spaces and stray punctuation artifacts
      '\s{2,}', ' ', 'g'
    )
    , ''
  );
$$;
```

2. Application in s2_interim SQL:

- Add a column `semantic_description` as a cleaned, human-friendly blend of the two description fields, preferring transaction-specific when present:

```
semantic_description AS capture_insights.util.clean_description(
  COALESCE(NULLIF(transaction_description, ''), prime_award_base_transaction_description)
)
```

3. Preservation and traceability:

- Do not overwrite raw description fields; keep them unchanged in `s1_raw` and as original in `s2_interim`.
- Downstream vectorization should reference `semantic_description` while joining back to originals for context.

4. Extensibility:

- If new noisy patterns are found, extend the REGEXP_REPLACE chain in the function via a migration. Maintain test cases for added patterns.

## Development Workflow

**Environment Setup**: Virtual environment creation and activation documented; Requirements installation scripted; Environment variables managed via .env files
**Testing Requirements**: Unit tests for data transformation logic using pytest; Integration tests for database operations; End-to-end tests for complete ETL workflows; Virtual environment isolation for test execution
**Code Review**: All ETL scripts must be reviewed for data integrity and performance; Database schema changes require architectural review; Python code style enforced via linting (black, flake8)
**Deployment**: Staging environment required for testing with sample data before production deployment; Production deployments use same virtual environment configuration
**Documentation**: API integration patterns documented; Database schema versioned; Vector embedding procedures documented; Virtual environment setup instructions maintained

## Governance

This constitution governs all ETL pipeline development and data operations. All code changes must demonstrate compliance with data integrity principles. Schema migrations require approval and testing. Vector embedding changes must include performance impact analysis. Emergency data fixes require post-incident review and constitution compliance verification.

**Amendment Process**: Constitution changes require documentation of impact on existing pipelines, approval from data architecture review, and migration plan for existing data.

**Compliance Review**: All pull requests must verify adherence to data integrity, schema consistency, and observability requirements. Performance benchmarks must be maintained or improved.

**Version**: 1.8.0 | **Ratified**: 2025-09-24 | **Last Amended**: 2025-09-24
