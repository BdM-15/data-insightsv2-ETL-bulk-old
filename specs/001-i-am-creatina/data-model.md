# Data Model: i-am-creatina

Constitution v1.9.0 | Research finalized 2025-09-24

## Overview

Logical + physical entities spanning archival metadata, progress/watermark control, and multi-layer award tables across schemas `s1_raw`, `s2_interim`, `s3_processed`.

## Entity Catalog

| Entity              | Layer                                  | Purpose                                                       |
| ------------------- | -------------------------------------- | ------------------------------------------------------------- |
| ArchiveFileMetadata | Filesystem + contract schema           | Persist provenance + integrity metadata for each ZIP download |
| ProgressChunk       | meta_chunk_progress table              | Track execution status per chunk window                       |
| Watermark           | meta_refresh_watermarks table          | Persist last successful incremental boundary                  |
| PrimeAwardRaw       | s1_raw.usaspending_prime_awards_slimv2 | Raw ingested prime award transactions (54 columns + metadata) |
| SubawardRaw         | s1_raw.usaspending_subawards_v2        | Raw ingested subaward rows (all fields)                       |
| PrimeAwardInterim   | s2_interim.usaspending_prime_awards    | Cleansed + cast + enriched prime awards                       |
| SubawardInterim     | s2_interim.usaspending_subawards       | Cleansed + cast + enriched subawards                          |
| PrimeAwardProcessed | s3_processed.usaspending_prime_awards  | Deduped canonical prime award transactions                    |
| SubawardProcessed   | s3_processed.usaspending_subawards     | Deduped canonical subaward records                            |

## Cross-Cutting Conventions

- All tables include `created_at timestamptz DEFAULT now()` and `updated_at timestamptz DEFAULT now()` except pure append logs.
- Raw text preserved in `s1_raw`; casting occurs in `s2_interim` using safe coercion (NULL on invalid).
- Business keys:
  - Prime transaction: `contract_transaction_unique_key`
  - Prime award: `contract_award_unique_key`
  - Subaward: composite (prime_award_unique_key, subaward_number)
- Tie-break precedence: last_modified_date DESC, action_date DESC, ingestion_ts DESC.

## ArchiveFileMetadata (JSON sidecar)

Mirror of research schema (not stored relationally initially). Contract ensures fields & types.

Key Fields:

- job_id (uuid) — matches acquisition request
- correlation_id (uuid)
- request.date_from/date_to/date_type
- file.archive_sha256 (sha256 hex)
- chunk.window_start/window_end

## ProgressChunk

Physical: `capture_insights.meta_chunk_progress`

Columns:

- id bigserial PK
- pipeline_name text NOT NULL (e.g., 'prime_awards_historical')
- window_start date NOT NULL
- window_end date NOT NULL (exclusive upper bound semantic)
- chunk_index integer NOT NULL (sequential from 0)
- job_id uuid NOT NULL
- correlation_id uuid NOT NULL
- status text NOT NULL ('pending','in_progress','success','failed')
- rows_staged bigint NULL
- rows_deduped bigint NULL
- archive_path_rel text NULL
- archive_sha256 text NULL
- error_class text NULL
- error_message text NULL
- created_at timestamptz DEFAULT now()
- updated_at timestamptz DEFAULT now()

Unique Constraint: (pipeline_name, window_start, window_end, chunk_index)

## Watermark

Physical: `capture_insights.meta_refresh_watermarks`

Columns:

- pipeline_name text PK (e.g., 'prime_awards_monthly_incremental')
- last_modified_to timestamptz NOT NULL
- overlap_days integer NOT NULL DEFAULT 7
- updated_at timestamptz NOT NULL DEFAULT now()

## PrimeAwardRaw

Physical: `capture_insights.s1_raw.usaspending_prime_awards_slimv2`

Columns (authoritative 54):
contract_transaction_unique_key text,
contract_award_unique_key text,
action_date_fiscal_year integer,
action_date date,
parent_award_id_piid text,
award_id_piid text,
modification_number text,
federal_action_obligation numeric,
total_dollars_obligated numeric,
potential_total_value_of_award numeric,
total_outlayed_amount_for_overall_award numeric,
period_of_performance_start_date date,
period_of_performance_current_end_date date,
period_of_performance_potential_end_date date,
ordering_period_end_date date,
primary_place_of_performance_city_name text,
primary_place_of_performance_state_code text,
prime_award_base_transaction_description text,
transaction_description text,
naics_code text,
naics_description text,
product_or_service_code text,
product_or_service_code_description text,
dod_acquisition_program_description text,
parent_award_agency_name text,
awarding_sub_agency_name text,
awarding_office_name text,
funding_agency_name text,
funding_sub_agency_name text,
funding_office_name text,
recipient_name text,
recipient_uei text,
recipient_parent_name text,
recipient_parent_uei text,
solicitation_date date,
solicitation_identifier text,
solicitation_procedures text,
extent_competed text,
type_of_set_aside text,
fair_opportunity_limited_sources text,
other_than_full_and_open_competition text,
number_of_offers_received integer,
subcontracting_plan text,
government_furnished_property text,
type_of_contract_pricing text,
action_type text,
award_type text,
type_of_idc text,
idv_type text,
undefinitized_action text,
program_acronym text,
multi_year_contract text,
multiple_or_single_award_idv text,
usaspending_permalink text,
fetch_date date NOT NULL DEFAULT CURRENT_DATE,
ingestion_ts timestamptz NOT NULL DEFAULT now(),
created_at timestamptz DEFAULT now(),
updated_at timestamptz DEFAULT now()

Indexes:

- (contract_transaction_unique_key)
- (contract_award_unique_key)

## SubawardRaw

Physical: `capture_insights.s1_raw.usaspending_subawards_v2`
All API provided columns (TBD enumerated via discovery script) + metadata:

- fetch_date date DEFAULT CURRENT_DATE
- ingestion_ts timestamptz DEFAULT now()
- created_at / updated_at

Indexes:

- (prime_award_unique_key, subaward_number)

## PrimeAwardInterim

Physical: `capture_insights.s2_interim.usaspending_prime_awards`
Derived from Raw with transformations:
Additional Columns:

- fiscal_year integer (from action_date)
- fiscal_quarter integer
- semantic_description text (clean_description(canonical))
- last_modified_date timestamptz (carried if present in API; if not, patched from incremental metadata) <-- Assumption: field available or derivable; if absent we add ingestion_ts as substitute (documented).

Casts applied to numerics, dates validated. Null-safe trimming.

## SubawardInterim

Physical: `capture_insights.s2_interim.usaspending_subawards`
Analogous transformations; create `semantic_description` if applicable fields exist (if subaward descriptions present) else omit.

## PrimeAwardProcessed

Physical: `capture_insights.s3_processed.usaspending_prime_awards`
Dedup Strategy:

- UPSERT key: contract_transaction_unique_key
- Keep latest by (last_modified_date, action_date, ingestion_ts)
- Maintain original transaction grain; additional summarized award-level views can be materialized later (not in scope now)

Columns: Mirror interim + dedupe audit fields:

- source_ingestion_ts (original ingestion_ts of chosen row)
- dedupe_rank (for diagnostics, removed afterward or kept nullable)

## SubawardProcessed

Physical: `capture_insights.s3_processed.usaspending_subawards`
Dedup Strategy:

- UPSERT key: (prime_award_unique_key, subaward_number)
- Latest precedence (modification/action and ingestion_ts)

## Relationships

| From                                          | To                                       | Type                 | Notes                      |
| --------------------------------------------- | ---------------------------------------- | -------------------- | -------------------------- |
| PrimeAwardProcessed.contract_award_unique_key | SubawardProcessed.prime_award_unique_key | 1-to-many            | Enables joined analytics   |
| ProgressChunk.archive_sha256                  | ArchiveFileMetadata.archive_sha256       | reference (external) | Integrity validation       |
| Watermark.pipeline_name                       | ProgressChunk.pipeline_name              | logical              | For incremental continuity |

## Assumptions & Clarifications

- `last_modified_date` present in prime award API indirectly through incremental endpoint; if not explicitly returned in row-level CSV, we will store an `api_last_modified_date` placeholder from metadata (Phase 1 verification task).
- Subaward incremental modification field to be verified; fallback to action_date monthly windows.

## Open Items for Phase 1 Validation

1. Enumerate actual subaward field list and freeze contract.
2. Confirm presence/name of last_modified_date field in prime award CSV; if variant name encountered, adjust mapping & header guard.
3. Decide whether to store sidecar metadata subset in a relational audit table (deferred).

## Migration Ordering

1. Create meta tables (watermark, progress)
2. Create raw schema tables
3. Create util schema + clean_description() function
4. Create interim tables (initial empty or on first transform run via CTAS)
5. Create processed tables (initial on first transform)
