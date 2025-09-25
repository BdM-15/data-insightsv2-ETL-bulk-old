# Tasks: i-am-creatina (USASpending Bulk ETL Foundation)

Input: Design documents from `specs/001-i-am-creatina/`
Prerequisites: `plan.md` (required), `research.md`, `data-model.md`, `contracts/`, `quickstart.md`

## Generation Context

- Feature directory: `specs/001-i-am-creatina`
- Entities (data-model): ArchiveFileMetadata, ProgressChunk, Watermark, PrimeAwardRaw, SubawardRaw, PrimeAwardInterim, SubawardInterim, PrimeAwardProcessed, SubawardProcessed
- Contracts: `contracts/archive_metadata.schema.json`, `contracts/progress_chunk_table.sql`, `contracts/watermark_table.sql`, `contracts/env_config.md`
- User scenarios (spec): Historical backfill, monthly incremental, daily incremental, fail-fast stop, semantic description availability, storage guardrails.

## Execution Flow (tasks phase)

1. Setup & scaffolding
2. Contract & integration tests (TDD) — MUST fail initially
3. Core models & utilities (raw/interim/processed schemas, config, cleansing)
4. Acquisition + staging implementation
5. Transformation + dedupe + lineage
6. Incremental + watermark logic
7. Observability, metrics, retention, guardrails
8. Polish (performance, docs, cleanup)

## Format

[ID] [P?] Description — includes concrete file paths. [P] indicates safe parallel execution (different files, no order dependency).

## Phase 3.1: Setup & Environment

- [x] T001 Create base source tree: `etl/acquisition/`, `etl/staging/`, `etl/sql/`, `etl/utils/`, `etl/checks/`, `scripts/`, `tests/` (skip if already exists)
- [x] T002 Add `etl/config.py` implementing env var loading per `contracts/env_config.md` with validation rules
- [x] T003 [P] Add `etl/utils/logging.py` JSON logger factory (correlation_id support)
- [x] T004 [P] Add `etl/utils/disk.py` free space + guard check util (MIN_FREE_GB, TARGET_PEAK_GB)
- [x] T005 [P] Add `etl/utils/hash.py` streaming SHA256 helper
- [x] T006 Initialize `tests/conftest.py` with fixtures: temp_download_dir, fake_config, pg_connection (placeholder)

## Phase 3.2: Contract & Schema / TDD Tests (Must Write Before Impl)

- [x] T007 Contract test archive metadata schema: `tests/contracts/test_archive_metadata_schema.py` validating example JSON against `contracts/archive_metadata.schema.json`
- [x] T008 Contract test progress chunk DDL expectations: reflect columns/types from `contracts/progress_chunk_table.sql` after applying SQL (uses information_schema)
- [x] T009 Contract test watermark table DDL expectations referencing `contracts/watermark_table.sql`
- [x] T010 Env config validation tests: `tests/config/test_env_config.py` covering defaults, required, and bounds (MIN_FREE_GB < TARGET_PEAK_GB, ranges)
- [x] T011 Historical backfill integration test skeleton: `tests/integration/test_historical_flow.py` (mocks API, asserts chunk progress rows written sequentially, fail-fast not triggered)
- [x] T012 Incremental daily integration test skeleton: `tests/integration/test_daily_incremental_flow.py` (watermark advance + overlap)
- [x] T013 Archive processing integration test skeleton: `tests/integration/test_archive_processing_flow.py` (extract, validate, load)
- [x] T014 Database schema validation test skeleton: `tests/integration/test_database_schema_validation.py` (schema management)
- [x] T015 End-to-end pipeline test skeleton: `tests/integration/test_end_to_end_pipeline.py` (complete flow)
- [x] T016 Cleansing function unit test: `tests/cleaning/test_clean_description.py` for IGF pattern removal

## Phase 3.3: Core Models / SQL / Utilities

- [x] T017 Implement SQL function `util.clean_description` via migration script `sql/util/010_clean_description.sql`
- [x] T018 Create meta tables migration script `sql/util/020_meta_tables.sql` aggregating `progress_chunk_table.sql` & `watermark_table.sql`
- [x] T019 Create raw table DDL script for prime awards 54 columns: `sql/00_s1_raw/010_prime_awards_raw.sql`
- [x] T020 Create raw table DDL script for subawards: `sql/00_s1_raw/020_subawards_raw.sql` (placeholder columns to be enumerated via discovery follow-up)
- [x] T021 Create interim table build script (CTAS or incremental) for prime awards: `sql/10_s2_interim/010_prime_awards_interim.sql`
- [x] T022 Create interim table build script for subawards: `sql/10_s2_interim/020_subawards_interim.sql`
- [x] **T023**: Create processed table DDL - prime awards
  - Create `sql/20_s3_processed/010_prime_awards_processed.sql`
  - Deduplication logic with canonical record tracking
  - UPSERT function for incremental processing
- [x] T024 Create processed table build script for subawards dedupe: `sql/20_s3_processed/020_subawards_processed.sql`
- [x] T025 Create SQL test for deduplication conflict resolution: `sql/util/test_deduplication_conflicts.sql`

## Phase 3.4: Acquisition & Staging Implementation

- [x] T026 Implement USASpending API client: `etl/acquisition/api_client.py` (POST job, poll, download streaming, retries)
- [x] T027 Implement archive manager: `etl/acquisition/archive.py` (path planning, write ZIP, compute hash, write sidecar JSON)
- [x] T028 Implement header validator: `etl/acquisition/headers.py` (compare to expected 54 prime headers list)
- [x] T029 Implement chunk planner: `etl/acquisition/chunk_planner.py` (historical windows, incremental windows + overlap)
- [x] T030 Implement disk guard pre-flight: integrate `disk.py` in planner or runner
- [x] T031 Implement raw loader: `etl/staging/raw_loader.py` (stream CSV -> COPY into `s1_raw` tables)
- [x] T032 Implement progress recorder: `etl/staging/progress.py` (insert/update rows in meta_chunk_progress)
- [x] T033 Implement fail-fast controller: `etl/staging/fail_fast.py` (raise & halt logic)
- [x] T034 Implement watermark manager: `etl/staging/watermark.py` (read/update overlaps, monthly vs daily)

## Phase 3.5: Transform & Processing

- [x] T035 Implement cleansing & interim transform runner: `etl/sql/transform_interim.py` (exec SQL scripts sequentially)
- [ ] T036 Implement processed layer merge + dedupe: `etl/sql/transform_processed.py`
- [ ] T037 Implement semantic description generation integration: extend interim SQL to call `clean_description`
- [ ] T038 Implement incremental merge logic (UPSERT) for processed layers: `etl/staging/merge.py`
- [ ] T039 Implement duplicate detection diagnostics query tool: `etl/checks/dupe_diagnostics.py`

## Phase 3.6: Incremental & Orchestration Scripts

- [ ] T040 Script: historical backfill entrypoint `scripts/fetch_historical.py` (ties planner, client, loader, progress, transforms)
- [ ] T041 Script: daily incremental entrypoint `scripts/fetch_incremental.py`
- [ ] T042 Script: monthly incremental mode extension in `scripts/fetch_incremental.py` (mode flag + window calc)
- [ ] T043 Script: transforms only runner `scripts/run_transforms.py`
- [ ] T044 Script: dry-run planner `scripts/dry_run.py` (estimates windows + disk)

## Phase 3.7: Observability, Metrics, Retention

- [ ] T045 Implement metrics event writer: `etl/utils/metrics.py` (append JSON lines)
- [ ] T046 Integrate metrics in acquisition + staging + transforms
- [ ] T047 Implement archive retention pruner: `etl/acquisition/retention.py` (delete old ZIPs > ARCHIVE_RETENTION_DAYS)
- [ ] T048 Implement checksum dedupe skip: integrate in archive manager
- [ ] T049 Implement structured error classification mapping: `etl/utils/errors.py`
- [ ] T050 Implement logging correlation & context injection (logger adapter or contextvar)

## Phase 3.8: Validation & Polish

- [ ] T051 Performance tests: `tests/performance/test_copy_vs_insert.py` (ensure COPY wins)
- [ ] T052 Data quality assertion tests: `tests/quality/test_required_fields_present.py`
- [ ] T053 Watermark continuity test: `tests/quality/test_watermark_continuity.py`
- [ ] T054 Retention pruner test: `tests/quality/test_retention.py`
- [ ] T055 Documentation update: enrich `quickstart.md` with examples of dry-run + restart procedure
- [ ] T056 Add README snippet or new `docs/restart.md` describing fail-fast restart process
- [ ] T057 Add CI workflow draft (future) placeholder `.github/workflows/ci.yml` (lint + tests matrix Python 3.13)
- [ ] T058 Add lint/format config: `pyproject.toml` updates (ruff, black) + `tests/style/test_import_order.py` optional
- [ ] T059 Add security minimal scan script placeholder `scripts/security_check.py` (dependency & basic license scan stub)
- [ ] T060 Final end-to-end rehearsal test: `tests/integration/test_full_rehearsal.py` (small synthetic dataset)

## Dependencies Summary

- T001 precedes all file-creation tasks.
- Config (T002) required before tests using env (T010, integration tests T011-T015).
- Logger/hash/disk utils (T003-T005) can proceed in parallel once tree exists.
- Contract tests (T007-T009) precede corresponding implementations (archive manager, progress recorder, watermark manager).
- Integration tests (T011-T015) precede acquisition + staging implementation tasks (T026+).
- SQL function & meta tables (T017-T018) precede transform scripts (T035+).
- Raw/interim/processed DDL (T019-T024) precede loaders & transforms (T031, T035, T036).
- Merge & dedupe (T036-T038) depend on interim + processed DDL.
- Orchestration scripts (T040-T044) depend on acquisition/staging + transforms.
- Metrics/retention (T045-T050) integrate after core acquisition/staging exists.
- Polish & performance tasks (T051+) run after functional core proven.

## Parallel Execution Guidance Examples

```
# Example: After T001 done, run in parallel:
T003, T004, T005, T006

# After contract tests written (T007-T016) and failing, implement core in parallel where safe:
T026 (api_client.py) | T027 (archive.py) | T028 (headers.py)

# SQL DDL tasks can run together:
T019, T020, T021, T022, T023, T024
```

## Validation Checklist

- [ ] Every contract file has at least one test task (archive metadata, progress chunk, watermark, env config)
- [ ] Every entity has tasks for representation or DDL
- [ ] Tests precede implementation (T007-T016 before T026+ and T035+)
- [ ] Parallel [P] markers only where file independence exists (initial util tasks)
- [ ] All tasks have explicit file paths
- [ ] Dedupe + watermark logic covered by tests (T011-T016, T053)

## Notes

- Subaward raw table column enumeration (T020) flagged placeholder—requires discovery script future task (could add T061 if needed later)
- Vector embedding pipeline intentionally excluded per spec; only semantic_description preparation is in-scope
- Python 3.13 + uv baseline assumed; no tasks for older interpreter compatibility
