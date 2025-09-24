# Phase 0 Research: i-am-creatina

Date: 2025-09-24  
Spec: `specs/001-i-am-creatina/spec.md`  
Constitution Version: 1.9.0

## Objectives

Establish concrete decisions for observability, metadata sidecars, watermarking, progress tracking, retry/backoff, disk guardrails, dedup SQL logic, and metrics so Constitution Check moves to full PASS before Phase 1.

## Decisions Summary Table

| Domain                     | Decision                                                                                                     | Rationale                                                        | Alternatives Considered                           | Status   |
| -------------------------- | ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- | ------------------------------------------------- | -------- |
| HTTP Client                | `requests` + `tenacity`                                                                                      | Mature, simple, retry flexibility                                | httpx (async adds complexity)                     | Accepted |
| DB Driver                  | `psycopg` (v3)                                                                                               | Async option later, better COPY API; still supports sync         | psycopg2 (legacy), SQLAlchemy (adds ORM overhead) | Accepted |
| Python Version Baseline    | Python 3.13 (guard: exit if <3.13)                                                                           | Latest stable bugfix line (3.13.x) before 3.14 GA; longer runway | 3.12 (security only), 3.11 (older, less perf)     | Accepted |
| Environment / Packaging    | `uv` (PEP 621 + lock)                                                                                        | Fast resolver, reproducible, no manual venv ceremony             | pip + requirements.txt (slower, no lock)          | Accepted |
| Logging Format             | JSON lines (one event per line)                                                                              | Structured parse, easy ingestion                                 | Plain text (harder parsing)                       | Accepted |
| Correlation ID             | UUID4 per chunk; propagate to all events                                                                     | Traceability across acquisition->staging->SQL                    | Per-request id (too granular)                     | Accepted |
| Sidecar Schema             | Fixed JSON with strict keys (see below)                                                                      | Deterministic, contract testable                                 | Free-form log snippet                             | Accepted |
| Retry Policy (POST)        | 5 attempts, exp backoff 2^n \* 1s jitter; max 64s                                                            | Balances resilience vs fail-fast                                 | Infinite retry (violates fail-fast)               | Accepted |
| Retry Policy (status poll) | Poll every 5s doubling to 60s cap; timeout `MAX_WAIT_SECONDS` (~3600)                                        | Reduces unnecessary polling                                      | Fixed short interval (wastes calls)               | Accepted |
| Disk Guard                 | Pre-chunk check: abort if free < `MIN_FREE_GB` (default 40GB)                                                | Prevent mid-load failure                                         | Post-facto alert (too late)                       | Accepted |
| Dedupe Prime Awards        | Latest by last_modified_date, tie-break action_date then ingestion_ts                                        | Constitution & freshness                                         | Keep earliest (loses corrections)                 | Accepted |
| Dedupe Subawards           | Composite key (prime_award_unique_key, subaward_number) latest by modification/action_date then ingestion_ts | Stable business identity                                         | Full row hash (unstable)                          | Accepted |
| Metrics Storage            | Append-only CSV in logs/metrics/ + optional DB table later                                                   | Simple start, low overhead                                       | Prometheus (overkill early)                       | Accepted |
| Free Disk Measurement      | Python `shutil.disk_usage(download_dir)`                                                                     | Cross-platform built-in                                          | psutil (extra dep)                                | Accepted |
| Hash Algorithm             | SHA256 streaming 64KB chunks                                                                                 | Standard, strong                                                 | MD5 (weaker), SHA1 (weaker)                       | Accepted |
| Temp Extraction            | `tempfile.TemporaryDirectory()` per chunk                                                                    | Auto cleanup                                                     | Manual fixed temp tree (cleanup risk)             | Accepted |
| COPY Method                | `psycopg.Cursor.copy_expert` with STDIN streaming                                                            | Efficient large loads                                            | INSERT batches (slow)                             | Accepted |

## Sidecar JSON Schema (Draft Contract)

Filename: `<ArchiveBaseName>.metadata.json`

```json
{
  "schema_version": "1.0.0",
  "job_id": "<uuid>",
  "correlation_id": "<uuid>",
  "request": {
    "endpoint": "https://api.usaspending.gov/api/v2/bulk_download/awards/",
    "payload_hash": "<sha256(request-payload-json-utf8)>",
    "date_from": "YYYY-MM-DD",
    "date_to": "YYYY-MM-DD",
    "date_type": "action_date|last_modified_date",
    "award_type_scope": "prime|sub"
  },
  "response": {
    "status_url": "<url>",
    "file_url": "<url>",
    "http_status": 200,
    "received_bytes": 12345678
  },
  "file": {
    "archive_path_rel": "data/downloads/usaspending/prime/2025/09/Prime_2025-09-24_H132233.zip",
    "archive_sha256": "<sha256>",
    "csv_expected_headers": ["contract_transaction_unique_key", "..."],
    "csv_header_present": true,
    "extracted_rows": null,
    "extracted_csv_bytes": null
  },
  "timing": {
    "requested_at": "2025-09-24T13:22:33Z",
    "ready_at": "2025-09-24T13:28:33Z",
    "download_started_at": "2025-09-24T13:28:40Z",
    "download_completed_at": "2025-09-24T13:29:05Z"
  },
  "chunk": {
    "window_start": "YYYY-MM-DD",
    "window_end": "YYYY-MM-DD",
    "chunk_index": 12,
    "chunk_span_days": 7
  },
  "integrity": {
    "zip_size_bytes": 12345678,
    "checksum_verified": true
  },
  "system": {
    "python_version": "3.13.x",
    "platform": "win32",
    "free_gb_pre": 142.3,
    "free_gb_post": 140.1
  },
  "fail_fast_triggered": false
}
```

Notes:

- `extracted_rows`/`extracted_csv_bytes` remain null unless debug mode persists CSV.
- `payload_hash` computed pre-request for reproducibility.
- `schema_version` increments on breaking structure changes.

## Watermark Table (DDL Draft)

```
CREATE TABLE IF NOT EXISTS capture_insights.meta_refresh_watermarks (
  pipeline_name text PRIMARY KEY,
  last_modified_to timestamptz NOT NULL,
  overlap_days integer NOT NULL DEFAULT 7,
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

## Progress Chunk Table (DDL Draft)

```
CREATE TABLE IF NOT EXISTS capture_insights.meta_chunk_progress (
  id bigserial PRIMARY KEY,
  pipeline_name text NOT NULL,
  window_start date NOT NULL,
  window_end date NOT NULL,
  chunk_index integer NOT NULL,
  job_id uuid NOT NULL,
  correlation_id uuid NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','in_progress','success','failed')),
  rows_staged bigint,
  rows_deduped bigint,
  archive_path_rel text,
  archive_sha256 text,
  error_class text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(pipeline_name, window_start, window_end, chunk_index)
);
```

## Metrics Catalog

| Metric                  | Type    | Description                           |
| ----------------------- | ------- | ------------------------------------- |
| chunk_rows_staged       | counter | Rows inserted into s1_raw for a chunk |
| chunk_rows_after_dedupe | gauge   | Rows retained after dedupe (s3)       |
| dedup_rows_removed      | counter | Rows eliminated by dedupe rules       |
| chunk_zip_bytes         | gauge   | ZIP file size                         |
| chunk_csv_bytes         | gauge   | Extracted CSV size (debug mode)       |
| copy_seconds            | timing  | Duration of COPY load                 |
| sql_transform_seconds   | timing  | Duration of s2 + s3 SQL sequence      |
| poll_attempts           | counter | Poll attempts for a job               |
| fail_fast_events        | counter | Number of chunks aborted by fail-fast |
| free_gb_pre             | gauge   | Free disk before chunk load           |
| free_gb_post            | gauge   | Free disk after cleanup               |
| checksum_mismatch       | counter | Integrity failures detected           |

Storage: Start as JSON lines appended to `logs/metrics/metrics-YYYYMMDD.log`.

## Logger Structure

JSON event keys (baseline):
`timestamp, level, event, correlation_id, job_id, pipeline_name, chunk_index, window_start, window_end, status, message, rows, zip_bytes, csv_bytes, elapsed_ms, free_gb_pre, free_gb_post, error_class, error_message`

## Dedupe SQL Sketch (Prime Awards)

```
-- s2_interim already cast types and includes last_modified_date, action_date, ingestion_ts
CREATE TABLE IF NOT EXISTS s3_processed.usaspending_prime_awards AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY contract_transaction_unique_key
           ORDER BY last_modified_date DESC NULLS LAST,
                    action_date DESC NULLS LAST,
                    ingestion_ts DESC
         ) AS rn
  FROM s2_interim.usaspending_prime_awards
) t
WHERE rn = 1;
```

Incremental merge approach: use staging temp table per chunk + INSERT ... ON CONFLICT (contract_transaction_unique_key) DO UPDATE setting columns when incoming row is newer by above precedence.

## Observability Open Item Resolution

All required metric names & logger fields now defined. Constitution Check Observability moves from PARTIAL to PASS.

## Risks & Mitigations

| Risk                                      | Impact              | Mitigation                                                             |
| ----------------------------------------- | ------------------- | ---------------------------------------------------------------------- |
| API rate/long job delays                  | Slower backfill     | Backoff + timeout + resumable progress table                           |
| Disk free falls below threshold mid-chunk | Failure wasted time | Pre-flight free-space assertion + small chunk size + immediate cleanup |
| Schema drift (field removed)              | Load abort          | Header guard + fail-fast + manual remediation step                     |
| Large WAL growth                          | Disk pressure       | UNLOGGED temp tables + limited indexing + periodic VACUUM              |
| Retry storms on outage                    | Noise & delay       | Bounded attempts & exponential backoff with jitter                     |

## Final Status

All clarifications resolved. Ready to proceed to Phase 1.
