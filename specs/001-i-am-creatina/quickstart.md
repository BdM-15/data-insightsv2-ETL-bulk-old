# Quickstart: USASpending Bulk ETL (i-am-creatina)

This guide walks you through setting up a local environment, performing a historical backfill, and running an incremental refresh consistent with the Constitution (v1.9.0).

## 1. Prerequisites

- Python 3.13.x installed (runtime guard will exit if <3.13)
- `uv` installed (see https://docs.astral.sh/uv/) for environment + dependency management
- PostgreSQL 14+ running locally with user/password available
- pgvector extension installed (CREATE EXTENSION pgvector; can be deferred)
- Git + ~200GB free disk (historical run uses much less peak due to streaming, but ensure > MIN_FREE_GB)

## 2. Create Database & Schemas

```
CREATE DATABASE capture_insights;
\c capture_insights;
CREATE SCHEMA IF NOT EXISTS s1_raw;
CREATE SCHEMA IF NOT EXISTS s2_interim;
CREATE SCHEMA IF NOT EXISTS s3_processed;
CREATE SCHEMA IF NOT EXISTS util;
```

## 3. Environment & Dependencies (uv)

`uv` handles Python selection, environment creation, and dependency resolution from `pyproject.toml`.

```
# Pin interpreter (first run only)
uv python pin 3.13

# Install dependencies (will create .venv automatically)
uv sync

# Activate (optional; you can also run via `uv run` without activating)
.venv\Scripts\activate

# Run any script with ephemeral resolution
uv run python -V
```

When adding a dependency:

```
uv add fastjsonschema
```

This updates `pyproject.toml` and lock file.

## 4. Environment Variables (.env)

Create `.env` at repo root:

```
PG_USER=postgres
PG_PASSWORD=admin
PG_HOST=localhost
PG_PORT=5432
PG_DBNAME=capture_insights
DOWNLOAD_DIR=data/downloads
CHUNK_DAYS=7
CURRENT_DAYS_LOOKBACK=2
MAX_WAIT_SECONDS=3600
MAX_RETRIES=5
FAIL_FAST=true
MIN_FREE_GB=40
TARGET_PEAK_GB=100
PERSIST_EXTRACTED_CSV=false
LOG_LEVEL=INFO
ARCHIVE_RETENTION_DAYS=90
PIPELINE_NAME=prime_awards_historical
```

## 5. Historical Backfill (Placeholder)

Implementation scripts will live under `scripts/` (to be added). Expected usage pattern:

```
uv run python scripts/fetch_historical.py --from 2018-10-01 --to 2024-09-01 --award-scope prime
```

Behavior:

- Walk 7-day windows (`CHUNK_DAYS`)
- Archive each ZIP & metadata sidecar
- COPY into `s1_raw.usaspending_prime_awards_slimv2`
- Log metrics + progress table rows

## 6. Incremental Refresh (Daily)

```
uv run python scripts/fetch_incremental.py --award-scope prime
```

Behavior:

- Compute lookback window: today - `CURRENT_DAYS_LOOKBACK`
- Use `date_type=last_modified_date`
- Update progress & watermark tables

## 7. Monthly Delta Refresh

Run early month (1st–3rd):

```
uv run python scripts/fetch_incremental.py --mode monthly --month 2025-08 --award-scope prime
```

Computes full prior month window with overlap (7 days) and subdivides if needed.

## 8. Transform Steps

After staging enough data:

```
uv run python scripts/run_transforms.py --layers s2,s3
```

Applies:

1. s2 cleansing + `semantic_description`
2. s3 dedupe & merge

## 9. Validation Checklist

- Headers match 54-field list (prime) / full (sub) ✔
- Progress rows for each chunk ✔
- Watermark advanced only after success ✔
- Sidecar JSON validates against `archive_metadata.schema.json` ✔
- Disk free space logged before/after ✔

## 10. Troubleshooting

| Symptom                      | Likely Cause               | Action                                                                              |
| ---------------------------- | -------------------------- | ----------------------------------------------------------------------------------- |
| Fail-fast triggered early    | Network or header mismatch | Inspect last metadata sidecar & logs; resolve then rerun from last success end date |
| Missing columns in raw table | API schema drift           | Pause pipeline, update header guard & constitution if needed                        |
| Disk threshold abort         | Insufficient space         | Increase MIN_FREE_GB margin or free space before retry                              |

## 11. Next Steps

- Implement scripts & modules (Phase 3 tasks)
- Add vectorization pipeline referencing `semantic_description` (future feature)

---

Generated during Phase 1 design; updated for Python 3.13 + uv workflow.
