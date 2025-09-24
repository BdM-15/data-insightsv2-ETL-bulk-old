# Environment Configuration Contract

All code MUST obtain settings via `etl.config` module; no direct os.environ access elsewhere.

| Variable               | Default                 | Required              | Description                            |
| ---------------------- | ----------------------- | --------------------- | -------------------------------------- |
| PG_USER                | postgres                | Yes                   | DB user                                |
| PG_PASSWORD            | admin                   | Yes (not in VCS)      | DB password                            |
| PG_HOST                | localhost               | Yes                   | DB host                                |
| PG_PORT                | 5432                    | Yes                   | DB port                                |
| PG_DBNAME              | capture_insights        | Yes                   | Database name                          |
| DOWNLOAD_DIR           | data/downloads          | Yes                   | Root for archived ZIPs                 |
| CHUNK_DAYS             | 7                       | Yes                   | Historical chunk span days             |
| CURRENT_DAYS_LOOKBACK  | 2                       | Yes                   | Daily incremental lookback             |
| MAX_WAIT_SECONDS       | 3600                    | Yes                   | Max wait for bulk job completion       |
| MAX_RETRIES            | 5                       | Yes                   | Post request retry attempts            |
| FAIL_FAST              | true                    | Yes                   | Abort pipeline on acquisition error    |
| MIN_FREE_GB            | 40                      | Yes                   | Minimum free disk before start chunk   |
| TARGET_PEAK_GB         | 100                     | Yes                   | Expected safe max working set          |
| PERSIST_EXTRACTED_CSV  | false                   | No                    | Keep extracted CSVs                    |
| LOG_LEVEL              | INFO                    | Yes                   | Logging verbosity                      |
| ARCHIVE_RETENTION_DAYS | 90                      | Yes                   | Prune policy for old ZIPs              |
| PIPELINE_NAME          | prime_awards_historical | Yes (script override) | Name used in progress/watermark tables |

Validation Rules:

- MIN_FREE_GB < TARGET_PEAK_GB must hold.
- CHUNK_DAYS between 1 and 31 inclusive.
- CURRENT_DAYS_LOOKBACK between 1 and 14.
- MAX_RETRIES between 1 and 10.
- MAX_WAIT_SECONDS >= 600.

Derived:

- DATABASE_URL = postgresql://PG_USER:PG_PASSWORD@PG_HOST:PG_PORT/PG_DBNAME
