# Feature Specification: Historical & Incremental USASpending Procurement ETL Pipeline for Capture Intelligence

**Feature Branch**: `001-i-am-creatina`  
**Created**: 2025-09-24  
**Status**: Draft  
**Input**: User description: "I am creatina a ETL project that pulls historical procurment data from usaspending.gov api bulk awards and puts it through an ETL process to develop a clean pipeline that will ultimately be used by capture managers to support leag agent generation for federal procurements. Capture manager typcially follow the shipley associates business development and capture management process."

## Execution Flow (main)

```
1. Parse user description from Input
	→ If empty: ERROR "No feature description provided"
2. Extract key concepts from description
	→ Identify: actors (capture manager, analyst), actions (ingest, validate, archive, enrich), data (prime awards, subawards), constraints (storage, fail-fast, required fields)
3. For each unclear aspect:
	→ Mark with [NEEDS CLARIFICATION: specific question]
4. Fill User Scenarios & Testing section
	→ If no clear user flow: ERROR "Cannot determine user scenarios"
5. Generate Functional Requirements
	→ Each requirement must be testable
	→ Mark ambiguous requirements
6. Identify Key Entities (data objects supporting capture intelligence)
7. Run Review Checklist
	→ If any [NEEDS CLARIFICATION]: WARN "Spec has uncertainties"
	→ If implementation details dominate: ERROR "Remove tech details"
8. Return: SUCCESS (spec ready for planning)
```

---

## ⚡ Quick Guidelines

- ✅ Focus on WHAT the pipeline must deliver (clean, reliable procurement data) and WHY (support capture lifecycle decisions)
- ❌ Avoid prescriptive implementation mechanics (specific libraries, code modules) beyond what is intrinsic to the feature scope
- 👥 Audience: business / capture stakeholders who need clarity on capability and governance

### Section Requirements

- Mandatory sections present: User Scenarios, Functional Requirements, Key Entities (data involved), Data Pipeline Requirements
- Optional sections (not added because out of scope): UI/Frontend, Security hardening (assumes organization standard baseline controls; no additional FedRAMP tailoring required for this feature scope)

## Assumptions

1. Audience includes technically literate business stakeholders (capture leadership, data product owner) comfortable with light technical terminology (e.g., "watermark", "deduplication").
2. No additional compliance-specific (FedRAMP/FISMA) feature requirements alter functional scope; standard corporate security controls already govern infrastructure.
3. Performance target: Historical backfill may run multi-hour; no strict SLA beyond successful completion and data fidelity. Incremental monthly run expected to finish within same business day.
4. Archive retention aligns with 90-day default; longer-term retention or cold storage policy is a future enhancement.
5. Embedding/vectorization occurs in a later feature; this spec only ensures cleaned text availability.
6. Manual operational intervention (restart after fail-fast stop) is acceptable and documented; no auto self-healing beyond retries.
7. Time synchronization (NTP) assumed accurate for window boundaries and watermark progression.
8. Runtime baseline is Python 3.13+ with uv-managed environment per project constitution; supporting earlier Python versions is explicitly out of scope for this feature.

### Ambiguity Handling

- Any unspecified performance SLAs, retention durations beyond archives, or downstream embedding latency have been left for future specs.
- Fail-fast rule clarified (stop at first unrecoverable chunk failure; manual restart) to eliminate gap-filling ambiguity.

---

## Glossary

- Watermark: Persisted pointer tracking the upper bound of processed window for incremental ingestion.
- Overlap Window: Backward extension (default 7 days) applied to lower bound to catch late-arriving or corrected records.
- Fail-Fast: Policy to terminate the run immediately upon unrecoverable chunk failure without advancing further chunks.
- Deduplication: Process of retaining the most current record per business key using modification date and tie-breakers.
- Semantic Description: Cleaned, normalized textual field derived from raw descriptions for downstream vectorization.
- Chunk: Discrete date-range request/processing unit (e.g., 7 calendar days for historical backfill).

---

## User Scenarios & Testing _(mandatory)_

### Primary User Story

A federal capture manager needs timely, accurate, and cleaned procurement award and subaward data (historical plus ongoing deltas) to identify, qualify, and track opportunities aligned to Shipley capture lifecycle phases (e.g., Identify, Qualify, Pursuit, Proposal, Post-Award). The pipeline supplies structured and enriched award activity enabling earlier insight into competitive landscape, agency spend patterns, and solicitation context to improve lead generation quality.

### Acceptance Scenarios

1. **Given** a new deployment with no prior data, **When** the pipeline executes a historical backfill for a defined date range, **Then** prime award and subaward data for that range are fully ingested (100% chunk success or explicit fail-fast stop) and available for downstream enrichment.
2. **Given** an established baseline of historical data, **When** the scheduled incremental monthly delta job runs, **Then** only new or modified records since the last watermark (plus 7-day overlap) are ingested and existing records updated without duplications.
3. **Given** malformed or incomplete API data (e.g., missing required header field), **When** ingestion is attempted, **Then** the load is halted, logged with severity=ERROR, and no partial/stale data is promoted.
4. **Given** a capture manager needs semantic search over opportunity descriptions, **When** processed award records are exposed to an embedding process, **Then** cleaned textual fields are available with one-to-one lineage to raw descriptions.
5. **Given** storage constraints, **When** large historical slices are processed, **Then** peak disk usage never violates MIN_FREE_GB (40GB threshold) and remains below TARGET_PEAK_GB (100GB informational cap).
6. **Given** a network or integrity failure during a chunk download (e.g., unreachable status_url, checksum mismatch), **When** the failure is detected, **Then** the pipeline stops within 60 seconds (fail-fast) and logs restart guidance referencing the last successful chunk end date.

### Edge Cases

- API returns a status that remains pending beyond timeout → pipeline aborts gracefully, marks chunk failed, no watermark advance.
- Duplicate award transactions with identical business keys but conflicting modification timestamps → latest modification wins; ties broken by action_date then ingestion timestamp.
- Late-arriving transaction appears after prior month closed → captured by overlap window and upserted.
- Partial archive corruption (ZIP checksum mismatch) → single automatic re-download; on second failure fail-fast terminates.
- Subawards endpoint schema change introducing new fields → schema guard blocks promotion until review.
- Network interruption mid-download → run terminates under fail-fast rule; no continuity gap filling attempted.

## Requirements _(mandatory)_

### Functional Requirements

- **FR-001**: System MUST ingest historical prime award procurement data (contracts only) from USASpending bulk awards API across a configurable date range.
- **FR-002**: System MUST ingest historical procurement subaward data with full schema capture.
- **FR-003**: System MUST perform monthly incremental delta ingestion using a last modified-based window plus an overlap buffer (default 7 days, configurable).
- **FR-004**: System MUST validate presence of all mandated prime award fields (including solicitation_identifier) before staging; if any missing → abort chunk.
- **FR-005**: System MUST archive each successful download (ZIP) with checksum (SHA256) and JSON metadata sidecar.
- **FR-006**: System MUST load raw data into a designated raw schema without transformation beyond metadata tagging.
- **FR-007**: System MUST perform SQL-based cleansing and deduplication producing canonical interim and processed layers.
- **FR-008**: System MUST support deterministic upsert logic keyed by contract and transaction identifiers (contract_award_unique_key, contract_transaction_unique_key).
- **FR-009**: System MUST provide a restartable mechanism that resumes from the last successfully completed chunk without duplicating data.
- **FR-010**: System MUST enforce storage guardrails: refuse to start a chunk if free disk < MIN_FREE_GB (default 40GB); log warning if projected working set > TARGET_PEAK_GB (default 100GB).
- **FR-011**: System MUST log structured operational events (payload hash, row counts, durations, sizes, disk usage snapshot, errors) for every chunk.
- **FR-012**: System MUST produce semantically cleaned description text alongside original fields for downstream vectorization.
- **FR-013**: System MUST retain historical archives for a configurable retention period (default 90 days) and prune expired artifacts safely.
- **FR-014**: System MUST prevent full production reloads outside a controlled exception process with documented approval.
- **FR-015**: System MUST expose a watermarked record of last successful incremental window (start, end, overlap, updated_at).
- **FR-016**: System MUST block promotion if schema drift (missing or renamed required fields) is detected.
- **FR-017**: Users MUST be able to query processed award data with consistent, stable column naming.
- **FR-018**: System MUST ensure original raw text remains retrievable for any processed record (traceability via checksum + foreign key lineage).
- **FR-019**: System MUST handle transient API failures with retry (up to 5 attempts, exponential backoff doubling to max 120s) before marking chunk failed.
- **FR-020**: System MUST record and surface any irrecoverable chunk failures for manual remediation (fail-fast event).
- **FR-021**: System MUST support configuration via environment variables mapped through a central configuration layer.
- **FR-022**: System MUST segregate raw, interim, and processed layers using distinct schemas.
- **FR-023**: System MUST ensure incremental runs do not reprocess unchanged data beyond the defined overlap window.
- **FR-024**: System MUST provide cleaned text fields to support future semantic search (vectorization out of scope here).
- **FR-025**: System MUST maintain audit lineage linking processed records to source archive checksum and file path.
- **FR-026**: System MUST mark any ambiguous or incomplete capture lifecycle mappings for future enrichment rather than guessing.
- **FR-027**: System MUST support a configurable chunk size (e.g., 7-day windows) for historical backfill to manage performance and storage.
- **FR-028**: System MUST allow forced reprocessing of a specific chunk through an explicit override while preserving prior archives.
- **FR-029**: System MUST expose high-level operational metrics (see Metrics section) per run.
- **FR-030**: System MUST detect and skip duplicate archive files based on checksum.
- **FR-031**: System MUST enforce deterministic ordering in deduplication conflict resolution (modification date, action_date, ingestion timestamp hierarchy).
- **FR-032**: System MUST provide a means to flag newly introduced API fields for review without blocking ingestion of unrelated data unless they collide with required names.
- **FR-033**: System MUST allow configuration of overlap days for incremental runs (default 7).
- **FR-034**: System MUST provide a manual dry-run mode that evaluates planned windows, projected disk usage, and API payload size estimates without performing downloads.
- **FR-035**: System MUST surface when minimum free disk threshold would be violated before starting a chunk and abort that chunk.
- **FR-036**: System MUST terminate the run within 60 seconds upon an unrecoverable download or extraction failure (fail-fast) without attempting later chunks and require manual restart from the last successful chunk end date.

### Key Entities _(include if feature involves data)_

- **Prime Award Transaction**: A single procurement transaction containing obligation, award identifiers, dates, competitive attributes, descriptions, and classification codes; uniquely identified by transaction and award keys.
- **Prime Award (Aggregated)**: Logical grouping of related transactions under a contract award key; used for summarizing lifecycle and obligation totals.
- **Subaward**: A procurement-related secondary award record associated with a prime award; includes recipient, amount, and action details.
- **Archive Artifact**: The stored ZIP (and optional extracted CSV) plus associated metadata sidecar containing request parameters and checksum.
- **Watermark Record**: A control record capturing last modified upper bound and overlap logic for incremental continuity.
- **Cleansed Description**: A semantic-friendly text synthesized from raw description fields after noise removal.

### Data Pipeline Requirements _(include for ETL features)_

- **Data Source**: USASpending bulk awards API (prime awards contracts only; procurement subawards). Endpoint supporting POST job creation and status polling.
- **Data Volume**: Historical load up to ~70M prime award transactions plus corresponding subawards; chunked (default 7-day windows) for feasibility; monthly increment windows produce smaller deltas.
- **Data Quality**: Mandatory field presence checks; checksum verification; schema drift detection; deterministic deduplication; preservation of raw originals.
- **Storage Schema**: Layered schemas (raw, interim, processed) with strict field preservation in raw and canonical naming in processed; audit tables for watermarks and job metadata.
- **Vector Requirements**: Provide cleaned textual field (`semantic_description`) and maintain mapping to original description fields; no embeddings generated within this feature scope.

## Metrics & Quantitative Criteria

Core metrics (logged per run and per chunk):

- ingestion_chunks_planned
- ingestion_chunks_completed
- ingestion_chunks_failed
- ingestion_rows_raw
- ingestion_rows_processed
- dedup_rows_removed
- fail_fast_events
- disk_free_gb_before_chunk / disk_free_gb_after_chunk
- wal_growth_estimate_mb (if observable)

Success thresholds:

- Duplicate residuals after dedup query = 0 business key collisions.
- Fail-fast events = 0 on healthy runs; when >0, run status = failed.
- Free disk never < MIN_FREE_GB at chunk start.
- Schema drift incidents block promotion 100% of occurrences.

## External Dependencies

- Availability of USASpending Bulk Awards API endpoint and status polling.
- Stable network connectivity to API and PostgreSQL.
- PostgreSQL instance with required schemas (raw, interim, processed) and pgvector extension installed.
- System clock synchronization (NTP) to ensure correct window boundaries.
- Sufficient disk capacity (baseline free space >= MIN_FREE_GB + projected working set).
- Python 3.13+ runtime and uv for environment and dependency management (infrastructure readiness assumed; not delivered by this feature).

## Non-Goals

- Generating embeddings or similarity indexes (future feature).
- Advanced opportunity classification or scoring.
- UI/dashboard presentation layer.
- Long-term archival/cold storage lifecycle beyond configured retention.
- Complex compliance tailoring (FedRAMP control mappings) beyond baseline security.

## Review & Acceptance Checklist

### Content Quality

- [x] No extraneous implementation mechanics beyond source identification
- [x] Focused on user value and business needs
- [x] Suitable for mixed business/technical stakeholder audience (glossary provided)
- [x] All mandatory sections completed

### Requirement Completeness

- [x] No unresolved clarification markers remain
- [x] Requirements are testable and unambiguous (quantitative thresholds included)
- [x] Success criteria are measurable (see Metrics & Quantitative Criteria)
- [x] Scope is clearly bounded (historical + incremental ingestion; excludes embeddings & advanced analytics)
- [x] Dependencies and assumptions identified (see Assumptions & External Dependencies)

## Execution Status

- [x] User description parsed
- [x] Key concepts extracted
- [x] Ambiguities marked (and resolved)
- [x] User scenarios defined
- [x] Requirements generated
- [x] Entities identified
- [x] Review checklist passed
