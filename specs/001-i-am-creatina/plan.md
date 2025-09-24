# Implementation Plan: i-am-creatina (USASpending Bulk ETL Foundation)

**Branch**: `001-i-am-creatina` | **Date**: 2025-09-24 | **Spec**: `specs/001-i-am-creatina/spec.md`
**Input**: Feature specification from `/specs/001-i-am-creatina/spec.md`

## Execution Flow (/plan command scope)

```
1. Load feature spec from Input path
   → If not found: ERROR "No feature spec at {path}"
2. Fill Technical Context (scan for NEEDS CLARIFICATION)
   → Detect Project Type from context (web=frontend+backend, mobile=app+api)
   → Set Structure Decision based on project type
3. Fill the Constitution Check section based on the content of the constitution document.
4. Evaluate Constitution Check section below
   → If violations exist: Document in Complexity Tracking
   → If no justification possible: ERROR "Simplify approach first"
   → Update Progress Tracking: Initial Constitution Check
5. Execute Phase 0 → research.md
   → If NEEDS CLARIFICATION remain: ERROR "Resolve unknowns"
6. Execute Phase 1 → contracts, data-model.md, quickstart.md, agent-specific template file (e.g., `CLAUDE.md` for Claude Code, `.github/copilot-instructions.md` for GitHub Copilot, `GEMINI.md` for Gemini CLI, `QWEN.md` for Qwen Code or `AGENTS.md` for opencode).
7. Re-evaluate Constitution Check section
   → If new violations: Refactor design, return to Phase 1
   → Update Progress Tracking: Post-Design Constitution Check
8. Plan Phase 2 → Describe task generation approach (DO NOT create tasks.md)
9. STOP - Ready for /tasks command
```

**IMPORTANT**: The /plan command STOPS at step 7. Phases 2-4 are executed by other commands:

- Phase 2: /tasks command creates tasks.md
- Phase 3-4: Implementation execution (manual or via tools)

## Summary

Foundational implementation of a storage-conscious, fail-fast, SQL-first ETL pipeline that ingests USASpending procurement prime awards (slim field set of 54 validated columns) and all procurement subaward fields into PostgreSQL (`capture_insights` DB) with schemas `s1_raw`, `s2_interim`, `s3_processed`, enabling incremental monthly delta-only refresh using `last_modified_date` and daily near-real incremental (short lookback) plus historical backfill via 7‑day chunked windows. Artifacts include archival metadata sidecars, progress & watermark tables, cleansing (IGF pattern removal) and preparation for future pgvector embedding on a canonical `semantic_description` field.

## Technical Context

**Language/Version**: Python 3.13+ (runtime guard exits if <3.13)  
**Primary Tooling**: uv (environment + dependency resolver, PEP 621 / lock), requests (HTTP), tenacity (retry/backoff), psycopg (v3 preferred), python-dotenv (local env), tqdm (optional progress), fastjsonschema or jsonschema (contract validation), logging stdlib  
**Storage**: PostgreSQL 14+ with pgvector extension (vector stage deferred), local filesystem for ZIP archive, ephemeral temp extraction  
**Testing**: pytest + hypothesis (optional) + contract schema validation; integration tests using a local PostgreSQL instance  
**Target Platform**: Windows dev (user), Linux container/VM prod (assumed)  
**Project Type**: single (backend ETL library + scripts)  
**Performance Goals**: Sustain ingest of 66–70M historical rows within bounded disk (<~150GB peak); Per-chunk load (7-day window) completes < 30 min (goal) with COPY; Maintain < 5% overhead for cleansing relative to staging time  
**Constraints**: <200GB free disk; fail-fast on acquisition errors; minimal memory footprint (<1GB resident for Python process); avoid pandas for large datasets  
**Scale/Scope**: Historical ~70M prime + subaward rows; Monthly incremental millions-scale updates; Daily incremental thousands to low millions

## Constitution Check

**Data Integrity First**: PASS – Plan preserves raw rows in `s1_raw`, archives original ZIP + checksum + metadata sidecar, enforces header validation, reversible transformations (raw retained).  
**Scalable ETL Architecture**: PASS – Chunked 7-day windows, sequential or limited concurrency, COPY-based loads, retry/backoff, streaming extraction, SQL transformations.  
**Schema Consistency**: PASS – Authoritative column list (54 fields) enforced; `s1_raw` mirrors API headers; later layers typed & deduped; schema-qualified references.  
**Vector-Ready Data Preparation**: PASS (deferred embeddings) – Provide `semantic_description` cleaned field; pgvector integration staged for later without blocking ingestion.  
**Observability and Monitoring**: PASS – Metrics catalog, logger schema, correlation id strategy, per-chunk measurement defined in Phase 0 research.

Gate Outcome: Post-Design constitution check passes; proceed to /tasks phase when invoked.

## Project Structure

### Documentation (this feature)

```
specs/001-i-am-creatina/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
└── tasks.md             # Phase 2 output (/tasks command - NOT created by /plan)
```

### Source Code (repository root)

```
# Adopt Single project (backend ETL focus)
etl/
  acquisition/
  staging/
  sql/
  utils/
  checks/
  __init__.py
sql/
  00_s1_raw/
  10_s2_interim/
  20_s3_processed/
  util/
logs/
data/downloads/
```

**Structure Decision**: Option 1 (Single project) — pure ETL/backend, no frontend/mobile concerns.

## Phase 0: Outline & Research

Open Items resolved; see `research.md` for final decisions.

## Phase 1: Design & Contracts

Artifacts produced: data-model.md, contracts (JSON schema + SQL DDL + env config), quickstart.md, initial contract test stub.

## Phase 2: Task Planning Approach

Unchanged from template; /tasks will enumerate ~25–30 tasks (tests first, then implementation) with parallelizable markers on independent artifacts (schema files, simple modules).

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
| --------- | ---------- | ------------------------------------ |
| (none)    | —          | —                                    |

## Progress Tracking

**Phase Status**:

- [x] Phase 0: Research complete (/plan command)
- [x] Phase 1: Design complete (/plan command)
- [ ] Phase 2: Task planning complete (/plan command - describe approach only)
- [ ] Phase 3: Tasks generated (/tasks command)
- [ ] Phase 4: Implementation complete
- [ ] Phase 5: Validation passed

**Gate Status**:

- [x] Initial Constitution Check: PASS (Observability refined in research)
- [x] Post-Design Constitution Check: PASS
- [x] All NEEDS CLARIFICATION resolved
- [ ] Complexity deviations documented

---

_Based on Constitution v1.9.0 - See `/memory/constitution.md`_
