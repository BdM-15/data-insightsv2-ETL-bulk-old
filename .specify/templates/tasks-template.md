# Tasks: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), research.md, data-model.md, contracts/

## Execution Flow (main)

```
1. Load plan.md from feature directory
   → If not found: ERROR "No implementation plan found"
   → Extract: tech stack, libraries, structure
2. Load optional design documents:
   → data-model.md: Extract entities → model tasks
   → contracts/: Each file → contract test task
   → research.md: Extract decisions → setup tasks
3. Generate tasks by category:
   → Setup: project init, dependencies, linting
   → Tests: contract tests, integration tests
   → Core: models, services, CLI commands
   → Integration: DB, middleware, logging
   → Polish: unit tests, performance, docs
4. Apply task rules:
   → Different files = mark [P] for parallel
   → Same file = sequential (no [P])
   → Tests before implementation (TDD)
5. Number tasks sequentially (T001, T002...)
6. Generate dependency graph
7. Create parallel execution examples
8. Validate task completeness:
   → All contracts have tests?
   → All entities have models?
   → All endpoints implemented?
9. Return: SUCCESS (tasks ready for execution)
```

## Format: `[ID] [P?] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `src/`, `tests/` at repository root
- **Web app**: `backend/src/`, `frontend/src/`
- **Mobile**: `api/src/`, `ios/src/` or `android/src/`
- Paths shown below assume single project - adjust based on plan.md structure

## Phase 3.1: ETL Setup

- [ ] T001 Create project structure per implementation plan
- [ ] T002 Initialize [language] project with PostgreSQL and pgvector dependencies
- [ ] T003 [P] Configure linting and formatting tools
- [ ] T004 [P] Setup PostgreSQL connection and schema validation
- [ ] T005 [P] Configure USASpending API client with rate limiting

## Phase 3.2: Data Pipeline Tests (TDD) ⚠️ MUST COMPLETE BEFORE 3.3

**CRITICAL: These tests MUST be written and MUST FAIL before ANY implementation**

- [ ] T006 [P] Schema validation test for prime awards in tests/schema/test_prime_awards_schema.py
- [ ] T007 [P] Schema validation test for subawards in tests/schema/test_subawards_schema.py
- [ ] T008 [P] ETL integration test for API extraction in tests/integration/test_api_extraction.py
- [ ] T009 [P] Data integrity test for bulk load in tests/integration/test_bulk_load.py
- [ ] T010 [P] Vector preparation test in tests/vector/test_embedding_pipeline.py

## Phase 3.3: Core ETL Implementation (ONLY after tests are failing)

- [ ] T011 [P] Prime awards data model in src/models/prime_awards.py
- [ ] T012 [P] Subawards data model in src/models/subawards.py
- [ ] T013 [P] USASpending API extractor in src/extractors/usaspending_extractor.py
- [ ] T014 [P] PostgreSQL bulk loader in src/loaders/postgresql_loader.py
- [ ] T015 Data transformation pipeline for schema compliance
- [ ] T016 Vector embedding pipeline for text fields
- [ ] T017 Audit trail logging implementation
- [ ] T018 Error handling and data validation

## Phase 3.4: Database Integration

- [ ] T019 Connect models to capture_insights.s1_raw schema
- [ ] T020 Implement incremental data updates
- [ ] T021 Setup pgvector indexes for semantic search
- [ ] T022 Connection pooling and transaction management

## Phase 3.5: Monitoring & Polish

- [ ] T023 [P] Performance monitoring for ETL operations in tests/performance/test_etl_performance.py
- [ ] T024 [P] Data quality validation tests
- [ ] T025 [P] Update ETL documentation
- [ ] T026 Correlation ID logging implementation
- [ ] T027 Run data pipeline validation

## Dependencies

- Tests (T004-T007) before implementation (T008-T014)
- T008 blocks T009, T015
- T016 blocks T018
- Implementation before polish (T019-T023)

## Parallel Example

```
# Launch T004-T007 together:
Task: "Contract test POST /api/users in tests/contract/test_users_post.py"
Task: "Contract test GET /api/users/{id} in tests/contract/test_users_get.py"
Task: "Integration test registration in tests/integration/test_registration.py"
Task: "Integration test auth in tests/integration/test_auth.py"
```

## Notes

- [P] tasks = different files, no dependencies
- Verify tests fail before implementing
- Commit after each task
- Avoid: vague tasks, same file conflicts

## Task Generation Rules

_Applied during main() execution_

1. **From Contracts**:
   - Each contract file → contract test task [P]
   - Each endpoint → implementation task
2. **From Data Model**:
   - Each entity → model creation task [P]
   - Relationships → service layer tasks
3. **From User Stories**:

   - Each story → integration test [P]
   - Quickstart scenarios → validation tasks

4. **Ordering**:
   - Setup → Tests → Models → Services → Endpoints → Polish
   - Dependencies block parallel execution

## Validation Checklist

_GATE: Checked by main() before returning_

- [ ] All contracts have corresponding tests
- [ ] All entities have model tasks
- [ ] All tests come before implementation
- [ ] Parallel tasks truly independent
- [ ] Each task specifies exact file path
- [ ] No task modifies same file as another [P] task
