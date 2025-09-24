<!--
SYNC IMPACT REPORT - Constitution Update 2025-09-24

Version Change: TEMPLATE → 1.0.0 → 1.1.0 → 1.2.0 → 1.3.0 (Added specific USASpending field requirements and API validation)

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
- Data Extraction Specifications: 50 specific prime award fields, full subaward extraction (ADDED v1.3.0)

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
**Schema**: `capture_insights.s1_raw` for all raw data storage (66-70M records expected)
**API Integration**: USASpending Bulk Award API as primary data source (procurement awards only, no grants)
**API Documentation**: https://github.com/fedspendingtransparency/usaspending-api/blob/master/usaspending_api/api_contracts/contracts/v2/bulk_download/awards.md
**Reference Implementation**: Follow patterns established in https://github.com/BdM-15/Data_Insights/tree/main/src/backend/data/data_acquisition
**Vector Operations**: pgvector for semantic search and similarity operations
**Data Formats**: JSON for API responses, normalized relational storage in PostgreSQL
**Dependency Management**: requirements.txt for production, requirements-dev.txt for development dependencies
**SQL Management**: Version-controlled SQL scripts for transformations, stored procedures for complex operations

## Data Extraction Specifications

### Prime Awards Field Requirements (usaspending_prime_awards_slimv2)

**Data Type**: Procurement contracts only (no grants)
**Required Fields**: Exactly 50 fields specified below MUST be extracted and stored
**Field Verification**: solicitation_identifier field name MUST be verified via API schema inspection before implementation

**TARGET_FIELDS** (49 confirmed + 1 TBD):

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
solicitation_procedures, extent_competed, type_of_set_aside,
fair_opportunity_limited_sources, other_than_full_and_open_competition,
number_of_offers_received, subcontracting_plan, government_furnished_property,
type_of_contract_pricing, action_type, award_type, type_of_idc, idv_type,
undefinitized_action, program_acronym, multi_year_contract,
multiple_or_single_award_idv, usaspending_permalink, [solicitation_identifier - TBD]
```

### Subawards Field Requirements (usaspending_subawards_v2)

**Data Type**: Procurement subawards only (no grants)
**Required Fields**: ALL available fields from USASpending API
**Extraction Strategy**: Full schema extraction to capture complete subaward data structure

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

**Version**: 1.3.0 | **Ratified**: 2025-09-24 | **Last Amended**: 2025-09-24
