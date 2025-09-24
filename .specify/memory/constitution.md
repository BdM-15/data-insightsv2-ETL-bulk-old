<!--
SYNC IMPACT REPORT - Constitution Update 2025-09-24

Version Change: TEMPLATE → 1.0.0 (Initial constitution creation)

Modified Principles:
- NEW: I. Data Integrity First (NON-NEGOTIABLE) - Government data accuracy and audit trails
- NEW: II. Scalable ETL Architecture - Bulk data processing and PostgreSQL optimization
- NEW: III. Schema Consistency - s1_raw schema structure for USASpending data
- NEW: IV. Vector-Ready Data Preparation - pgvector integration for semantic search
- NEW: V. Observability and Monitoring - Comprehensive logging and monitoring

Added Sections:
- Technology Standards: PostgreSQL + pgvector, USASpending API integration
- Development Workflow: Testing, review, and deployment requirements

Templates Requiring Updates:
✅ plan-template.md - Updated (Constitution Check section aligned with data pipeline principles)
✅ spec-template.md - Updated (Added Data Pipeline Requirements section)
✅ tasks-template.md - Updated (ETL-specific task phases and categories)
✅ commands/*.md - Not applicable (no command files found)

Follow-up TODOs: None - all placeholders filled
-->

# Data Insights v2 ETL Bulk Pipeline Constitution

## Core Principles

### I. Data Integrity First (NON-NEGOTIABLE)

All data ingestion from USASpending API must maintain complete fidelity and audit trails. Schema validation MUST occur before insertion into PostgreSQL; Data transformations MUST be reversible and logged; No silent data loss or corruption permitted; Source data lineage tracking required for all records.

**Rationale**: Government procurement data accuracy is critical for compliance and decision-making. Any data corruption undermines the entire analytical pipeline.

### II. Scalable ETL Architecture

ETL processes MUST handle bulk data operations efficiently. Batch processing preferred over real-time for large datasets; Incremental updates supported for delta processing; Memory-efficient streaming for large API responses; PostgreSQL connection pooling and transaction management enforced.

**Rationale**: USASpending bulk data can be massive (millions of records). Architecture must scale without performance degradation.

### III. Schema Consistency

Database schema `s1_raw` MUST maintain strict structure for tables `usaspending_prime_awards_slimv2` and `usaspending_subawards_v2`. Column definitions match API specification exactly; Data types enforced at database level; Foreign key relationships maintained; Version-controlled schema migrations required.

**Rationale**: Consistent schema enables reliable downstream analytics and ensures data quality across the entire pipeline.

### IV. Vector-Ready Data Preparation

All textual data MUST be prepared for pgvector integration. Text fields normalized and cleaned before vectorization; Embedding generation pipeline documented and versioned; Vector indexes optimized for semantic search performance; Metadata preserved for vector-to-source traceability.

**Rationale**: Semantic search capabilities depend on high-quality text processing and efficient vector operations.

### V. Observability and Monitoring

Every ETL operation MUST be logged, monitored, and traceable. Structured logging with correlation IDs; Performance metrics for API calls, database operations, and vectorization; Error handling with detailed context; Health checks for all pipeline components.

**Rationale**: Data pipelines are complex systems requiring comprehensive monitoring to ensure reliability and enable rapid troubleshooting.

## Technology Standards

**Database**: PostgreSQL with pgvector extension for vector operations
**Schema**: `capture_insights.s1_raw` for all raw data storage
**API Integration**: USASpending Bulk Award API as primary data source
**Reference Implementation**: Follow patterns established in https://github.com/BdM-15/Data_Insights/tree/main/src/backend/data/data_acquisition
**Vector Operations**: pgvector for semantic search and similarity operations
**Data Formats**: JSON for API responses, normalized relational storage in PostgreSQL

## Development Workflow

**Testing Requirements**: Unit tests for data transformation logic; Integration tests for database operations; End-to-end tests for complete ETL workflows
**Code Review**: All ETL scripts must be reviewed for data integrity and performance; Database schema changes require architectural review
**Deployment**: Staging environment required for testing with sample data before production deployment
**Documentation**: API integration patterns documented; Database schema versioned; Vector embedding procedures documented

## Governance

This constitution governs all ETL pipeline development and data operations. All code changes must demonstrate compliance with data integrity principles. Schema migrations require approval and testing. Vector embedding changes must include performance impact analysis. Emergency data fixes require post-incident review and constitution compliance verification.

**Amendment Process**: Constitution changes require documentation of impact on existing pipelines, approval from data architecture review, and migration plan for existing data.

**Compliance Review**: All pull requests must verify adherence to data integrity, schema consistency, and observability requirements. Performance benchmarks must be maintained or improved.

**Version**: 1.0.0 | **Ratified**: 2025-09-24 | **Last Amended**: 2025-09-24
