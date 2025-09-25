# USASpending ETL Pipeline

A storage-conscious, fail-fast ETL pipeline for USASpending procurement data.

## Features

- Bulk download of prime awards and subawards via USASpending API
- PostgreSQL-based 3-layer processing (raw → interim → processed)
- Deduplication with configurable precedence rules
- Incremental processing with watermarks
- Archive retention and integrity checking
- Comprehensive observability and metrics

## Requirements

- Python 3.13+
- PostgreSQL 14+ with pgvector extension
- 40GB+ free disk space for processing

## Quick Start

See `specs/001-i-am-creatina/quickstart.md` for detailed setup and usage instructions.
