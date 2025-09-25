"""Test archive metadata schema contract validation.

This test MUST fail initially until archive metadata generation is implemented.
"""
import json
from pathlib import Path

import pytest

# Note: This will fail until we add jsonschema to dependencies
try:
    import jsonschema
except ImportError:
    pytest.skip("jsonschema not available", allow_module_level=True)


def load_schema():
    """Load the archive metadata JSON schema."""
    schema_path = Path(__file__).parents[2] / "specs" / "001-i-am-creatina" / "contracts" / "archive_metadata.schema.json"
    with open(schema_path) as f:
        return json.load(f)


def test_valid_archive_metadata():
    """Test that valid archive metadata passes schema validation."""
    schema = load_schema()
    
    # This will fail until archive manager creates valid metadata
    valid_metadata = {
        "schema_version": "1.0.0",
        "job_id": "123e4567-e89b-12d3-a456-426614174000",
        "correlation_id": "123e4567-e89b-12d3-a456-426614174001", 
        "request": {
            "endpoint": "https://api.usaspending.gov/api/v2/bulk_download/awards/",
            "payload_hash": "a" * 64,  # 64-char hex
            "date_from": "2024-01-01",
            "date_to": "2024-01-07",
            "date_type": "action_date",
            "award_type_scope": "prime"
        },
        "response": {
            "status_url": "https://api.usaspending.gov/status/123",
            "file_url": "https://api.usaspending.gov/download/123.zip",
            "http_status": 200,
            "received_bytes": 12345678
        },
        "file": {
            "archive_path_rel": "data/downloads/prime/2024/01/Prime_2024-01-01_123.zip",
            "archive_sha256": "b" * 64,  # 64-char hex
            "csv_expected_headers": ["contract_transaction_unique_key", "contract_award_unique_key"],
            "csv_header_present": True,
            "extracted_rows": 1000,
            "extracted_csv_bytes": 5000000
        },
        "timing": {
            "requested_at": "2024-01-01T10:00:00Z",
            "ready_at": "2024-01-01T10:05:00Z", 
            "download_started_at": "2024-01-01T10:05:30Z",
            "download_completed_at": "2024-01-01T10:06:00Z"
        },
        "chunk": {
            "window_start": "2024-01-01",
            "window_end": "2024-01-07",
            "chunk_index": 0,
            "chunk_span_days": 7
        },
        "integrity": {
            "zip_size_bytes": 12345678,
            "checksum_verified": True
        },
        "system": {
            "python_version": "3.13.0",
            "platform": "win32",
            "free_gb_pre": 142.5,
            "free_gb_post": 140.2
        },
        "fail_fast_triggered": False
    }
    
    # This should not raise an exception
    jsonschema.validate(valid_metadata, schema)


def test_invalid_archive_metadata_missing_required():
    """Test that metadata missing required fields fails validation."""
    schema = load_schema()
    
    # Missing required field 'job_id'
    invalid_metadata = {
        "schema_version": "1.0.0",
        "correlation_id": "123e4567-e89b-12d3-a456-426614174001"
    }
    
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_metadata, schema)


def test_invalid_archive_metadata_wrong_format():
    """Test that metadata with wrong field formats fails validation.""" 
    schema = load_schema()
    
    # Invalid UUID format
    invalid_metadata = {
        "schema_version": "1.0.0",
        "job_id": "not-a-uuid",
        "correlation_id": "123e4567-e89b-12d3-a456-426614174001",
        "request": {
            "endpoint": "https://api.usaspending.gov/api/v2/bulk_download/awards/",
            "payload_hash": "a" * 64,
            "date_from": "2024-01-01", 
            "date_to": "2024-01-07",
            "date_type": "action_date",
            "award_type_scope": "prime"
        },
        "response": {
            "status_url": "https://api.usaspending.gov/status/123",
            "file_url": "https://api.usaspending.gov/download/123.zip", 
            "http_status": 200,
            "received_bytes": 12345678
        },
        "file": {
            "archive_path_rel": "data/downloads/prime/2024/01/Prime_2024-01-01_123.zip",
            "archive_sha256": "b" * 64,
            "csv_expected_headers": ["contract_transaction_unique_key"],
            "csv_header_present": True,
            "extracted_rows": None,
            "extracted_csv_bytes": None
        },
        "timing": {
            "requested_at": "2024-01-01T10:00:00Z",
            "ready_at": "2024-01-01T10:05:00Z",
            "download_started_at": "2024-01-01T10:05:30Z", 
            "download_completed_at": "2024-01-01T10:06:00Z"
        },
        "chunk": {
            "window_start": "2024-01-01",
            "window_end": "2024-01-07",
            "chunk_index": 0,
            "chunk_span_days": 7
        },
        "integrity": {
            "zip_size_bytes": 12345678,
            "checksum_verified": True
        },
        "system": {
            "python_version": "3.13.0",
            "platform": "win32",
            "free_gb_pre": 142.5,
            "free_gb_post": 140.2
        },
        "fail_fast_triggered": False
    }
    
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid_metadata, schema)


def test_archive_metadata_schema_loads():
    """Test that the schema file itself is valid JSON."""
    schema = load_schema()
    
    # Basic checks
    assert schema["title"] == "Archive File Metadata"
    assert "required" in schema
    assert len(schema["required"]) > 0