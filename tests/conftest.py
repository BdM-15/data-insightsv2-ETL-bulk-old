"""Pytest configuration and fixtures for ETL tests."""
import os
import tempfile
from pathlib import Path
from typing import Generator

import pytest

from etl.config import ETLConfig, reset_config


@pytest.fixture
def temp_download_dir() -> Generator[str, None, None]:
    """Provide temporary download directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def fake_config() -> Generator[ETLConfig, None, None]:
    """Provide test configuration with safe defaults."""
    # Store original environment
    original_env = dict(os.environ)
    
    # Set test environment variables
    test_env = {
        'PG_USER': 'test_user',
        'PG_PASSWORD': 'test_pass',
        'PG_HOST': 'localhost',
        'PG_PORT': '5432',
        'PG_DBNAME': 'test_capture_insights',
        'DOWNLOAD_DIR': 'test_downloads',
        'CHUNK_DAYS': '3',
        'CURRENT_DAYS_LOOKBACK': '1',
        'MAX_WAIT_SECONDS': '600',
        'MAX_RETRIES': '3',
        'FAIL_FAST': 'true',
        'MIN_FREE_GB': '5',
        'TARGET_PEAK_GB': '20',
        'PERSIST_EXTRACTED_CSV': 'false',
        'LOG_LEVEL': 'DEBUG',
        'ARCHIVE_RETENTION_DAYS': '7',
        'PIPELINE_NAME': 'test_pipeline'
    }
    
    # Update environment
    os.environ.update(test_env)
    
    # Reset config to pick up test env
    reset_config()
    
    try:
        from etl.config import get_config
        yield get_config()
    finally:
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)
        reset_config()


@pytest.fixture
def pg_connection():
    """Placeholder for PostgreSQL connection fixture.
    
    TODO: Implement when database integration tests are needed.
    This would typically create a test database connection
    using the fake_config fixture.
    """
    # For now, return None as placeholder
    # In future implementation, this would:
    # 1. Create test database connection
    # 2. Set up test schemas
    # 3. Yield connection
    # 4. Clean up test data
    return None


@pytest.fixture(autouse=True)
def reset_correlation_context():
    """Reset correlation context between tests."""
    from etl.utils.logging import correlation_id_var
    
    # Reset correlation ID context
    correlation_id_var.set(None)
    
    yield
    
    # Clean up after test
    correlation_id_var.set(None)