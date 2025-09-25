"""Test environment configuration validation.

These tests verify that config loading and validation works per the contract.
"""
import os
import pytest

from etl.config import ETLConfig, ConfigValidationError, load_config, reset_config


def test_config_defaults():
    """Test that default configuration values match contract."""
    config = ETLConfig()
    
    # Database defaults
    assert config.pg_user == "postgres"
    assert config.pg_password == "admin" 
    assert config.pg_host == "localhost"
    assert config.pg_port == 5432
    assert config.pg_dbname == "capture_insights"
    
    # File system defaults
    assert config.download_dir == "data/downloads"
    
    # Processing defaults
    assert config.chunk_days == 7
    assert config.current_days_lookback == 2
    assert config.max_wait_seconds == 3600
    assert config.max_retries == 5
    assert config.fail_fast == True
    
    # Disk guardrails
    assert config.min_free_gb == 40.0
    assert config.target_peak_gb == 100.0
    
    # Optional defaults
    assert config.persist_extracted_csv == False
    assert config.log_level == "INFO"
    assert config.archive_retention_days == 90
    assert config.pipeline_name == "prime_awards_historical"


def test_config_validation_min_free_gb_less_than_target_peak():
    """Test validation rule: MIN_FREE_GB < TARGET_PEAK_GB must hold."""
    config = ETLConfig(min_free_gb=150.0, target_peak_gb=100.0)
    
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    
    assert "MIN_FREE_GB" in str(exc_info.value)
    assert "TARGET_PEAK_GB" in str(exc_info.value)


def test_config_validation_chunk_days_range():
    """Test validation rule: CHUNK_DAYS between 1 and 31 inclusive."""
    # Test lower bound
    config = ETLConfig(chunk_days=0)
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    assert "CHUNK_DAYS" in str(exc_info.value)
    
    # Test upper bound
    config = ETLConfig(chunk_days=32)
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    assert "CHUNK_DAYS" in str(exc_info.value)
    
    # Test valid values
    for days in [1, 7, 31]:
        config = ETLConfig(chunk_days=days)
        config.validate()  # Should not raise


def test_config_validation_current_days_lookback_range():
    """Test validation rule: CURRENT_DAYS_LOOKBACK between 1 and 14."""
    # Test lower bound
    config = ETLConfig(current_days_lookback=0)
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    assert "CURRENT_DAYS_LOOKBACK" in str(exc_info.value)
    
    # Test upper bound
    config = ETLConfig(current_days_lookback=15)
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    assert "CURRENT_DAYS_LOOKBACK" in str(exc_info.value)
    
    # Test valid values
    for days in [1, 7, 14]:
        config = ETLConfig(current_days_lookback=days)
        config.validate()  # Should not raise


def test_config_validation_max_retries_range():
    """Test validation rule: MAX_RETRIES between 1 and 10."""
    # Test lower bound
    config = ETLConfig(max_retries=0)
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    assert "MAX_RETRIES" in str(exc_info.value)
    
    # Test upper bound 
    config = ETLConfig(max_retries=11)
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    assert "MAX_RETRIES" in str(exc_info.value)
    
    # Test valid values
    for retries in [1, 5, 10]:
        config = ETLConfig(max_retries=retries)
        config.validate()  # Should not raise


def test_config_validation_max_wait_seconds_minimum():
    """Test validation rule: MAX_WAIT_SECONDS >= 600."""
    config = ETLConfig(max_wait_seconds=599)
    with pytest.raises(ConfigValidationError) as exc_info:
        config.validate()
    assert "MAX_WAIT_SECONDS" in str(exc_info.value)
    
    # Test valid values
    for seconds in [600, 3600, 7200]:
        config = ETLConfig(max_wait_seconds=seconds)
        config.validate()  # Should not raise


def test_config_database_url_property():
    """Test that database_url property constructs correct URL."""
    config = ETLConfig(
        pg_user="testuser",
        pg_password="testpass", 
        pg_host="testhost",
        pg_port=5433,
        pg_dbname="testdb"
    )
    
    expected_url = "postgresql://testuser:testpass@testhost:5433/testdb"
    assert config.database_url == expected_url


def test_load_config_from_environment():
    """Test loading configuration from environment variables."""
    # Store original environment 
    original_env = dict(os.environ)
    
    try:
        # Set test environment variables
        test_env = {
            'PG_USER': 'envuser',
            'PG_PASSWORD': 'envpass',
            'PG_HOST': 'envhost',
            'PG_PORT': '5434',
            'PG_DBNAME': 'envdb',
            'DOWNLOAD_DIR': 'test/downloads',
            'CHUNK_DAYS': '14',
            'CURRENT_DAYS_LOOKBACK': '3',
            'MAX_WAIT_SECONDS': '7200',
            'MAX_RETRIES': '7',
            'FAIL_FAST': 'false',
            'MIN_FREE_GB': '50.5',
            'TARGET_PEAK_GB': '200.0',
            'PERSIST_EXTRACTED_CSV': 'true',
            'LOG_LEVEL': 'DEBUG',
            'ARCHIVE_RETENTION_DAYS': '60',
            'PIPELINE_NAME': 'test_pipeline'
        }
        
        os.environ.update(test_env)
        reset_config()
        
        config = load_config()
        
        # Verify values were loaded from environment
        assert config.pg_user == 'envuser'
        assert config.pg_password == 'envpass'
        assert config.pg_host == 'envhost'
        assert config.pg_port == 5434
        assert config.pg_dbname == 'envdb'
        assert config.download_dir == 'test/downloads'
        assert config.chunk_days == 14
        assert config.current_days_lookback == 3
        assert config.max_wait_seconds == 7200
        assert config.max_retries == 7
        assert config.fail_fast == False
        assert config.min_free_gb == 50.5
        assert config.target_peak_gb == 200.0
        assert config.persist_extracted_csv == True
        assert config.log_level == 'DEBUG'
        assert config.archive_retention_days == 60
        assert config.pipeline_name == 'test_pipeline'
        
    finally:
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)
        reset_config()


def test_load_config_invalid_integer():
    """Test that invalid integer values raise ConfigValidationError."""
    original_env = dict(os.environ)
    
    try:
        os.environ['PG_PORT'] = 'not-a-number'
        reset_config()
        
        with pytest.raises(ConfigValidationError) as exc_info:
            load_config()
        
        assert "Invalid integer value for PG_PORT" in str(exc_info.value)
        
    finally:
        os.environ.clear()
        os.environ.update(original_env)
        reset_config()


def test_load_config_invalid_float():
    """Test that invalid float values raise ConfigValidationError."""
    original_env = dict(os.environ)
    
    try:
        os.environ['MIN_FREE_GB'] = 'not-a-number'
        reset_config()
        
        with pytest.raises(ConfigValidationError) as exc_info:
            load_config()
        
        assert "Invalid float value for MIN_FREE_GB" in str(exc_info.value)
        
    finally:
        os.environ.clear()
        os.environ.update(original_env)
        reset_config()


def test_boolean_conversion():
    """Test boolean environment variable conversion."""
    original_env = dict(os.environ)
    
    try:
        # Test various true values
        for true_val in ['true', 'True', 'TRUE', '1', 'yes', 'YES', 'on', 'ON']:
            os.environ['FAIL_FAST'] = true_val
            reset_config()
            config = load_config()
            assert config.fail_fast == True, f"'{true_val}' should be True"
        
        # Test various false values
        for false_val in ['false', 'False', 'FALSE', '0', 'no', 'NO', 'off', 'OFF']:
            os.environ['FAIL_FAST'] = false_val
            reset_config()
            config = load_config()
            assert config.fail_fast == False, f"'{false_val}' should be False"
        
    finally:
        os.environ.clear()
        os.environ.update(original_env)
        reset_config()