"""Configuration module for USASpending ETL pipeline.

All environment variable access MUST go through this module.
No direct os.environ access allowed elsewhere in the codebase.
"""
import os
from typing import Any, Dict
from dataclasses import dataclass


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


@dataclass
class ETLConfig:
    """ETL pipeline configuration with validation."""
    
    # Database connection
    pg_user: str = "postgres"
    pg_password: str = "admin"
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_dbname: str = "capture_insights"
    
    # File system
    download_dir: str = "data/downloads"
    
    # Processing parameters
    chunk_days: int = 7
    current_days_lookback: int = 2
    max_wait_seconds: int = 3600
    max_retries: int = 5
    fail_fast: bool = True
    
    # Disk guardrails
    min_free_gb: float = 40.0
    target_peak_gb: float = 100.0
    
    # Optional settings
    persist_extracted_csv: bool = False
    log_level: str = "INFO"
    archive_retention_days: int = 90
    pipeline_name: str = "prime_awards_historical"
    
    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection URL."""
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_dbname}"
    
    def validate(self) -> None:
        """Validate configuration rules."""
        errors = []
        
        # MIN_FREE_GB < TARGET_PEAK_GB must hold
        if self.min_free_gb >= self.target_peak_gb:
            errors.append(f"MIN_FREE_GB ({self.min_free_gb}) must be < TARGET_PEAK_GB ({self.target_peak_gb})")
        
        # CHUNK_DAYS between 1 and 31 inclusive
        if not (1 <= self.chunk_days <= 31):
            errors.append(f"CHUNK_DAYS ({self.chunk_days}) must be between 1 and 31")
        
        # CURRENT_DAYS_LOOKBACK between 1 and 14
        if not (1 <= self.current_days_lookback <= 14):
            errors.append(f"CURRENT_DAYS_LOOKBACK ({self.current_days_lookback}) must be between 1 and 14")
        
        # MAX_RETRIES between 1 and 10
        if not (1 <= self.max_retries <= 10):
            errors.append(f"MAX_RETRIES ({self.max_retries}) must be between 1 and 10")
        
        # MAX_WAIT_SECONDS >= 600
        if self.max_wait_seconds < 600:
            errors.append(f"MAX_WAIT_SECONDS ({self.max_wait_seconds}) must be >= 600")
        
        if errors:
            raise ConfigValidationError(f"Configuration validation failed: {'; '.join(errors)}")


def _str_to_bool(value: str) -> bool:
    """Convert string to boolean."""
    return value.lower() in ('true', '1', 'yes', 'on')


def load_config() -> ETLConfig:
    """Load configuration from environment variables with validation."""
    
    def get_env_int(key: str, default: int) -> int:
        """Get integer from environment with default."""
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            raise ConfigValidationError(f"Invalid integer value for {key}: {value}")
    
    def get_env_float(key: str, default: float) -> float:
        """Get float from environment with default."""
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            raise ConfigValidationError(f"Invalid float value for {key}: {value}")
    
    def get_env_bool(key: str, default: bool) -> bool:
        """Get boolean from environment with default."""
        value = os.getenv(key)
        if value is None:
            return default
        return _str_to_bool(value)
    
    config = ETLConfig(
        pg_user=os.getenv("PG_USER", "postgres"),
        pg_password=os.getenv("PG_PASSWORD", "admin"),
        pg_host=os.getenv("PG_HOST", "localhost"),
        pg_port=get_env_int("PG_PORT", 5432),
        pg_dbname=os.getenv("PG_DBNAME", "capture_insights"),
        
        download_dir=os.getenv("DOWNLOAD_DIR", "data/downloads"),
        
        chunk_days=get_env_int("CHUNK_DAYS", 7),
        current_days_lookback=get_env_int("CURRENT_DAYS_LOOKBACK", 2),
        max_wait_seconds=get_env_int("MAX_WAIT_SECONDS", 3600),
        max_retries=get_env_int("MAX_RETRIES", 5),
        fail_fast=get_env_bool("FAIL_FAST", True),
        
        min_free_gb=get_env_float("MIN_FREE_GB", 40.0),
        target_peak_gb=get_env_float("TARGET_PEAK_GB", 100.0),
        
        persist_extracted_csv=get_env_bool("PERSIST_EXTRACTED_CSV", False),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        archive_retention_days=get_env_int("ARCHIVE_RETENTION_DAYS", 90),
        pipeline_name=os.getenv("PIPELINE_NAME", "prime_awards_historical")
    )
    
    # Validate configuration
    config.validate()
    
    return config


# Global config instance
_config: ETLConfig = None


def get_config() -> ETLConfig:
    """Get global configuration instance, loading if necessary."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Reset global config (for testing)."""
    global _config
    _config = None