"""
Enhanced Configuration Management System

Provides comprehensive configuration management with environment-specific settings,
validation, inheritance, and secure credential handling. Supports multiple
configuration sources and runtime configuration updates.

Constitution adherence:
- SQL-first: Database configuration stored and validated via SQL
- Fail-fast: Configuration validation fails immediately on invalid settings
- Storage-conscious: Efficient configuration storage and caching
- Modular: Pluggable configuration providers and validation rules
"""

import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import tempfile

import psycopg

from ..utils.logging import get_logger


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass


class ConfigurationProvider:
    """
    Base class for configuration providers (file, database, environment).
    """
    
    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(f"ConfigProvider.{name}")
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from this provider."""
        raise NotImplementedError
        
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Save configuration to this provider."""
        raise NotImplementedError
        
    def is_available(self) -> bool:
        """Check if this provider is available."""
        raise NotImplementedError


class EnvironmentConfigProvider(ConfigurationProvider):
    """Configuration provider for environment variables."""
    
    def __init__(self):
        super().__init__("environment")
        self.prefix = "ETL_"
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(self.prefix):
                config_key = key[len(self.prefix):].lower()
                
                # Convert string values to appropriate types
                if value.lower() in ('true', 'false'):
                    config[config_key] = value.lower() == 'true'
                elif value.isdigit():
                    config[config_key] = int(value)
                else:
                    config[config_key] = value
                    
        return config
        
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Environment variables can't be saved directly."""
        return False
        
    def is_available(self) -> bool:
        """Environment is always available."""
        return True


class FileConfigProvider(ConfigurationProvider):
    """Configuration provider for JSON/YAML files."""
    
    def __init__(self, file_path: Union[str, Path]):
        super().__init__("file")
        self.file_path = Path(file_path)
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        if not self.file_path.exists():
            return {}
            
        try:
            with open(self.file_path, 'r') as f:
                if self.file_path.suffix.lower() == '.json':
                    return json.load(f)
                elif self.file_path.suffix.lower() in ('.yml', '.yaml'):
                    import yaml
                    return yaml.safe_load(f)
                else:
                    raise ConfigurationError(f"Unsupported file format: {self.file_path.suffix}")
                    
        except Exception as e:
            self.logger.error(f"Failed to load config from {self.file_path}: {e}")
            raise ConfigurationError(f"Failed to load config file: {e}")
            
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Save configuration to file."""
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.file_path, 'w') as f:
                if self.file_path.suffix.lower() == '.json':
                    json.dump(config, f, indent=2, default=str)
                elif self.file_path.suffix.lower() in ('.yml', '.yaml'):
                    import yaml
                    yaml.dump(config, f, default_flow_style=False)
                else:
                    raise ConfigurationError(f"Unsupported file format: {self.file_path.suffix}")
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save config to {self.file_path}: {e}")
            return False
            
    def is_available(self) -> bool:
        """Check if file is available for reading/writing."""
        return self.file_path.parent.exists() or self.file_path.exists()


class DatabaseConfigProvider(ConfigurationProvider):
    """Configuration provider for database storage."""
    
    def __init__(self, connection_string: str):
        super().__init__("database")
        self.connection_string = connection_string
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from database."""
        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT config_key, config_value, value_type
                        FROM s3_processed.configuration
                        WHERE is_active = true
                        ORDER BY config_key
                    """)
                    
                    config = {}
                    for key, value, value_type in cur.fetchall():
                        # Convert string value to appropriate type
                        if value_type == 'boolean':
                            config[key] = value.lower() == 'true'
                        elif value_type == 'integer':
                            config[key] = int(value)
                        elif value_type == 'float':
                            config[key] = float(value)
                        elif value_type == 'json':
                            config[key] = json.loads(value)
                        else:
                            config[key] = value
                            
                    return config
                    
        except Exception as e:
            self.logger.warning(f"Failed to load config from database: {e}")
            return {}
            
    def save_config(self, config: Dict[str, Any]) -> bool:
        """Save configuration to database."""
        try:
            with psycopg.connect(self.connection_string) as conn:
                with conn.cursor() as cur:
                    # Create configuration table if it doesn't exist
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS s3_processed.configuration (
                            id SERIAL PRIMARY KEY,
                            config_key VARCHAR(255) UNIQUE NOT NULL,
                            config_value TEXT NOT NULL,
                            value_type VARCHAR(50) NOT NULL,
                            description TEXT,
                            is_active BOOLEAN DEFAULT true,
                            created_at TIMESTAMP DEFAULT NOW(),
                            updated_at TIMESTAMP DEFAULT NOW()
                        )
                    """)
                    
                    # Save each config item
                    for key, value in config.items():
                        # Determine value type
                        if isinstance(value, bool):
                            value_type = 'boolean'
                            value_str = str(value).lower()
                        elif isinstance(value, int):
                            value_type = 'integer'
                            value_str = str(value)
                        elif isinstance(value, float):
                            value_type = 'float'
                            value_str = str(value)
                        elif isinstance(value, (dict, list)):
                            value_type = 'json'
                            value_str = json.dumps(value)
                        else:
                            value_type = 'string'
                            value_str = str(value)
                            
                        cur.execute("""
                            INSERT INTO s3_processed.configuration 
                            (config_key, config_value, value_type, updated_at)
                            VALUES (%s, %s, %s, NOW())
                            ON CONFLICT (config_key) DO UPDATE SET
                                config_value = EXCLUDED.config_value,
                                value_type = EXCLUDED.value_type,
                                updated_at = NOW()
                        """, (key, value_str, value_type))
                        
                    conn.commit()
                    return True
                    
        except Exception as e:
            self.logger.error(f"Failed to save config to database: {e}")
            return False
            
    def is_available(self) -> bool:
        """Check if database is available."""
        try:
            with psycopg.connect(self.connection_string) as conn:
                return True
        except:
            return False


class ConfigurationValidator:
    """Validates configuration values and structure."""
    
    def __init__(self):
        self.logger = get_logger("ConfigValidator")
        
        # Define configuration schema
        self.schema = {
            'database_host': {'type': str, 'required': True},
            'database_port': {'type': int, 'required': True, 'min': 1, 'max': 65535},
            'database_name': {'type': str, 'required': True},
            'database_user': {'type': str, 'required': True},
            'database_password': {'type': str, 'required': True, 'sensitive': True},
            'work_dir': {'type': str, 'required': True},
            'api_key': {'type': str, 'required': True, 'sensitive': True},
            'api_base_url': {'type': str, 'required': True},
            'chunk_size': {'type': int, 'required': False, 'min': 1000, 'max': 100000, 'default': 10000},
            'max_retries': {'type': int, 'required': False, 'min': 0, 'max': 10, 'default': 3},
            'disk_limit_gb': {'type': int, 'required': False, 'min': 1, 'max': 1000, 'default': 50},
            'enable_metrics': {'type': bool, 'required': False, 'default': True},
            'log_level': {'type': str, 'required': False, 'choices': ['DEBUG', 'INFO', 'WARNING', 'ERROR'], 'default': 'INFO'}
        }
        
    def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate configuration against schema.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Dict: Validated configuration with defaults applied
            
        Raises:
            ConfigurationError: If validation fails
        """
        validated_config = {}
        errors = []
        
        # Check all schema fields
        for field_name, field_spec in self.schema.items():
            value = config.get(field_name)
            
            # Check required fields
            if field_spec.get('required', False) and value is None:
                errors.append(f"Required field '{field_name}' is missing")
                continue
                
            # Apply defaults
            if value is None and 'default' in field_spec:
                value = field_spec['default']
                
            # Skip validation if still None (optional field without default)
            if value is None:
                continue
                
            # Type validation
            expected_type = field_spec['type']
            if not isinstance(value, expected_type):
                try:
                    # Try to convert
                    if expected_type == int:
                        value = int(value)
                    elif expected_type == float:
                        value = float(value)
                    elif expected_type == bool:
                        if isinstance(value, str):
                            value = value.lower() in ('true', '1', 'yes', 'on')
                        else:
                            value = bool(value)
                    elif expected_type == str:
                        value = str(value)
                    else:
                        errors.append(f"Field '{field_name}' has invalid type. Expected {expected_type.__name__}")
                        continue
                except (ValueError, TypeError):
                    errors.append(f"Field '{field_name}' cannot be converted to {expected_type.__name__}")
                    continue
                    
            # Range validation for numbers
            if isinstance(value, (int, float)):
                if 'min' in field_spec and value < field_spec['min']:
                    errors.append(f"Field '{field_name}' ({value}) is below minimum ({field_spec['min']})")
                    continue
                if 'max' in field_spec and value > field_spec['max']:
                    errors.append(f"Field '{field_name}' ({value}) is above maximum ({field_spec['max']})")
                    continue
                    
            # Choice validation
            if 'choices' in field_spec and value not in field_spec['choices']:
                errors.append(f"Field '{field_name}' ({value}) not in allowed choices: {field_spec['choices']}")
                continue
                
            validated_config[field_name] = value
            
        # Check for unknown fields
        unknown_fields = set(config.keys()) - set(self.schema.keys())
        if unknown_fields:
            self.logger.warning(f"Unknown configuration fields: {unknown_fields}")
            
        if errors:
            raise ConfigurationError(f"Configuration validation failed: {'; '.join(errors)}")
            
        return validated_config
        
    def validate_database_connection(self, config: Dict[str, Any]) -> bool:
        """
        Validate database connection using configuration.
        
        Args:
            config: Configuration containing database settings
            
        Returns:
            bool: True if connection successful
            
        Raises:
            ConfigurationError: If connection fails
        """
        try:
            connection_string = (
                f"host={config['database_host']} "
                f"port={config['database_port']} "
                f"dbname={config['database_name']} "
                f"user={config['database_user']} "
                f"password={config['database_password']}"
            )
            
            with psycopg.connect(connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                    
            self.logger.info("Database connection validation successful")
            return True
            
        except Exception as e:
            raise ConfigurationError(f"Database connection validation failed: {e}")
            
    def validate_work_directory(self, config: Dict[str, Any]) -> bool:
        """
        Validate work directory is accessible.
        
        Args:
            config: Configuration containing work_dir setting
            
        Returns:
            bool: True if directory is accessible
            
        Raises:
            ConfigurationError: If directory validation fails
        """
        work_dir = Path(config['work_dir'])
        
        try:
            # Create directory if it doesn't exist
            work_dir.mkdir(parents=True, exist_ok=True)
            
            # Test write access
            test_file = work_dir / '.test_write'
            with open(test_file, 'w') as f:
                f.write('test')
            test_file.unlink()
            
            self.logger.info(f"Work directory validation successful: {work_dir}")
            return True
            
        except Exception as e:
            raise ConfigurationError(f"Work directory validation failed: {e}")


class EnhancedConfigurationManager:
    """
    Advanced configuration management system with multiple providers,
    validation, inheritance, and secure credential handling.
    """
    
    def __init__(self, environment: str = 'development'):
        self.environment = environment
        self.logger = get_logger("ConfigManager")
        self.validator = ConfigurationValidator()
        self.providers: List[ConfigurationProvider] = []
        self._config_cache: Optional[Dict[str, Any]] = None
        self._last_reload = None
        
        # Initialize providers
        self._initialize_providers()
        
    def _initialize_providers(self):
        """Initialize configuration providers in priority order."""
        
        # 1. Environment variables (highest priority)
        env_provider = EnvironmentConfigProvider()
        self.providers.append(env_provider)
        
        # 2. Environment-specific config file
        env_config_file = Path(f"config/{self.environment}.json")
        if env_config_file.exists() or env_config_file.parent.exists():
            self.providers.append(FileConfigProvider(env_config_file))
            
        # 3. Default config file
        default_config_file = Path("config/default.json")
        if default_config_file.exists() or default_config_file.parent.exists():
            self.providers.append(FileConfigProvider(default_config_file))
            
        # 4. Database provider (will be added after initial config load)
        
        self.logger.info(f"Initialized {len(self.providers)} configuration providers")
        
    def load_configuration(self, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load configuration from all providers with proper inheritance.
        
        Args:
            force_reload: Force reload even if cache is available
            
        Returns:
            Dict: Merged and validated configuration
        """
        if self._config_cache and not force_reload:
            return self._config_cache
            
        merged_config = {}
        
        # Load from providers in reverse priority order (lowest to highest)
        for provider in reversed(self.providers):
            if not provider.is_available():
                self.logger.debug(f"Provider '{provider.name}' not available, skipping")
                continue
                
            try:
                provider_config = provider.load_config()
                
                # Merge configuration (higher priority overrides lower)
                for key, value in provider_config.items():
                    merged_config[key] = value
                    
                self.logger.debug(f"Loaded {len(provider_config)} settings from '{provider.name}'")
                
            except Exception as e:
                self.logger.warning(f"Failed to load config from '{provider.name}': {e}")
                
        # Add database provider if we have database connection info
        if all(key in merged_config for key in ['database_host', 'database_port', 'database_name', 'database_user', 'database_password']):
            connection_string = (
                f"host={merged_config['database_host']} "
                f"port={merged_config['database_port']} "
                f"dbname={merged_config['database_name']} "
                f"user={merged_config['database_user']} "
                f"password={merged_config['database_password']}"
            )
            
            db_provider = DatabaseConfigProvider(connection_string)
            if db_provider.is_available():
                try:
                    db_config = db_provider.load_config()
                    merged_config.update(db_config)
                    
                    # Add to providers list if not already there
                    if not any(isinstance(p, DatabaseConfigProvider) for p in self.providers):
                        self.providers.insert(1, db_provider)  # Insert as high priority
                        
                    self.logger.debug(f"Loaded {len(db_config)} settings from database")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to load config from database: {e}")
                    
        # Validate merged configuration
        try:
            validated_config = self.validator.validate_config(merged_config)
            
            # Cache the validated configuration
            self._config_cache = validated_config
            self._last_reload = datetime.now()
            
            self.logger.info(f"Configuration loaded successfully: {len(validated_config)} settings")
            return validated_config
            
        except ConfigurationError as e:
            self.logger.error(f"Configuration validation failed: {e}")
            raise
            
    def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a specific configuration setting.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        config = self.load_configuration()
        return config.get(key, default)
        
    def set_setting(self, key: str, value: Any, persist: bool = True) -> bool:
        """
        Set a configuration setting.
        
        Args:
            key: Configuration key
            value: New value
            persist: Whether to persist the change to storage
            
        Returns:
            bool: True if successful
        """
        # Update cache
        if self._config_cache:
            self._config_cache[key] = value
        else:
            self._config_cache = {key: value}
            
        if persist:
            # Try to save to the first writable provider
            for provider in self.providers:
                if hasattr(provider, 'save_config'):
                    try:
                        if provider.save_config({key: value}):
                            self.logger.info(f"Setting '{key}' saved to '{provider.name}'")
                            return True
                    except Exception as e:
                        self.logger.warning(f"Failed to save setting to '{provider.name}': {e}")
                        
            self.logger.warning(f"Failed to persist setting '{key}' to any provider")
            return False
            
        return True
        
    def validate_current_configuration(self) -> Dict[str, Any]:
        """
        Perform comprehensive validation of current configuration.
        
        Returns:
            Dict: Validation results
        """
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'tests': []
        }
        
        try:
            config = self.load_configuration()
            
            # Test 1: Schema validation (already done in load_configuration)
            validation_results['tests'].append({
                'name': 'Schema Validation',
                'status': 'passed',
                'message': 'Configuration schema is valid'
            })
            
            # Test 2: Database connection
            try:
                self.validator.validate_database_connection(config)
                validation_results['tests'].append({
                    'name': 'Database Connection',
                    'status': 'passed',
                    'message': 'Database connection successful'
                })
            except ConfigurationError as e:
                validation_results['valid'] = False
                validation_results['errors'].append(str(e))
                validation_results['tests'].append({
                    'name': 'Database Connection',
                    'status': 'failed',
                    'message': str(e)
                })
                
            # Test 3: Work directory
            try:
                self.validator.validate_work_directory(config)
                validation_results['tests'].append({
                    'name': 'Work Directory',
                    'status': 'passed',
                    'message': 'Work directory is accessible'
                })
            except ConfigurationError as e:
                validation_results['valid'] = False
                validation_results['errors'].append(str(e))
                validation_results['tests'].append({
                    'name': 'Work Directory',
                    'status': 'failed',
                    'message': str(e)
                })
                
            # Test 4: API connectivity (basic URL validation)
            api_url = config.get('api_base_url')
            if api_url:
                if api_url.startswith(('http://', 'https://')):
                    validation_results['tests'].append({
                        'name': 'API URL Format',
                        'status': 'passed',
                        'message': 'API URL format is valid'
                    })
                else:
                    validation_results['warnings'].append('API URL should start with http:// or https://')
                    validation_results['tests'].append({
                        'name': 'API URL Format',
                        'status': 'warning',
                        'message': 'API URL format may be invalid'
                    })
                    
        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Configuration loading failed: {e}")
            
        return validation_results
        
    def get_configuration_summary(self) -> Dict[str, Any]:
        """
        Get a summary of current configuration (excluding sensitive values).
        
        Returns:
            Dict: Configuration summary
        """
        try:
            config = self.load_configuration()
            
            # Mask sensitive values
            masked_config = {}
            for key, value in config.items():
                field_spec = self.validator.schema.get(key, {})
                if field_spec.get('sensitive', False):
                    masked_config[key] = '***MASKED***'
                else:
                    masked_config[key] = value
                    
            return {
                'environment': self.environment,
                'providers': [p.name for p in self.providers if p.is_available()],
                'last_reload': self._last_reload,
                'setting_count': len(config),
                'settings': masked_config
            }
            
        except Exception as e:
            return {
                'environment': self.environment,
                'error': str(e),
                'providers': [p.name for p in self.providers],
                'last_reload': self._last_reload
            }
            
    def export_configuration(self, file_path: Union[str, Path], 
                           include_sensitive: bool = False) -> bool:
        """
        Export configuration to a file.
        
        Args:
            file_path: Path to export file
            include_sensitive: Whether to include sensitive values
            
        Returns:
            bool: True if export successful
        """
        try:
            config = self.load_configuration()
            
            if not include_sensitive:
                # Filter out sensitive values
                filtered_config = {}
                for key, value in config.items():
                    field_spec = self.validator.schema.get(key, {})
                    if not field_spec.get('sensitive', False):
                        filtered_config[key] = value
                config = filtered_config
                
            export_data = {
                'exported_at': datetime.now().isoformat(),
                'environment': self.environment,
                'configuration': config
            }
            
            file_provider = FileConfigProvider(file_path)
            return file_provider.save_config(export_data)
            
        except Exception as e:
            self.logger.error(f"Failed to export configuration: {e}")
            return False