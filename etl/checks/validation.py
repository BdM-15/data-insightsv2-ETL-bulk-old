"""
Automated Validation Suite

Comprehensive validation system for data quality and pipeline health monitoring.
Provides automated checks for data integrity, schema compliance, and pipeline
operations with configurable rules and alerting.

Constitution adherence:
- SQL-first: Uses database queries for validation checks and analysis
- Fail-fast: Stops validation on critical failures, reports all issues
- Storage-conscious: Efficient validation queries with minimal resource usage
- Modular: Pluggable validation rules and customizable check suites
"""

import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable

import psycopg

from ..config import Config
from ..utils.logging import get_logger
from ..utils.metrics import MetricsCollector


class ValidationRule:
    """
    Defines a single validation rule with execution logic and criteria.
    """
    
    def __init__(self, name: str, description: str, severity: str,
                 check_function: Callable, **kwargs):
        self.name = name
        self.description = description
        self.severity = severity  # 'critical', 'warning', 'info'
        self.check_function = check_function
        self.parameters = kwargs
        self.logger = get_logger(f"ValidationRule.{name}")
        
        # Validate severity
        if severity not in ['critical', 'warning', 'info']:
            raise ValueError(f"Invalid severity '{severity}'. Must be 'critical', 'warning', or 'info'")
            
    def execute(self, validator: 'DataValidator') -> Dict[str, Any]:
        """
        Execute the validation rule.
        
        Args:
            validator: DataValidator instance providing database access
            
        Returns:
            Dict: Validation result with status and details
        """
        result = {
            'rule_name': self.name,
            'description': self.description,
            'severity': self.severity,
            'status': 'unknown',
            'message': '',
            'details': {},
            'executed_at': datetime.now()
        }
        
        try:
            check_result = self.check_function(validator, **self.parameters)
            
            if isinstance(check_result, bool):
                result['status'] = 'passed' if check_result else 'failed'
                result['message'] = f"Validation {'passed' if check_result else 'failed'}"
            elif isinstance(check_result, dict):
                result.update(check_result)
            else:
                result['status'] = 'error'
                result['message'] = f"Invalid check function return type: {type(check_result)}"
                
        except Exception as e:
            result['status'] = 'error'
            result['message'] = f"Validation error: {str(e)}"
            result['details']['error_type'] = type(e).__name__
            result['details']['traceback'] = traceback.format_exc()
            self.logger.error(f"Validation rule '{self.name}' failed: {e}")
            
        return result


class DataValidator:
    """
    Main validation system that executes validation rules and manages results.
    """
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.logger = get_logger(self.__class__.__name__)
        self._connection: Optional[psycopg.Connection] = None
        self.rules: List[ValidationRule] = []
        self.metrics_collector: Optional[MetricsCollector] = None
        
        # Load default validation rules
        self._load_default_rules()
        
    def __enter__(self):
        """Context manager entry."""
        self._connect()
        if self.config.enable_metrics:
            self.metrics_collector = MetricsCollector(self.config)
            self.metrics_collector.__enter__()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.metrics_collector:
            self.metrics_collector.__exit__(exc_type, exc_val, exc_tb)
        if self._connection:
            self._connection.close()
            self._connection = None
            
    def _connect(self):
        """Establish database connection."""
        try:
            self._connection = psycopg.connect(
                host=self.config.database_host,
                port=self.config.database_port,
                dbname=self.config.database_name,
                user=self.config.database_user,
                password=self.config.database_password,
                autocommit=True
            )
            self.logger.debug("Connected to database for validation")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
            
    def get_connection(self) -> psycopg.Connection:
        """Get database connection for validation rules."""
        if not self._connection:
            self._connect()
        return self._connection
        
    def add_rule(self, rule: ValidationRule):
        """Add a custom validation rule."""
        self.rules.append(rule)
        self.logger.info(f"Added validation rule: {rule.name}")
        
    def _load_default_rules(self):
        """Load default validation rules."""
        
        # Schema validation rules
        self.add_rule(ValidationRule(
            name="required_tables_exist",
            description="Check that all required tables exist",
            severity="critical",
            check_function=self._check_required_tables
        ))
        
        self.add_rule(ValidationRule(
            name="table_schemas_valid",
            description="Validate table schemas match expected structure",
            severity="critical",
            check_function=self._check_table_schemas
        ))
        
        # Data quality rules
        self.add_rule(ValidationRule(
            name="awards_data_freshness",
            description="Check that awards data is recent",
            severity="warning",
            check_function=self._check_data_freshness,
            table_name="s3_processed.awards",
            date_column="last_modified_date",
            max_age_hours=48
        ))
        
        self.add_rule(ValidationRule(
            name="no_duplicate_awards",
            description="Check for duplicate award records",
            severity="warning",
            check_function=self._check_duplicates,
            table_name="s3_processed.awards",
            key_columns=["award_id"]
        ))
        
        self.add_rule(ValidationRule(
            name="required_fields_populated",
            description="Check that required fields are not null",
            severity="critical",
            check_function=self._check_required_fields,
            table_name="s3_processed.awards",
            required_fields=["award_id", "federal_action_obligation", "recipient_name"]
        ))
        
        # Pipeline health rules
        self.add_rule(ValidationRule(
            name="recent_pipeline_success",
            description="Check that pipelines have run successfully recently",
            severity="critical",
            check_function=self._check_recent_pipeline_success,
            max_hours_since_success=24
        ))
        
        self.add_rule(ValidationRule(
            name="pipeline_error_rate",
            description="Check pipeline error rates are within acceptable limits",
            severity="warning",
            check_function=self._check_pipeline_error_rate,
            max_error_rate_percent=10,
            lookback_hours=168  # 7 days
        ))
        
        # Data volume rules
        self.add_rule(ValidationRule(
            name="data_volume_anomalies",
            description="Check for unusual data volume changes",
            severity="warning",
            check_function=self._check_data_volume_anomalies,
            table_name="s3_processed.awards",
            max_change_percent=50
        ))
        
        # Storage and performance rules
        self.add_rule(ValidationRule(
            name="storage_usage_check",
            description="Monitor database storage usage",
            severity="warning",
            check_function=self._check_storage_usage,
            max_usage_percent=80
        ))
        
        self.logger.info(f"Loaded {len(self.rules)} default validation rules")
        
    def run_validation_suite(self, rule_names: Optional[List[str]] = None,
                           severity_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the complete validation suite or specific rules.
        
        Args:
            rule_names: Optional list of specific rule names to run
            severity_filter: Optional severity filter ('critical', 'warning', 'info')
            
        Returns:
            Dict: Complete validation results
        """
        execution_id = None
        if self.metrics_collector:
            execution_id = self.metrics_collector.record_pipeline_start(
                "validation_suite", "validation"
            )
            
        results = {
            'execution_id': execution_id,
            'started_at': datetime.now(),
            'completed_at': None,
            'total_rules': 0,
            'rules_passed': 0,
            'rules_failed': 0,
            'rules_error': 0,
            'critical_failures': 0,
            'warning_failures': 0,
            'info_failures': 0,
            'overall_status': 'unknown',
            'rule_results': [],
            'summary': {}
        }
        
        try:
            # Filter rules to run
            rules_to_run = []
            for rule in self.rules:
                if rule_names and rule.name not in rule_names:
                    continue
                if severity_filter and rule.severity != severity_filter:
                    continue
                rules_to_run.append(rule)
                
            results['total_rules'] = len(rules_to_run)
            
            # Execute each rule
            for rule in rules_to_run:
                self.logger.info(f"Executing validation rule: {rule.name}")
                
                rule_result = rule.execute(self)
                results['rule_results'].append(rule_result)
                
                # Update counters
                if rule_result['status'] == 'passed':
                    results['rules_passed'] += 1
                elif rule_result['status'] == 'failed':
                    results['rules_failed'] += 1
                    if rule.severity == 'critical':
                        results['critical_failures'] += 1
                    elif rule.severity == 'warning':
                        results['warning_failures'] += 1
                    elif rule.severity == 'info':
                        results['info_failures'] += 1
                else:  # error
                    results['rules_error'] += 1
                    
            # Determine overall status
            if results['critical_failures'] > 0:
                results['overall_status'] = 'critical'
            elif results['rules_error'] > 0:
                results['overall_status'] = 'error'
            elif results['warning_failures'] > 0:
                results['overall_status'] = 'warning'
            else:
                results['overall_status'] = 'passed'
                
            results['completed_at'] = datetime.now()
            duration = (results['completed_at'] - results['started_at']).total_seconds()
            
            # Create summary
            results['summary'] = {
                'duration_seconds': duration,
                'success_rate': (results['rules_passed'] / results['total_rules'] * 100) if results['total_rules'] > 0 else 0,
                'critical_issues': results['critical_failures'],
                'warnings': results['warning_failures'],
                'errors': results['rules_error']
            }
            
            # Record metrics
            if self.metrics_collector and execution_id:
                self.metrics_collector.record_pipeline_end(
                    execution_id,
                    'completed' if results['overall_status'] in ['passed', 'warning'] else 'failed',
                    results['total_rules']
                )
                
                self.metrics_collector.record_stage_metrics(
                    execution_id,
                    'validation_suite',
                    results['summary']
                )
                
            self.logger.info(
                f"Validation suite completed: {results['overall_status']} "
                f"({results['rules_passed']}/{results['total_rules']} passed, "
                f"{results['critical_failures']} critical, "
                f"{results['warning_failures']} warnings)"
            )
            
        except Exception as e:
            results['overall_status'] = 'error'
            results['completed_at'] = datetime.now()
            self.logger.error(f"Validation suite execution failed: {e}")
            
            if self.metrics_collector and execution_id:
                self.metrics_collector.record_pipeline_end(
                    execution_id, 'failed', error_message=str(e)
                )
                
            raise
            
        return results
        
    # Built-in validation check functions
    
    def _check_required_tables(self, **kwargs) -> Dict[str, Any]:
        """Check that all required tables exist."""
        required_tables = [
            's1_raw.api_responses',
            's2_interim.awards',
            's3_processed.awards',
            's3_processed.pipeline_executions'
        ]
        
        missing_tables = []
        
        with self.get_connection().cursor() as cur:
            for table in required_tables:
                schema, table_name = table.split('.')
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema, table_name))
                
                exists = cur.fetchone()[0]
                if not exists:
                    missing_tables.append(table)
                    
        if missing_tables:
            return {
                'status': 'failed',
                'message': f"Missing required tables: {', '.join(missing_tables)}",
                'details': {'missing_tables': missing_tables}
            }
        else:
            return {
                'status': 'passed',
                'message': f"All {len(required_tables)} required tables exist",
                'details': {'checked_tables': required_tables}
            }
            
    def _check_table_schemas(self, **kwargs) -> Dict[str, Any]:
        """Validate table schemas match expected structure."""
        schema_issues = []
        
        # Define expected schemas for key tables
        expected_schemas = {
            's3_processed.awards': [
                'award_id', 'federal_action_obligation', 'recipient_name',
                'awarding_agency_name', 'last_modified_date', 'semantic_description'
            ]
        }
        
        with self.get_connection().cursor() as cur:
            for table, expected_columns in expected_schemas.items():
                schema, table_name = table.split('.')
                
                # Get actual columns
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table_name))
                
                actual_columns = [row[0] for row in cur.fetchall()]
                
                # Check for missing columns
                missing_columns = set(expected_columns) - set(actual_columns)
                if missing_columns:
                    schema_issues.append({
                        'table': table,
                        'issue': 'missing_columns',
                        'columns': list(missing_columns)
                    })
                    
        if schema_issues:
            return {
                'status': 'failed',
                'message': f"Schema validation failed for {len(schema_issues)} tables",
                'details': {'schema_issues': schema_issues}
            }
        else:
            return {
                'status': 'passed',
                'message': "All table schemas are valid",
                'details': {'checked_tables': list(expected_schemas.keys())}
            }
            
    def _check_data_freshness(self, table_name: str, date_column: str,
                             max_age_hours: int, **kwargs) -> Dict[str, Any]:
        """Check that data is recent."""
        with self.get_connection().cursor() as cur:
            cur.execute(f"""
                SELECT 
                    MAX({date_column}) as latest_date,
                    EXTRACT(EPOCH FROM (NOW() - MAX({date_column}))) / 3600 as hours_old
                FROM {table_name}
            """, ())
            
            result = cur.fetchone()
            if not result or not result[0]:
                return {
                    'status': 'failed',
                    'message': f"No data found in {table_name}",
                    'details': {'table_name': table_name}
                }
                
            latest_date, hours_old = result
            
            if hours_old > max_age_hours:
                return {
                    'status': 'failed',
                    'message': f"Data is {hours_old:.1f} hours old (max {max_age_hours})",
                    'details': {
                        'latest_date': latest_date,
                        'hours_old': hours_old,
                        'max_age_hours': max_age_hours
                    }
                }
            else:
                return {
                    'status': 'passed',
                    'message': f"Data is fresh ({hours_old:.1f} hours old)",
                    'details': {
                        'latest_date': latest_date,
                        'hours_old': hours_old
                    }
                }
                
    def _check_duplicates(self, table_name: str, key_columns: List[str],
                         **kwargs) -> Dict[str, Any]:
        """Check for duplicate records."""
        key_cols_str = ', '.join(key_columns)
        
        with self.get_connection().cursor() as cur:
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT ({key_cols_str})) as unique_records,
                    COUNT(*) - COUNT(DISTINCT ({key_cols_str})) as duplicates
                FROM {table_name}
            """, ())
            
            result = cur.fetchone()
            total_records, unique_records, duplicates = result
            
            if duplicates > 0:
                duplicate_rate = (duplicates / total_records) * 100 if total_records > 0 else 0
                return {
                    'status': 'failed',
                    'message': f"Found {duplicates} duplicate records ({duplicate_rate:.2f}%)",
                    'details': {
                        'total_records': total_records,
                        'unique_records': unique_records,
                        'duplicates': duplicates,
                        'duplicate_rate_percent': duplicate_rate
                    }
                }
            else:
                return {
                    'status': 'passed',
                    'message': f"No duplicates found in {total_records} records",
                    'details': {
                        'total_records': total_records,
                        'unique_records': unique_records
                    }
                }
                
    def _check_required_fields(self, table_name: str, required_fields: List[str],
                              **kwargs) -> Dict[str, Any]:
        """Check that required fields are not null."""
        null_counts = {}
        
        with self.get_connection().cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cur.fetchone()[0]
            
            for field in required_fields:
                cur.execute(f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE {field} IS NULL
                """, ())
                
                null_count = cur.fetchone()[0]
                null_counts[field] = null_count
                
        # Check for any nulls in required fields
        fields_with_nulls = {field: count for field, count in null_counts.items() if count > 0}
        
        if fields_with_nulls:
            return {
                'status': 'failed',
                'message': f"Required fields have null values: {list(fields_with_nulls.keys())}",
                'details': {
                    'total_records': total_records,
                    'null_counts': null_counts,
                    'fields_with_nulls': fields_with_nulls
                }
            }
        else:
            return {
                'status': 'passed',
                'message': f"All required fields are populated in {total_records} records",
                'details': {
                    'total_records': total_records,
                    'checked_fields': required_fields
                }
            }
            
    def _check_recent_pipeline_success(self, max_hours_since_success: int,
                                      **kwargs) -> Dict[str, Any]:
        """Check that pipelines have run successfully recently."""
        with self.get_connection().cursor() as cur:
            cur.execute("""
                SELECT 
                    pipeline_name,
                    MAX(start_time) as last_success,
                    EXTRACT(EPOCH FROM (NOW() - MAX(start_time))) / 3600 as hours_since_success
                FROM s3_processed.pipeline_executions
                WHERE status = 'completed'
                GROUP BY pipeline_name
            """, ())
            
            results = cur.fetchall()
            
            if not results:
                return {
                    'status': 'failed',
                    'message': "No successful pipeline runs found",
                    'details': {}
                }
                
            stale_pipelines = []
            for pipeline_name, last_success, hours_since in results:
                if hours_since > max_hours_since_success:
                    stale_pipelines.append({
                        'pipeline': pipeline_name,
                        'last_success': last_success,
                        'hours_since': hours_since
                    })
                    
            if stale_pipelines:
                return {
                    'status': 'failed',
                    'message': f"{len(stale_pipelines)} pipelines haven't run successfully recently",
                    'details': {'stale_pipelines': stale_pipelines}
                }
            else:
                return {
                    'status': 'passed',
                    'message': f"All {len(results)} pipelines have run successfully recently",
                    'details': {'pipeline_status': [
                        {'pipeline': p, 'last_success': ls, 'hours_since': hs}
                        for p, ls, hs in results
                    ]}
                }
                
    def _check_pipeline_error_rate(self, max_error_rate_percent: float,
                                  lookback_hours: int, **kwargs) -> Dict[str, Any]:
        """Check pipeline error rates are within acceptable limits."""
        with self.get_connection().cursor() as cur:
            cur.execute("""
                SELECT 
                    pipeline_name,
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                    (SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)::float / COUNT(*) * 100) as error_rate_percent
                FROM s3_processed.pipeline_executions
                WHERE start_time >= NOW() - INTERVAL '%s hours'
                GROUP BY pipeline_name
                HAVING COUNT(*) >= 3  -- Only check pipelines with at least 3 runs
            """, (lookback_hours,))
            
            results = cur.fetchall()
            
            high_error_pipelines = []
            for pipeline_name, total_runs, failed_runs, error_rate in results:
                if error_rate > max_error_rate_percent:
                    high_error_pipelines.append({
                        'pipeline': pipeline_name,
                        'total_runs': total_runs,
                        'failed_runs': failed_runs,
                        'error_rate_percent': error_rate
                    })
                    
            if high_error_pipelines:
                return {
                    'status': 'failed',
                    'message': f"{len(high_error_pipelines)} pipelines have high error rates",
                    'details': {'high_error_pipelines': high_error_pipelines}
                }
            else:
                return {
                    'status': 'passed',
                    'message': f"All {len(results)} pipelines have acceptable error rates",
                    'details': {'pipeline_stats': [
                        {'pipeline': p, 'total_runs': tr, 'failed_runs': fr, 'error_rate_percent': er}
                        for p, tr, fr, er in results
                    ]}
                }
                
    def _check_data_volume_anomalies(self, table_name: str, max_change_percent: float,
                                    **kwargs) -> Dict[str, Any]:
        """Check for unusual data volume changes."""
        with self.get_connection().cursor() as cur:
            # Get record counts for the last few days
            cur.execute(f"""
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as record_count
                FROM {table_name}
                WHERE created_at >= NOW() - INTERVAL '7 days'
                  AND created_at IS NOT NULL
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                LIMIT 3
            """, ())
            
            results = cur.fetchall()
            
            if len(results) < 2:
                return {
                    'status': 'warning',
                    'message': "Insufficient data to check volume anomalies",
                    'details': {'available_days': len(results)}
                }
                
            # Calculate change between most recent days
            latest_count = results[0][1]
            previous_count = results[1][1]
            
            if previous_count == 0:
                change_percent = 100 if latest_count > 0 else 0
            else:
                change_percent = ((latest_count - previous_count) / previous_count) * 100
                
            if abs(change_percent) > max_change_percent:
                return {
                    'status': 'warning',
                    'message': f"Data volume changed by {change_percent:+.1f}% (max {max_change_percent}%)",
                    'details': {
                        'latest_count': latest_count,
                        'previous_count': previous_count,
                        'change_percent': change_percent,
                        'daily_counts': [(str(date), count) for date, count in results]
                    }
                }
            else:
                return {
                    'status': 'passed',
                    'message': f"Data volume change is normal ({change_percent:+.1f}%)",
                    'details': {
                        'latest_count': latest_count,
                        'previous_count': previous_count,
                        'change_percent': change_percent
                    }
                }
                
    def _check_storage_usage(self, max_usage_percent: float, **kwargs) -> Dict[str, Any]:
        """Monitor database storage usage."""
        with self.get_connection().cursor() as cur:
            cur.execute("""
                SELECT 
                    schemaname,
                    SUM(pg_total_relation_size(schemaname||'.'||tablename)) as total_size_bytes
                FROM pg_tables
                WHERE schemaname IN ('s1_raw', 's2_interim', 's3_processed')
                GROUP BY schemaname
                ORDER BY total_size_bytes DESC
            """, ())
            
            schema_sizes = cur.fetchall()
            total_size_bytes = sum(size for _, size in schema_sizes)
            total_size_mb = total_size_bytes / (1024 * 1024)
            
            # Get database size limit (simplified check)
            # In a real implementation, you'd check actual disk usage
            estimated_limit_mb = 10000  # 10GB limit for example
            usage_percent = (total_size_mb / estimated_limit_mb) * 100
            
            schema_details = [
                {
                    'schema': schema,
                    'size_mb': size_bytes / (1024 * 1024),
                    'size_percent': (size_bytes / total_size_bytes) * 100 if total_size_bytes > 0 else 0
                }
                for schema, size_bytes in schema_sizes
            ]
            
            if usage_percent > max_usage_percent:
                return {
                    'status': 'warning',
                    'message': f"Database storage usage is {usage_percent:.1f}% (max {max_usage_percent}%)",
                    'details': {
                        'total_size_mb': total_size_mb,
                        'usage_percent': usage_percent,
                        'schema_breakdown': schema_details
                    }
                }
            else:
                return {
                    'status': 'passed',
                    'message': f"Database storage usage is acceptable ({usage_percent:.1f}%)",
                    'details': {
                        'total_size_mb': total_size_mb,
                        'usage_percent': usage_percent,
                        'schema_breakdown': schema_details
                    }
                }