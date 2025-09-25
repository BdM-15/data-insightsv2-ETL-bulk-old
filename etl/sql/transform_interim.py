"""
Interim transform runner for SQL-based ETL transformations.

This module executes SQL transformation scripts sequentially to process data from
s1_raw layer to s2_interim layer with proper error handling and transaction management.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from datetime import datetime
import re

from ..config import get_config
from ..utils.logging import get_logger
from ..staging.raw_loader import get_db_connection
from ..staging.fail_fast import FailFastController, ErrorSeverity

logger = get_logger(__name__)


class TransformError(Exception):
    """Raised when SQL transformation fails."""
    pass


class SQLScriptError(TransformError):
    """Raised when SQL script execution fails."""
    
    def __init__(self, message: str, script_path: str, sql_error: Optional[str] = None):
        super().__init__(message)
        self.script_path = script_path
        self.sql_error = sql_error


class InterimTransformRunner:
    """
    Executes SQL transformation scripts for interim layer processing.
    
    Features:
    - Sequential execution of SQL scripts in defined order
    - Transaction management with rollback on errors
    - Progress tracking and timing metrics
    - SQL script validation and error reporting
    - Parameterized script execution
    - Dependency validation between scripts
    """
    
    def __init__(self, sql_dir: Optional[Path] = None, fail_fast_controller: Optional[FailFastController] = None):
        """
        Initialize interim transform runner.
        
        Args:
            sql_dir: Directory containing SQL scripts (defaults to sql/10_s2_interim)
            fail_fast_controller: Optional fail-fast controller for error handling
        """
        # Default to interim SQL directory
        if sql_dir is None:
            sql_dir = Path(__file__).parent.parent.parent / "sql" / "10_s2_interim"
        
        self.sql_dir = Path(sql_dir)
        self.fail_fast_controller = fail_fast_controller
        
        # Execution statistics
        self.stats = {
            'scripts_executed': 0,
            'scripts_failed': 0,
            'total_duration_seconds': 0,
            'start_time': None,
            'end_time': None,
            'script_timings': {}
        }
        
        logger.debug(f"Initialized InterimTransformRunner: sql_dir={self.sql_dir}")
    
    def execute_transform_batch(self, script_pattern: str = "*.sql", parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a batch of SQL transformation scripts.
        
        Args:
            script_pattern: Glob pattern for script files (defaults to "*.sql")
            parameters: Optional parameters to pass to SQL scripts
            
        Returns:
            Dictionary with execution results and statistics
            
        Raises:
            TransformError: If transformation fails
        """
        self.stats['start_time'] = datetime.now()
        
        try:
            # Find and sort script files
            script_files = self._find_script_files(script_pattern)
            
            if not script_files:
                raise TransformError(f"No SQL scripts found matching pattern '{script_pattern}' in {self.sql_dir}")
            
            logger.info(f"Starting interim transform batch: {len(script_files)} scripts")
            
            # Execute scripts sequentially
            results = []
            for script_file in script_files:
                result = self._execute_single_script(script_file, parameters)
                results.append(result)
            
            self.stats['end_time'] = datetime.now()
            self.stats['total_duration_seconds'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            logger.info(f"Transform batch completed: {self.stats['scripts_executed']} scripts, "
                       f"{self.stats['total_duration_seconds']:.1f}s total")
            
            return {
                'success': True,
                'scripts_executed': self.stats['scripts_executed'],
                'scripts_failed': self.stats['scripts_failed'],
                'duration_seconds': self.stats['total_duration_seconds'],
                'script_results': results,
                'statistics': self.stats.copy()
            }
            
        except Exception as e:
            self.stats['end_time'] = datetime.now()
            
            if self.fail_fast_controller:
                self.fail_fast_controller.handle_error(e, {'operation': 'interim_transform_batch'})
            
            logger.error(f"Transform batch failed: {e}")
            raise TransformError(f"Batch execution failed: {e}")
    
    def execute_single_transform(self, script_name: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a single SQL transformation script.
        
        Args:
            script_name: Name of the SQL script file
            parameters: Optional parameters to pass to SQL script
            
        Returns:
            Dictionary with execution results
            
        Raises:
            TransformError: If script execution fails
        """
        script_path = self.sql_dir / script_name
        
        if not script_path.exists():
            raise TransformError(f"SQL script not found: {script_path}")
        
        return self._execute_single_script(script_path, parameters)
    
    def _find_script_files(self, pattern: str) -> List[Path]:
        """Find and sort SQL script files by name."""
        if not self.sql_dir.exists():
            raise TransformError(f"SQL directory does not exist: {self.sql_dir}")
        
        script_files = list(self.sql_dir.glob(pattern))
        
        # Sort by filename to ensure consistent execution order
        script_files.sort(key=lambda p: p.name)
        
        logger.debug(f"Found {len(script_files)} SQL scripts: {[f.name for f in script_files]}")
        return script_files
    
    def _execute_single_script(self, script_path: Path, parameters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute a single SQL script with transaction management."""
        script_start = datetime.now()
        
        logger.info(f"Executing SQL script: {script_path.name}")
        
        try:
            # Read SQL content
            sql_content = self._read_sql_file(script_path)
            
            # Execute SQL in transaction
            with get_db_connection() as conn:
                with conn.transaction():
                    result = self._execute_sql(conn, sql_content, parameters, script_path)
            
            script_end = datetime.now()
            duration = (script_end - script_start).total_seconds()
            
            # Update statistics
            self.stats['scripts_executed'] += 1
            self.stats['script_timings'][script_path.name] = duration
            
            logger.info(f"Script {script_path.name} completed successfully ({duration:.2f}s)")
            
            return {
                'script_name': script_path.name,
                'success': True,
                'duration_seconds': duration,
                'rows_affected': result.get('rows_affected', 0),
                'messages': result.get('messages', [])
            }
            
        except Exception as e:
            script_end = datetime.now()
            duration = (script_end - script_start).total_seconds()
            
            # Update error statistics
            self.stats['scripts_failed'] += 1
            self.stats['script_timings'][script_path.name] = duration
            
            error_msg = f"Script {script_path.name} failed: {e}"
            logger.error(error_msg)
            
            # Handle error through fail-fast controller if available
            if self.fail_fast_controller:
                self.fail_fast_controller.handle_error(
                    e, 
                    {'script_path': str(script_path), 'duration': duration},
                    operation='sql_script_execution'
                )
            
            raise SQLScriptError(error_msg, str(script_path), str(e))
    
    def _read_sql_file(self, script_path: Path) -> str:
        """Read and validate SQL file content."""
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            if not content:
                raise TransformError(f"SQL script is empty: {script_path}")
            
            # Basic SQL validation
            self._validate_sql_content(content, script_path)
            
            return content
            
        except Exception as e:
            raise TransformError(f"Failed to read SQL script {script_path}: {e}")
    
    def _validate_sql_content(self, sql_content: str, script_path: Path) -> None:
        """Basic validation of SQL content."""
        # Check for dangerous operations in production
        dangerous_patterns = [
            r'\bDROP\s+DATABASE\b',
            r'\bTRUNCATE\s+TABLE\b.*(?!capture_insights\.)',  # Allow truncate in our schema
            r'\bDELETE\s+FROM\b.*(?!capture_insights\.)',     # Allow delete in our schema
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, sql_content, re.IGNORECASE):
                logger.warning(f"Potentially dangerous SQL operation detected in {script_path.name}: {pattern}")
        
        # Ensure script targets correct schema
        if 'capture_insights' not in sql_content.lower():
            logger.warning(f"SQL script {script_path.name} may not target capture_insights schema")
    
    def _execute_sql(self, conn: psycopg.Connection, sql_content: str, parameters: Optional[Dict[str, Any]], script_path: Path) -> Dict[str, Any]:
        """Execute SQL content with parameter substitution."""
        
        try:
            # Simple parameter substitution (for basic cases)
            if parameters:
                for key, value in parameters.items():
                    placeholder = f"${{{key}}}"
                    if placeholder in sql_content:
                        # Basic string replacement (in production, use proper parameterization)
                        sql_content = sql_content.replace(placeholder, str(value))
                        logger.debug(f"Substituted parameter {key} = {value}")
            
            # Execute SQL
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
                
                # Gather execution info
                rows_affected = cursor.rowcount if cursor.rowcount >= 0 else 0
                
                # Collect any messages/notices
                messages = []
                if hasattr(conn, 'notices'):
                    messages = list(conn.notices)
                    conn.notices.clear()  # Clear notices for next execution
                
                logger.debug(f"SQL execution completed: {rows_affected} rows affected, {len(messages)} messages")
                
                return {
                    'rows_affected': rows_affected,
                    'messages': messages
                }
        
        except psycopg.Error as e:
            logger.error(f"PostgreSQL error in {script_path.name}: {e}")
            raise SQLScriptError(f"SQL execution failed: {e}", str(script_path), str(e))
        
        except Exception as e:
            logger.error(f"Unexpected error executing {script_path.name}: {e}")
            raise
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of transform execution statistics."""
        
        if self.stats['start_time']:
            total_duration = (
                (self.stats['end_time'] or datetime.now()) - self.stats['start_time']
            ).total_seconds()
        else:
            total_duration = 0
        
        return {
            'sql_directory': str(self.sql_dir),
            'scripts_executed': self.stats['scripts_executed'],
            'scripts_failed': self.stats['scripts_failed'],
            'success_rate': (
                self.stats['scripts_executed'] / 
                max(self.stats['scripts_executed'] + self.stats['scripts_failed'], 1) * 100
            ),
            'total_duration_seconds': total_duration,
            'avg_script_duration': (
                total_duration / max(self.stats['scripts_executed'], 1)
            ),
            'script_timings': self.stats['script_timings'].copy(),
            'execution_complete': self.stats['end_time'] is not None
        }
    
    def validate_script_dependencies(self) -> List[str]:
        """
        Validate that required database objects exist before running transforms.
        
        Returns:
            List of validation warnings/errors
        """
        warnings = []
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Check for required raw tables
                    cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'capture_insights' 
                    AND table_name LIKE 's1_raw_%'
                    """)
                    
                    raw_tables = [row['table_name'] for row in cursor.fetchall()]
                    
                    required_raw_tables = [
                        's1_raw_usaspending_prime_awards_slimv2',
                        's1_raw_usaspending_subawards'
                    ]
                    
                    for required_table in required_raw_tables:
                        if required_table not in raw_tables:
                            warnings.append(f"Required raw table missing: {required_table}")
                    
                    # Check for required utility functions
                    cursor.execute("""
                    SELECT routine_name 
                    FROM information_schema.routines 
                    WHERE routine_schema = 'capture_insights' 
                    AND routine_type = 'FUNCTION'
                    """)
                    
                    functions = [row['routine_name'] for row in cursor.fetchall()]
                    
                    required_functions = ['upsert_prime_awards_deduped']
                    
                    for required_func in required_functions:
                        if required_func not in functions:
                            warnings.append(f"Required function missing: {required_func}")
        
        except Exception as e:
            warnings.append(f"Dependency validation failed: {e}")
        
        return warnings


# Convenience function for running standard interim transforms
def run_interim_transforms(parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Run standard interim transformation scripts.
    
    Args:
        parameters: Optional parameters to pass to scripts
        
    Returns:
        Dictionary with execution results
    """
    runner = InterimTransformRunner()
    
    # Validate dependencies first
    warnings = runner.validate_script_dependencies()
    if warnings:
        for warning in warnings:
            logger.warning(f"Dependency check: {warning}")
    
    # Execute transforms
    return runner.execute_transform_batch(parameters=parameters)