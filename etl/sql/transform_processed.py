"""
Transform Processed Layer Module

Orchestrates merge and deduplication operations for the processed layer (s3_processed).
Executes SQL scripts that transform interim data into canonical, deduplicated records.

Constitution v1.9.0 | Task: T036
Dependencies: s2_interim.* tables populated, SQL scripts in sql/20_s3_processed/
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from uuid import uuid4

import psycopg
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import get_config
from ..utils.logging import get_logger
from ..utils.disk import check_available_space_gb

logger = get_logger(__name__)

class ProcessedLayerTransform:
    """Orchestrate processed layer merge and deduplication operations."""
    
    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        """Initialize processed layer transform.
        
        Args:
            connection_params: Database connection parameters. If None, loads from config.
        """
        self.config = get_config()
        self.connection_params = connection_params or {
            'host': self.config.db_host,
            'port': self.config.db_port,
            'dbname': self.config.db_name,
            'user': self.config.db_user,
            'password': self.config.db_password
        }
        self.sql_scripts_dir = Path(__file__).parent.parent.parent / "sql" / "20_s3_processed"
        self.correlation_id = str(uuid4())
        
        logger.info(f"ProcessedLayerTransform initialized", extra={
            'correlation_id': self.correlation_id,
            'sql_scripts_dir': str(self.sql_scripts_dir)
        })
    
    def _get_connection(self) -> psycopg.Connection:
        """Get database connection with retry logic."""
        return psycopg.connect(**self.connection_params)
    
    def _execute_sql_file(self, connection: psycopg.Connection, sql_file_path: Path) -> Dict[str, Any]:
        """Execute a SQL file and return execution metrics.
        
        Args:
            connection: Database connection
            sql_file_path: Path to SQL file
            
        Returns:
            Dict with execution metrics
        """
        logger.info(f"Executing SQL file: {sql_file_path.name}", extra={
            'correlation_id': self.correlation_id,
            'sql_file': str(sql_file_path)
        })
        
        start_time = time.time()
        
        try:
            with sql_file_path.open('r', encoding='utf-8') as f:
                sql_content = f.read()
            
            with connection.cursor() as cursor:
                cursor.execute(sql_content)
                connection.commit()
            
            execution_time = time.time() - start_time
            
            metrics = {
                'sql_file': sql_file_path.name,
                'execution_time_seconds': round(execution_time, 3),
                'status': 'success'
            }
            
            logger.info(f"SQL file executed successfully: {sql_file_path.name}", extra={
                'correlation_id': self.correlation_id,
                **metrics
            })
            
            return metrics
            
        except Exception as e:
            execution_time = time.time() - start_time
            
            metrics = {
                'sql_file': sql_file_path.name,
                'execution_time_seconds': round(execution_time, 3),
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__
            }
            
            logger.error(f"SQL file execution failed: {sql_file_path.name}", extra={
                'correlation_id': self.correlation_id,
                **metrics
            })
            
            raise
    
    def _get_table_stats(self, connection: psycopg.Connection, table_name: str) -> Dict[str, Any]:
        """Get statistics for a processed table.
        
        Args:
            connection: Database connection
            table_name: Table name (without schema prefix)
            
        Returns:
            Dict with table statistics
        """
        full_table_name = f"capture_insights.{table_name}"
        
        stats_query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE is_canonical = true) as canonical_records,
            COUNT(*) FILTER (WHERE dedupe_total_duplicates > 1) as duplicate_records,
            MAX(dedupe_total_duplicates) as max_duplicates_per_key,
            COUNT(DISTINCT CASE WHEN dedupe_total_duplicates > 1 THEN 
                CASE 
                    WHEN '{table_name}' = 's3_processed_usaspending_prime_awards' 
                    THEN contract_transaction_unique_key
                    ELSE subaward_report_key
                END
            END) as duplicate_groups,
            MIN(processed_at) as earliest_processed_at,
            MAX(processed_at) as latest_processed_at
        FROM {full_table_name};
        """
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(stats_query)
                row = cursor.fetchone()
                
                if row:
                    return {
                        'table_name': table_name,
                        'total_records': row[0] or 0,
                        'canonical_records': row[1] or 0,
                        'duplicate_records': row[2] or 0,
                        'max_duplicates_per_key': row[3] or 0,
                        'duplicate_groups': row[4] or 0,
                        'earliest_processed_at': row[5].isoformat() if row[5] else None,
                        'latest_processed_at': row[6].isoformat() if row[6] else None
                    }
                else:
                    return {'table_name': table_name, 'total_records': 0}
                    
        except Exception as e:
            logger.warning(f"Failed to get stats for {table_name}: {e}", extra={
                'correlation_id': self.correlation_id,
                'table_name': table_name,
                'error': str(e)
            })
            return {'table_name': table_name, 'error': str(e)}
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def transform_prime_awards(self) -> Dict[str, Any]:
        """Transform prime awards from interim to processed layer.
        
        Returns:
            Dict with transformation results and statistics
        """
        logger.info("Starting prime awards processed layer transformation", extra={
            'correlation_id': self.correlation_id,
            'operation': 'transform_prime_awards'
        })
        
        sql_file = self.sql_scripts_dir / "010_prime_awards_processed.sql"
        
        if not sql_file.exists():
            raise FileNotFoundError(f"Prime awards SQL script not found: {sql_file}")
        
        # Check disk space before transformation
        if check_available_space_gb() < self.config.min_free_space_gb:
            raise RuntimeError(f"Insufficient disk space for transformation")
        
        with self._get_connection() as conn:
            # Execute transformation SQL
            execution_metrics = self._execute_sql_file(conn, sql_file)
            
            # Get post-transformation statistics
            table_stats = self._get_table_stats(conn, "s3_processed_usaspending_prime_awards")
            
            result = {
                'entity_type': 'prime_awards',
                'transformation_status': 'completed',
                **execution_metrics,
                'table_statistics': table_stats,
                'correlation_id': self.correlation_id
            }
            
            logger.info("Prime awards transformation completed", extra={
                'correlation_id': self.correlation_id,
                **result
            })
            
            return result
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def transform_subawards(self) -> Dict[str, Any]:
        """Transform subawards from interim to processed layer.
        
        Returns:
            Dict with transformation results and statistics
        """
        logger.info("Starting subawards processed layer transformation", extra={
            'correlation_id': self.correlation_id,
            'operation': 'transform_subawards'
        })
        
        sql_file = self.sql_scripts_dir / "020_subawards_processed.sql"
        
        if not sql_file.exists():
            raise FileNotFoundError(f"Subawards SQL script not found: {sql_file}")
        
        # Check disk space before transformation
        if check_available_space_gb() < self.config.min_free_space_gb:
            raise RuntimeError(f"Insufficient disk space for transformation")
        
        with self._get_connection() as conn:
            # Execute transformation SQL
            execution_metrics = self._execute_sql_file(conn, sql_file)
            
            # Get post-transformation statistics
            table_stats = self._get_table_stats(conn, "s3_processed_usaspending_subawards")
            
            result = {
                'entity_type': 'subawards',
                'transformation_status': 'completed',
                **execution_metrics,
                'table_statistics': table_stats,
                'correlation_id': self.correlation_id
            }
            
            logger.info("Subawards transformation completed", extra={
                'correlation_id': self.correlation_id,
                **result
            })
            
            return result
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def transform_all(self) -> Dict[str, Any]:
        """Transform all entities from interim to processed layer.
        
        Returns:
            Dict with comprehensive transformation results
        """
        logger.info("Starting full processed layer transformation", extra={
            'correlation_id': self.correlation_id,
            'operation': 'transform_all'
        })
        
        start_time = time.time()
        results = {
            'correlation_id': self.correlation_id,
            'operation': 'transform_all',
            'start_time': time.time(),
            'transformations': []
        }
        
        try:
            # Transform prime awards
            prime_awards_result = self.transform_prime_awards()
            results['transformations'].append(prime_awards_result)
            
            # Transform subawards
            subawards_result = self.transform_subawards()
            results['transformations'].append(subawards_result)
            
            # Calculate summary metrics
            total_execution_time = sum(
                t.get('execution_time_seconds', 0) 
                for t in results['transformations']
            )
            
            total_canonical_records = sum(
                t.get('table_statistics', {}).get('canonical_records', 0) 
                for t in results['transformations']
            )
            
            total_duplicate_groups = sum(
                t.get('table_statistics', {}).get('duplicate_groups', 0) 
                for t in results['transformations']
            )
            
            results.update({
                'status': 'completed',
                'total_execution_time_seconds': round(total_execution_time, 3),
                'total_wall_time_seconds': round(time.time() - start_time, 3),
                'total_canonical_records': total_canonical_records,
                'total_duplicate_groups': total_duplicate_groups,
                'entities_transformed': len(results['transformations'])
            })
            
            logger.info("Full processed layer transformation completed", extra={
                'correlation_id': self.correlation_id,
                **{k: v for k, v in results.items() if k != 'transformations'}
            })
            
            return results
            
        except Exception as e:
            results.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'total_wall_time_seconds': round(time.time() - start_time, 3)
            })
            
            logger.error("Full processed layer transformation failed", extra={
                'correlation_id': self.correlation_id,
                **{k: v for k, v in results.items() if k != 'transformations'}
            })
            
            raise
    
    def validate_processed_tables(self) -> Dict[str, Any]:
        """Validate processed layer table integrity and consistency.
        
        Returns:
            Dict with validation results
        """
        logger.info("Validating processed layer tables", extra={
            'correlation_id': self.correlation_id,
            'operation': 'validate_processed_tables'
        })
        
        validation_results = {
            'correlation_id': self.correlation_id,
            'operation': 'validate_processed_tables',
            'tables_validated': [],
            'validation_errors': [],
            'overall_status': 'pending'
        }
        
        validation_queries = {
            's3_processed_usaspending_prime_awards': [
                ("primary_key_uniqueness", "SELECT COUNT(*) - COUNT(DISTINCT contract_transaction_unique_key) as duplicates FROM capture_insights.s3_processed_usaspending_prime_awards"),
                ("canonical_consistency", "SELECT COUNT(*) as non_canonical_rank_1 FROM capture_insights.s3_processed_usaspending_prime_awards WHERE dedupe_rank = 1 AND is_canonical = false"),
                ("dedupe_rank_consistency", "SELECT COUNT(*) as invalid_ranks FROM capture_insights.s3_processed_usaspending_prime_awards WHERE dedupe_rank < 1 OR dedupe_rank > dedupe_total_duplicates"),
                ("semantic_description_completeness", "SELECT COUNT(*) as missing_descriptions FROM capture_insights.s3_processed_usaspending_prime_awards WHERE semantic_description IS NULL OR semantic_description = ''")
            ],
            's3_processed_usaspending_subawards': [
                ("primary_key_uniqueness", "SELECT COUNT(*) - COUNT(DISTINCT subaward_report_key) as duplicates FROM capture_insights.s3_processed_usaspending_subawards"),
                ("canonical_consistency", "SELECT COUNT(*) as non_canonical_rank_1 FROM capture_insights.s3_processed_usaspending_subawards WHERE dedupe_rank = 1 AND is_canonical = false"),
                ("dedupe_rank_consistency", "SELECT COUNT(*) as invalid_ranks FROM capture_insights.s3_processed_usaspending_subawards WHERE dedupe_rank < 1 OR dedupe_rank > dedupe_total_duplicates"),
                ("semantic_description_completeness", "SELECT COUNT(*) as missing_descriptions FROM capture_insights.s3_processed_usaspending_subawards WHERE semantic_description IS NULL OR semantic_description = ''")
            ]
        }
        
        try:
            with self._get_connection() as conn:
                for table_name, checks in validation_queries.items():
                    table_validation = {
                        'table_name': table_name,
                        'checks_passed': 0,
                        'checks_failed': 0,
                        'check_results': []
                    }
                    
                    for check_name, query in checks:
                        try:
                            with conn.cursor() as cursor:
                                cursor.execute(query)
                                result = cursor.fetchone()
                                
                                if result and len(result) > 0:
                                    value = result[0]
                                    passed = value == 0  # Most checks expect 0 for success
                                    
                                    check_result = {
                                        'check_name': check_name,
                                        'passed': passed,
                                        'value': value,
                                        'query': query
                                    }
                                    
                                    table_validation['check_results'].append(check_result)
                                    
                                    if passed:
                                        table_validation['checks_passed'] += 1
                                    else:
                                        table_validation['checks_failed'] += 1
                                        validation_results['validation_errors'].append(f"{table_name}.{check_name}: {value}")
                                
                        except Exception as e:
                            check_result = {
                                'check_name': check_name,
                                'passed': False,
                                'error': str(e),
                                'query': query
                            }
                            table_validation['check_results'].append(check_result)
                            table_validation['checks_failed'] += 1
                            validation_results['validation_errors'].append(f"{table_name}.{check_name}: {str(e)}")
                    
                    validation_results['tables_validated'].append(table_validation)
                
                # Determine overall status
                total_failures = sum(t['checks_failed'] for t in validation_results['tables_validated'])
                validation_results['overall_status'] = 'passed' if total_failures == 0 else 'failed'
                
                logger.info("Processed layer validation completed", extra={
                    'correlation_id': self.correlation_id,
                    'overall_status': validation_results['overall_status'],
                    'total_validation_errors': len(validation_results['validation_errors'])
                })
                
                return validation_results
                
        except Exception as e:
            validation_results.update({
                'overall_status': 'error',
                'error': str(e),
                'error_type': type(e).__name__
            })
            
            logger.error("Processed layer validation failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise


def main():
    """CLI entry point for processed layer transformation."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Transform interim data to processed layer")
    parser.add_argument('--entity', choices=['prime_awards', 'subawards', 'all'], default='all',
                      help='Entity type to transform (default: all)')
    parser.add_argument('--validate', action='store_true',
                      help='Run validation checks after transformation')
    parser.add_argument('--correlation-id', type=str,
                      help='Correlation ID for tracking (auto-generated if not provided)')
    
    args = parser.parse_args()
    
    # Initialize transformer
    transformer = ProcessedLayerTransform()
    if args.correlation_id:
        transformer.correlation_id = args.correlation_id
    
    try:
        # Execute transformation
        if args.entity == 'prime_awards':
            result = transformer.transform_prime_awards()
        elif args.entity == 'subawards':
            result = transformer.transform_subawards()
        else:  # 'all'
            result = transformer.transform_all()
        
        print(f"✅ Transformation completed successfully")
        print(f"📊 Correlation ID: {result['correlation_id']}")
        
        if 'total_canonical_records' in result:
            print(f"📈 Total canonical records: {result['total_canonical_records']:,}")
            print(f"🔄 Total duplicate groups: {result['total_duplicate_groups']:,}")
            print(f"⏱️  Total execution time: {result['total_execution_time_seconds']}s")
        
        # Run validation if requested
        if args.validate:
            print("\n🔍 Running validation checks...")
            validation_result = transformer.validate_processed_tables()
            
            if validation_result['overall_status'] == 'passed':
                print("✅ All validation checks passed")
            else:
                print(f"❌ Validation failed with {len(validation_result['validation_errors'])} errors:")
                for error in validation_result['validation_errors']:
                    print(f"   - {error}")
                exit(1)
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        logger.exception("Processed layer transformation failed")
        exit(1)


if __name__ == '__main__':
    main()