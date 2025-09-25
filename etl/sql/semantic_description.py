"""
Semantic Description Module

Integrates the clean_description() SQL function to generate cleaned semantic descriptions
for award records, removing IGF patterns and normalizing text for analysis.

Constitution v1.9.0 | Task: T037
Dependencies: util.clean_description() function, s2_interim tables
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from uuid import uuid4

import psycopg
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import get_config
from ..utils.logging import get_logger
from ..utils.disk import check_available_space_gb

logger = get_logger(__name__)

class SemanticDescriptionGenerator:
    """Generate and apply semantic descriptions using the clean_description function."""
    
    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        """Initialize semantic description generator.
        
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
        self.correlation_id = str(uuid4())
        
        logger.info("SemanticDescriptionGenerator initialized", extra={
            'correlation_id': self.correlation_id
        })
    
    def _get_connection(self) -> psycopg.Connection:
        """Get database connection with retry logic."""
        return psycopg.connect(**self.connection_params)
    
    def verify_clean_description_function(self) -> Dict[str, Any]:
        """Verify that the util.clean_description() function exists and works.
        
        Returns:
            Dict with verification results
        """
        logger.info("Verifying clean_description function", extra={
            'correlation_id': self.correlation_id,
            'operation': 'verify_clean_description_function'
        })
        
        test_cases = [
            {
                'input': 'Contract for services [IGF:12345]',
                'expected': 'Contract for services',
                'description': 'Basic IGF removal'
            },
            {
                'input': 'Award [IGF:111] with [igf:222] multiple patterns',
                'expected': 'Award with multiple patterns',
                'description': 'Multiple IGF patterns with case variation'
            },
            {
                'input': 'Clean description without patterns',
                'expected': 'Clean description without patterns',
                'description': 'No IGF pattern'
            },
            {
                'input': None,
                'expected': '',
                'description': 'NULL input handling'
            },
            {
                'input': '[IGF:ONLY]',
                'expected': '',
                'description': 'Only IGF pattern'
            }
        ]
        
        verification_result = {
            'correlation_id': self.correlation_id,
            'function_exists': False,
            'tests_passed': 0,
            'tests_failed': 0,
            'test_results': [],
            'overall_status': 'pending'
        }
        
        try:
            with self._get_connection() as conn:
                # First check if function exists
                existence_query = """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.routines 
                    WHERE routine_schema = 'util' 
                    AND routine_name = 'clean_description'
                    AND routine_type = 'FUNCTION'
                );
                """
                
                with conn.cursor() as cursor:
                    cursor.execute(existence_query)
                    verification_result['function_exists'] = cursor.fetchone()[0]
                
                if not verification_result['function_exists']:
                    verification_result['overall_status'] = 'failed'
                    verification_result['error'] = 'clean_description function not found in util schema'
                    return verification_result
                
                # Run test cases
                for test_case in test_cases:
                    test_query = "SELECT util.clean_description(%s);"
                    
                    with conn.cursor() as cursor:
                        cursor.execute(test_query, (test_case['input'],))
                        result = cursor.fetchone()[0]
                        
                        passed = result == test_case['expected']
                        
                        test_result = {
                            'description': test_case['description'],
                            'input': test_case['input'],
                            'expected': test_case['expected'],
                            'actual': result,
                            'passed': passed
                        }
                        
                        verification_result['test_results'].append(test_result)
                        
                        if passed:
                            verification_result['tests_passed'] += 1
                        else:
                            verification_result['tests_failed'] += 1
                
                # Determine overall status
                verification_result['overall_status'] = (
                    'passed' if verification_result['tests_failed'] == 0 
                    else 'failed'
                )
                
                logger.info("Clean description function verification completed", extra={
                    'correlation_id': self.correlation_id,
                    'overall_status': verification_result['overall_status'],
                    'tests_passed': verification_result['tests_passed'],
                    'tests_failed': verification_result['tests_failed']
                })
                
                return verification_result
                
        except Exception as e:
            verification_result.update({
                'overall_status': 'error',
                'error': str(e),
                'error_type': type(e).__name__
            })
            
            logger.error("Clean description function verification failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def update_prime_awards_semantic_descriptions(self, batch_size: int = 10000) -> Dict[str, Any]:
        """Update semantic descriptions for prime awards in the interim layer.
        
        Args:
            batch_size: Number of records to process per batch
            
        Returns:
            Dict with update results and statistics
        """
        logger.info("Starting prime awards semantic description update", extra={
            'correlation_id': self.correlation_id,
            'operation': 'update_prime_awards_semantic_descriptions',
            'batch_size': batch_size
        })
        
        start_time = time.time()
        
        # First get count of records that need updating
        count_query = """
        SELECT COUNT(*) 
        FROM capture_insights.s2_interim_usaspending_prime_awards 
        WHERE semantic_description IS NULL 
           OR semantic_description = ''
           OR semantic_description != util.clean_description(
               COALESCE(transaction_description, prime_award_base_transaction_description, '')
           );
        """
        
        # Update query using clean_description function
        update_query = """
        UPDATE capture_insights.s2_interim_usaspending_prime_awards 
        SET 
            semantic_description = util.clean_description(
                COALESCE(transaction_description, prime_award_base_transaction_description, '')
            ),
            updated_at = now()
        WHERE contract_transaction_unique_key = ANY(%s);
        """
        
        # Batch selection query
        batch_query = """
        SELECT contract_transaction_unique_key
        FROM capture_insights.s2_interim_usaspending_prime_awards 
        WHERE semantic_description IS NULL 
           OR semantic_description = ''
           OR semantic_description != util.clean_description(
               COALESCE(transaction_description, prime_award_base_transaction_description, '')
           )
        ORDER BY contract_transaction_unique_key
        LIMIT %s OFFSET %s;
        """
        
        result = {
            'entity_type': 'prime_awards',
            'correlation_id': self.correlation_id,
            'operation': 'update_semantic_descriptions',
            'batch_size': batch_size,
            'total_records_to_update': 0,
            'records_updated': 0,
            'batches_processed': 0,
            'start_time': start_time,
            'status': 'pending'
        }
        
        try:
            with self._get_connection() as conn:
                # Get total count of records to update
                with conn.cursor() as cursor:
                    cursor.execute(count_query)
                    result['total_records_to_update'] = cursor.fetchone()[0]
                
                if result['total_records_to_update'] == 0:
                    result.update({
                        'status': 'completed',
                        'execution_time_seconds': round(time.time() - start_time, 3),
                        'message': 'No records require semantic description updates'
                    })
                    return result
                
                logger.info(f"Found {result['total_records_to_update']:,} prime awards requiring semantic description updates")
                
                # Process in batches
                offset = 0
                while offset < result['total_records_to_update']:
                    batch_start = time.time()
                    
                    # Get batch of keys
                    with conn.cursor() as cursor:
                        cursor.execute(batch_query, (batch_size, offset))
                        batch_keys = [row[0] for row in cursor.fetchall()]
                    
                    if not batch_keys:
                        break
                    
                    # Update batch
                    with conn.cursor() as cursor:
                        cursor.execute(update_query, (batch_keys,))
                        batch_updated = cursor.rowcount
                        conn.commit()
                    
                    result['records_updated'] += batch_updated
                    result['batches_processed'] += 1
                    offset += batch_size
                    
                    batch_time = time.time() - batch_start
                    
                    logger.info(f"Batch {result['batches_processed']} completed", extra={
                        'correlation_id': self.correlation_id,
                        'batch_size': len(batch_keys),
                        'batch_updated': batch_updated,
                        'batch_time_seconds': round(batch_time, 3),
                        'total_updated': result['records_updated'],
                        'progress_percent': round((result['records_updated'] / result['total_records_to_update']) * 100, 2)
                    })
                    
                    # Check disk space periodically
                    if result['batches_processed'] % 10 == 0:
                        if check_available_space_gb() < self.config.min_free_space_gb:
                            raise RuntimeError("Insufficient disk space during semantic description update")
                
                result.update({
                    'status': 'completed',
                    'execution_time_seconds': round(time.time() - start_time, 3)
                })
                
                logger.info("Prime awards semantic description update completed", extra={
                    'correlation_id': self.correlation_id,
                    'total_updated': result['records_updated'],
                    'execution_time': result['execution_time_seconds']
                })
                
                return result
                
        except Exception as e:
            result.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'execution_time_seconds': round(time.time() - start_time, 3)
            })
            
            logger.error("Prime awards semantic description update failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def update_subawards_semantic_descriptions(self, batch_size: int = 10000) -> Dict[str, Any]:
        """Update semantic descriptions for subawards in the interim layer.
        
        Args:
            batch_size: Number of records to process per batch
            
        Returns:
            Dict with update results and statistics
        """
        logger.info("Starting subawards semantic description update", extra={
            'correlation_id': self.correlation_id,
            'operation': 'update_subawards_semantic_descriptions',
            'batch_size': batch_size
        })
        
        start_time = time.time()
        
        # First get count of records that need updating
        count_query = """
        SELECT COUNT(*) 
        FROM capture_insights.s2_interim_usaspending_subawards 
        WHERE semantic_description IS NULL 
           OR semantic_description = ''
           OR semantic_description != util.clean_description(
               COALESCE(subaward_description, '')
           );
        """
        
        # Update query using clean_description function
        update_query = """
        UPDATE capture_insights.s2_interim_usaspending_subawards 
        SET 
            semantic_description = util.clean_description(
                COALESCE(subaward_description, '')
            ),
            updated_at = now()
        WHERE subaward_report_key = ANY(%s);
        """
        
        # Batch selection query
        batch_query = """
        SELECT subaward_report_key
        FROM capture_insights.s2_interim_usaspending_subawards 
        WHERE semantic_description IS NULL 
           OR semantic_description = ''
           OR semantic_description != util.clean_description(
               COALESCE(subaward_description, '')
           )
        ORDER BY subaward_report_key
        LIMIT %s OFFSET %s;
        """
        
        result = {
            'entity_type': 'subawards',
            'correlation_id': self.correlation_id,
            'operation': 'update_semantic_descriptions',
            'batch_size': batch_size,
            'total_records_to_update': 0,
            'records_updated': 0,
            'batches_processed': 0,
            'start_time': start_time,
            'status': 'pending'
        }
        
        try:
            with self._get_connection() as conn:
                # Get total count of records to update
                with conn.cursor() as cursor:
                    cursor.execute(count_query)
                    result['total_records_to_update'] = cursor.fetchone()[0]
                
                if result['total_records_to_update'] == 0:
                    result.update({
                        'status': 'completed',
                        'execution_time_seconds': round(time.time() - start_time, 3),
                        'message': 'No records require semantic description updates'
                    })
                    return result
                
                logger.info(f"Found {result['total_records_to_update']:,} subawards requiring semantic description updates")
                
                # Process in batches
                offset = 0
                while offset < result['total_records_to_update']:
                    batch_start = time.time()
                    
                    # Get batch of keys
                    with conn.cursor() as cursor:
                        cursor.execute(batch_query, (batch_size, offset))
                        batch_keys = [row[0] for row in cursor.fetchall()]
                    
                    if not batch_keys:
                        break
                    
                    # Update batch
                    with conn.cursor() as cursor:
                        cursor.execute(update_query, (batch_keys,))
                        batch_updated = cursor.rowcount
                        conn.commit()
                    
                    result['records_updated'] += batch_updated
                    result['batches_processed'] += 1
                    offset += batch_size
                    
                    batch_time = time.time() - batch_start
                    
                    logger.info(f"Batch {result['batches_processed']} completed", extra={
                        'correlation_id': self.correlation_id,
                        'batch_size': len(batch_keys),
                        'batch_updated': batch_updated,
                        'batch_time_seconds': round(batch_time, 3),
                        'total_updated': result['records_updated'],
                        'progress_percent': round((result['records_updated'] / result['total_records_to_update']) * 100, 2)
                    })
                    
                    # Check disk space periodically
                    if result['batches_processed'] % 10 == 0:
                        if check_available_space_gb() < self.config.min_free_space_gb:
                            raise RuntimeError("Insufficient disk space during semantic description update")
                
                result.update({
                    'status': 'completed',
                    'execution_time_seconds': round(time.time() - start_time, 3)
                })
                
                logger.info("Subawards semantic description update completed", extra={
                    'correlation_id': self.correlation_id,
                    'total_updated': result['records_updated'],
                    'execution_time': result['execution_time_seconds']
                })
                
                return result
                
        except Exception as e:
            result.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'execution_time_seconds': round(time.time() - start_time, 3)
            })
            
            logger.error("Subawards semantic description update failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def update_all_semantic_descriptions(self, batch_size: int = 10000) -> Dict[str, Any]:
        """Update semantic descriptions for all entity types.
        
        Args:
            batch_size: Number of records to process per batch
            
        Returns:
            Dict with comprehensive update results
        """
        logger.info("Starting semantic description update for all entities", extra={
            'correlation_id': self.correlation_id,
            'operation': 'update_all_semantic_descriptions',
            'batch_size': batch_size
        })
        
        start_time = time.time()
        results = {
            'correlation_id': self.correlation_id,
            'operation': 'update_all_semantic_descriptions',
            'batch_size': batch_size,
            'start_time': start_time,
            'updates': []
        }
        
        try:
            # First verify the clean_description function
            verification = self.verify_clean_description_function()
            if verification['overall_status'] != 'passed':
                raise RuntimeError(f"clean_description function verification failed: {verification.get('error', 'Unknown error')}")
            
            # Update prime awards
            prime_awards_result = self.update_prime_awards_semantic_descriptions(batch_size)
            results['updates'].append(prime_awards_result)
            
            # Update subawards
            subawards_result = self.update_subawards_semantic_descriptions(batch_size)
            results['updates'].append(subawards_result)
            
            # Calculate summary metrics
            total_records_updated = sum(
                u.get('records_updated', 0) 
                for u in results['updates']
            )
            
            total_execution_time = sum(
                u.get('execution_time_seconds', 0) 
                for u in results['updates']
            )
            
            results.update({
                'status': 'completed',
                'total_records_updated': total_records_updated,
                'total_execution_time_seconds': round(total_execution_time, 3),
                'total_wall_time_seconds': round(time.time() - start_time, 3),
                'entities_processed': len(results['updates'])
            })
            
            logger.info("All semantic description updates completed", extra={
                'correlation_id': self.correlation_id,
                'total_records_updated': total_records_updated,
                'total_execution_time': total_execution_time
            })
            
            return results
            
        except Exception as e:
            results.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'total_wall_time_seconds': round(time.time() - start_time, 3)
            })
            
            logger.error("Semantic description update for all entities failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def get_semantic_description_stats(self) -> Dict[str, Any]:
        """Get statistics about semantic descriptions across all tables.
        
        Returns:
            Dict with semantic description statistics
        """
        logger.info("Generating semantic description statistics", extra={
            'correlation_id': self.correlation_id,
            'operation': 'get_semantic_description_stats'
        })
        
        stats_queries = {
            'prime_awards_interim': """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE semantic_description IS NOT NULL AND semantic_description != '') as with_semantic_description,
                    COUNT(*) FILTER (WHERE semantic_description IS NULL OR semantic_description = '') as without_semantic_description,
                    AVG(LENGTH(semantic_description)) FILTER (WHERE semantic_description IS NOT NULL) as avg_description_length
                FROM capture_insights.s2_interim_usaspending_prime_awards;
            """,
            'subawards_interim': """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE semantic_description IS NOT NULL AND semantic_description != '') as with_semantic_description,
                    COUNT(*) FILTER (WHERE semantic_description IS NULL OR semantic_description = '') as without_semantic_description,
                    AVG(LENGTH(semantic_description)) FILTER (WHERE semantic_description IS NOT NULL) as avg_description_length
                FROM capture_insights.s2_interim_usaspending_subawards;
            """,
            'prime_awards_processed': """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE semantic_description IS NOT NULL AND semantic_description != '') as with_semantic_description,
                    COUNT(*) FILTER (WHERE semantic_description IS NULL OR semantic_description = '') as without_semantic_description,
                    AVG(LENGTH(semantic_description)) FILTER (WHERE semantic_description IS NOT NULL) as avg_description_length
                FROM capture_insights.s3_processed_usaspending_prime_awards;
            """,
            'subawards_processed': """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE semantic_description IS NOT NULL AND semantic_description != '') as with_semantic_description,
                    COUNT(*) FILTER (WHERE semantic_description IS NULL OR semantic_description = '') as without_semantic_description,
                    AVG(LENGTH(semantic_description)) FILTER (WHERE semantic_description IS NOT NULL) as avg_description_length
                FROM capture_insights.s3_processed_usaspending_subawards;
            """
        }
        
        stats_result = {
            'correlation_id': self.correlation_id,
            'operation': 'get_semantic_description_stats',
            'table_stats': {},
            'overall_stats': {},
            'status': 'pending'
        }
        
        try:
            with self._get_connection() as conn:
                for table_name, query in stats_queries.items():
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            row = cursor.fetchone()
                            
                            if row:
                                stats_result['table_stats'][table_name] = {
                                    'total_records': row[0] or 0,
                                    'with_semantic_description': row[1] or 0,
                                    'without_semantic_description': row[2] or 0,
                                    'avg_description_length': round(row[3] or 0, 2),
                                    'completion_percentage': round((row[1] or 0) / max(row[0], 1) * 100, 2)
                                }
                            else:
                                stats_result['table_stats'][table_name] = {'total_records': 0}
                                
                    except Exception as e:
                        stats_result['table_stats'][table_name] = {'error': str(e)}
                        logger.warning(f"Failed to get stats for {table_name}: {e}")
                
                # Calculate overall statistics
                total_records = sum(
                    stats.get('total_records', 0) 
                    for stats in stats_result['table_stats'].values()
                    if 'error' not in stats
                )
                
                total_with_descriptions = sum(
                    stats.get('with_semantic_description', 0) 
                    for stats in stats_result['table_stats'].values()
                    if 'error' not in stats
                )
                
                stats_result['overall_stats'] = {
                    'total_records': total_records,
                    'total_with_semantic_description': total_with_descriptions,
                    'overall_completion_percentage': round(total_with_descriptions / max(total_records, 1) * 100, 2)
                }
                
                stats_result['status'] = 'completed'
                
                logger.info("Semantic description statistics generated", extra={
                    'correlation_id': self.correlation_id,
                    'total_records': total_records,
                    'completion_percentage': stats_result['overall_stats']['overall_completion_percentage']
                })
                
                return stats_result
                
        except Exception as e:
            stats_result.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__
            })
            
            logger.error("Failed to generate semantic description statistics", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise


def main():
    """CLI entry point for semantic description management."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage semantic descriptions for award data")
    parser.add_argument('command', choices=['verify', 'update', 'stats'], 
                       help='Command to execute')
    parser.add_argument('--entity', choices=['prime_awards', 'subawards', 'all'], default='all',
                       help='Entity type to process (default: all)')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='Batch size for updates (default: 10000)')
    parser.add_argument('--correlation-id', type=str,
                       help='Correlation ID for tracking (auto-generated if not provided)')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = SemanticDescriptionGenerator()
    if args.correlation_id:
        generator.correlation_id = args.correlation_id
    
    try:
        if args.command == 'verify':
            result = generator.verify_clean_description_function()
            print(f"🔍 Function verification: {result['overall_status'].upper()}")
            print(f"📊 Tests: {result['tests_passed']} passed, {result['tests_failed']} failed")
            
            if result['tests_failed'] > 0:
                print("\n❌ Failed tests:")
                for test in result['test_results']:
                    if not test['passed']:
                        print(f"   - {test['description']}: expected '{test['expected']}', got '{test['actual']}'")
                exit(1)
            else:
                print("✅ All verification tests passed")
        
        elif args.command == 'update':
            if args.entity == 'prime_awards':
                result = generator.update_prime_awards_semantic_descriptions(args.batch_size)
            elif args.entity == 'subawards':
                result = generator.update_subawards_semantic_descriptions(args.batch_size)
            else:  # 'all'
                result = generator.update_all_semantic_descriptions(args.batch_size)
            
            print(f"✅ Semantic description update completed")
            print(f"📊 Correlation ID: {result['correlation_id']}")
            
            if 'total_records_updated' in result:
                print(f"📈 Total records updated: {result['total_records_updated']:,}")
                print(f"⏱️  Total execution time: {result['total_execution_time_seconds']}s")
            else:
                print(f"📈 Records updated: {result['records_updated']:,}")
                print(f"⏱️  Execution time: {result['execution_time_seconds']}s")
        
        elif args.command == 'stats':
            result = generator.get_semantic_description_stats()
            print(f"📊 Semantic Description Statistics")
            print(f"🔗 Correlation ID: {result['correlation_id']}")
            print(f"📈 Overall completion: {result['overall_stats']['overall_completion_percentage']}%")
            print(f"📋 Total records: {result['overall_stats']['total_records']:,}")
            
            print("\n📋 Per-table statistics:")
            for table_name, stats in result['table_stats'].items():
                if 'error' not in stats:
                    print(f"   {table_name}: {stats['completion_percentage']}% ({stats['with_semantic_description']:,}/{stats['total_records']:,})")
                else:
                    print(f"   {table_name}: ERROR - {stats['error']}")
        
    except Exception as e:
        print(f"❌ Operation failed: {e}")
        logger.exception("Semantic description operation failed")
        exit(1)


if __name__ == '__main__':
    main()