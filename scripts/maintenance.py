"""
Maintenance Utilities

Provides data retention, cleanup, and system maintenance operations
for the ETL pipeline to manage storage and system health.

Constitution v1.9.0 | Task: T043
Dependencies: All ETL modules, PostgreSQL utilities
"""

import logging
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, List
from uuid import uuid4
import json
import shutil
import psycopg
from psycopg.rows import dict_row

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.config import get_config
from etl.utils.logging import get_logger
from etl.utils.disk import check_available_space_gb
from etl.staging.watermark import WatermarkManager

logger = get_logger(__name__)

class MaintenanceUtilities:
    """Comprehensive maintenance operations for the ETL pipeline."""
    
    def __init__(self):
        """Initialize maintenance utilities."""
        self.config = get_config()
        self.correlation_id = str(uuid4())
        
        logger.info("MaintenanceUtilities initialized", extra={
            'correlation_id': self.correlation_id
        })
    
    def cleanup_old_data(self, retention_days: int, dry_run: bool = True) -> Dict[str, Any]:
        """Clean up old data based on retention policy.
        
        Args:
            retention_days: Number of days to retain data
            dry_run: If True, only simulate cleanup without deleting
            
        Returns:
            Dict with cleanup results
        """
        logger.info(f"Starting data cleanup - retention: {retention_days} days, dry_run: {dry_run}", extra={
            'correlation_id': self.correlation_id,
            'retention_days': retention_days,
            'dry_run': dry_run
        })
        
        cleanup_start_time = time.time()
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        cleanup_results = {
            'operation': 'data_cleanup',
            'correlation_id': self.correlation_id,
            'retention_days': retention_days,
            'cutoff_date': cutoff_date.isoformat(),
            'dry_run': dry_run,
            'tables_processed': [],
            'total_rows_identified': 0,
            'total_rows_deleted': 0,
            'space_freed_estimate_mb': 0
        }
        
        try:
            with psycopg.connect(self.config.database_url) as conn:
                conn.row_factory = dict_row
                
                # Define tables to clean up with their timestamp columns
                cleanup_targets = [
                    {
                        'table': 's1_raw_usaspending_prime_awards_slimv2',
                        'timestamp_column': 'etl_created_at',
                        'description': 'Raw prime awards data'
                    },
                    {
                        'table': 's1_raw_usaspending_subawards_v2',
                        'timestamp_column': 'etl_created_at',
                        'description': 'Raw subawards data'
                    },
                    {
                        'table': 's2_interim_usaspending_prime_awards',
                        'timestamp_column': 'etl_created_at',
                        'description': 'Interim prime awards data'
                    },
                    {
                        'table': 's2_interim_usaspending_subawards',
                        'timestamp_column': 'etl_created_at',
                        'description': 'Interim subawards data'
                    },
                    {
                        'table': 'etl_progress_log',
                        'timestamp_column': 'created_at',
                        'description': 'ETL progress logs'
                    }
                ]
                
                for target in cleanup_targets:
                    table_name = target['table']
                    timestamp_col = target['timestamp_column']
                    
                    logger.info(f"Processing cleanup for table: {table_name}")
                    
                    try:
                        # Count rows to be deleted
                        count_query = f"""
                            SELECT COUNT(*) as row_count,
                                   pg_size_pretty(pg_total_relation_size('{table_name}')) as current_size,
                                   pg_total_relation_size('{table_name}') as size_bytes
                            FROM {table_name}
                            WHERE {timestamp_col} < %s
                        """
                        
                        with conn.cursor() as cur:
                            cur.execute(count_query, (cutoff_date,))
                            count_result = cur.fetchone()
                            
                            rows_to_delete = count_result['row_count']
                            current_size = count_result['current_size']
                            size_bytes = count_result['size_bytes']
                            
                            table_result = {
                                'table_name': table_name,
                                'description': target['description'],
                                'rows_identified': rows_to_delete,
                                'current_table_size': current_size,
                                'size_bytes': size_bytes,
                                'rows_deleted': 0,
                                'space_freed_mb': 0
                            }
                            
                            if rows_to_delete > 0:
                                # Estimate space savings (rough approximation)
                                estimated_freed_bytes = size_bytes * (rows_to_delete / max(1, rows_to_delete + 100))
                                table_result['estimated_space_freed_mb'] = round(estimated_freed_bytes / 1024 / 1024, 2)
                                
                                if not dry_run:
                                    # Execute deletion
                                    delete_query = f"DELETE FROM {table_name} WHERE {timestamp_col} < %s"
                                    cur.execute(delete_query, (cutoff_date,))
                                    rows_deleted = cur.rowcount
                                    
                                    table_result['rows_deleted'] = rows_deleted
                                    table_result['space_freed_mb'] = table_result['estimated_space_freed_mb']
                                    
                                    logger.info(f"Deleted {rows_deleted} rows from {table_name}")
                                else:
                                    logger.info(f"Would delete {rows_to_delete} rows from {table_name} (dry run)")
                            else:
                                logger.info(f"No old data found in {table_name}")
                            
                            cleanup_results['tables_processed'].append(table_result)
                            cleanup_results['total_rows_identified'] += rows_to_delete
                            cleanup_results['total_rows_deleted'] += table_result['rows_deleted']
                            cleanup_results['space_freed_estimate_mb'] += table_result.get('space_freed_mb', 0)
                    
                    except Exception as e:
                        logger.error(f"Failed to process table {table_name}: {e}")
                        cleanup_results['tables_processed'].append({
                            'table_name': table_name,
                            'description': target['description'],
                            'error': str(e),
                            'status': 'failed'
                        })
                
                # Vacuum analyze after deletions (if not dry run)
                if not dry_run and cleanup_results['total_rows_deleted'] > 0:
                    logger.info("Running VACUUM ANALYZE to reclaim space")
                    
                    with conn.cursor() as cur:
                        for table_result in cleanup_results['tables_processed']:
                            if table_result.get('rows_deleted', 0) > 0:
                                table_name = table_result['table_name']
                                cur.execute(f"VACUUM ANALYZE {table_name}")
                                logger.info(f"Vacuumed table: {table_name}")
                
                cleanup_results.update({
                    'execution_time_seconds': round(time.time() - cleanup_start_time, 3),
                    'overall_status': 'success'
                })
                
                logger.info("Data cleanup completed", extra={
                    'correlation_id': self.correlation_id,
                    'total_rows_identified': cleanup_results['total_rows_identified'],
                    'total_rows_deleted': cleanup_results['total_rows_deleted'],
                    'dry_run': dry_run
                })
                
                return cleanup_results
                
        except Exception as e:
            cleanup_results.update({
                'execution_time_seconds': round(time.time() - cleanup_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Data cleanup failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def cleanup_archive_files(self, retention_days: int, dry_run: bool = True) -> Dict[str, Any]:
        """Clean up old archive files from local storage.
        
        Args:
            retention_days: Number of days to retain archive files
            dry_run: If True, only simulate cleanup without deleting
            
        Returns:
            Dict with cleanup results
        """
        logger.info(f"Starting archive cleanup - retention: {retention_days} days, dry_run: {dry_run}", extra={
            'correlation_id': self.correlation_id,
            'retention_days': retention_days,
            'dry_run': dry_run
        })
        
        cleanup_start_time = time.time()
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        archive_results = {
            'operation': 'archive_cleanup',
            'correlation_id': self.correlation_id,
            'retention_days': retention_days,
            'cutoff_date': cutoff_date.isoformat(),
            'dry_run': dry_run,
            'files_identified': 0,
            'files_deleted': 0,
            'space_freed_mb': 0,
            'directories_processed': []
        }
        
        try:
            # Archive directories to check
            archive_dirs = [
                Path(self.config.archive_dir) / "prime_awards",
                Path(self.config.archive_dir) / "subawards"
            ]
            
            for archive_dir in archive_dirs:
                if not archive_dir.exists():
                    logger.info(f"Archive directory does not exist: {archive_dir}")
                    continue
                
                logger.info(f"Processing archive directory: {archive_dir}")
                
                dir_result = {
                    'directory': str(archive_dir),
                    'files_identified': 0,
                    'files_deleted': 0,
                    'space_freed_mb': 0,
                    'processed_files': []
                }
                
                # Find old archive files
                for file_path in archive_dir.rglob('*.zip'):
                    try:
                        file_stat = file_path.stat()
                        file_modified = datetime.fromtimestamp(file_stat.st_mtime)
                        file_size_mb = round(file_stat.st_size / 1024 / 1024, 2)
                        
                        if file_modified < cutoff_date:
                            dir_result['files_identified'] += 1
                            archive_results['files_identified'] += 1
                            
                            file_info = {
                                'filename': file_path.name,
                                'path': str(file_path),
                                'size_mb': file_size_mb,
                                'modified_date': file_modified.isoformat(),
                                'deleted': False
                            }
                            
                            if not dry_run:
                                file_path.unlink()
                                file_info['deleted'] = True
                                dir_result['files_deleted'] += 1
                                dir_result['space_freed_mb'] += file_size_mb
                                archive_results['files_deleted'] += 1
                                archive_results['space_freed_mb'] += file_size_mb
                                
                                logger.info(f"Deleted archive file: {file_path.name} ({file_size_mb}MB)")
                            else:
                                logger.info(f"Would delete archive file: {file_path.name} ({file_size_mb}MB) (dry run)")
                            
                            dir_result['processed_files'].append(file_info)
                    
                    except Exception as e:
                        logger.error(f"Error processing archive file {file_path}: {e}")
                
                archive_results['directories_processed'].append(dir_result)
            
            archive_results.update({
                'execution_time_seconds': round(time.time() - cleanup_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info("Archive cleanup completed", extra={
                'correlation_id': self.correlation_id,
                'files_identified': archive_results['files_identified'],
                'files_deleted': archive_results['files_deleted'],
                'space_freed_mb': archive_results['space_freed_mb'],
                'dry_run': dry_run
            })
            
            return archive_results
            
        except Exception as e:
            archive_results.update({
                'execution_time_seconds': round(time.time() - cleanup_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Archive cleanup failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def optimize_database(self, full_vacuum: bool = False) -> Dict[str, Any]:
        """Optimize database performance through maintenance operations.
        
        Args:
            full_vacuum: If True, perform VACUUM FULL (requires exclusive lock)
            
        Returns:
            Dict with optimization results
        """
        logger.info(f"Starting database optimization - full_vacuum: {full_vacuum}", extra={
            'correlation_id': self.correlation_id,
            'full_vacuum': full_vacuum
        })
        
        optimization_start_time = time.time()
        
        optimization_results = {
            'operation': 'database_optimization',
            'correlation_id': self.correlation_id,
            'full_vacuum': full_vacuum,
            'operations_performed': [],
            'table_statistics': []
        }
        
        try:
            with psycopg.connect(self.config.database_url) as conn:
                conn.row_factory = dict_row
                
                # Get list of main tables
                main_tables = [
                    's1_raw_usaspending_prime_awards_slimv2',
                    's1_raw_usaspending_subawards_v2', 
                    's2_interim_usaspending_prime_awards',
                    's2_interim_usaspending_subawards',
                    's3_processed_canonical_prime_awards',
                    's3_processed_canonical_subawards'
                ]
                
                with conn.cursor() as cur:
                    # Collect pre-optimization statistics
                    for table in main_tables:
                        try:
                            cur.execute(f"""
                                SELECT 
                                    '{table}' as table_name,
                                    COUNT(*) as row_count,
                                    pg_size_pretty(pg_total_relation_size('{table}')) as total_size,
                                    pg_total_relation_size('{table}') as size_bytes
                                FROM {table}
                            """)
                            
                            result = cur.fetchone()
                            if result:
                                optimization_results['table_statistics'].append({
                                    'table_name': table,
                                    'pre_optimization': {
                                        'row_count': result['row_count'],
                                        'total_size': result['total_size'],
                                        'size_bytes': result['size_bytes']
                                    }
                                })
                        except Exception as e:
                            logger.warning(f"Could not get statistics for table {table}: {e}")
                    
                    # Perform optimization operations
                    if full_vacuum:
                        logger.info("Performing VACUUM FULL on main tables")
                        for table in main_tables:
                            try:
                                vacuum_start = time.time()
                                cur.execute(f"VACUUM FULL {table}")
                                vacuum_time = round(time.time() - vacuum_start, 3)
                                
                                optimization_results['operations_performed'].append({
                                    'operation': 'VACUUM FULL',
                                    'table': table,
                                    'execution_time_seconds': vacuum_time,
                                    'status': 'success'
                                })
                                
                                logger.info(f"VACUUM FULL completed for {table} ({vacuum_time}s)")
                                
                            except Exception as e:
                                logger.error(f"VACUUM FULL failed for table {table}: {e}")
                                optimization_results['operations_performed'].append({
                                    'operation': 'VACUUM FULL',
                                    'table': table,
                                    'status': 'failed',
                                    'error': str(e)
                                })
                    else:
                        logger.info("Performing VACUUM ANALYZE on main tables")
                        for table in main_tables:
                            try:
                                vacuum_start = time.time()
                                cur.execute(f"VACUUM ANALYZE {table}")
                                vacuum_time = round(time.time() - vacuum_start, 3)
                                
                                optimization_results['operations_performed'].append({
                                    'operation': 'VACUUM ANALYZE',
                                    'table': table,
                                    'execution_time_seconds': vacuum_time,
                                    'status': 'success'
                                })
                                
                                logger.info(f"VACUUM ANALYZE completed for {table} ({vacuum_time}s)")
                                
                            except Exception as e:
                                logger.error(f"VACUUM ANALYZE failed for table {table}: {e}")
                                optimization_results['operations_performed'].append({
                                    'operation': 'VACUUM ANALYZE',
                                    'table': table,
                                    'status': 'failed',
                                    'error': str(e)
                                })
                    
                    # Reindex main tables
                    logger.info("Reindexing main tables")
                    for table in main_tables:
                        try:
                            reindex_start = time.time()
                            cur.execute(f"REINDEX TABLE {table}")
                            reindex_time = round(time.time() - reindex_start, 3)
                            
                            optimization_results['operations_performed'].append({
                                'operation': 'REINDEX',
                                'table': table,
                                'execution_time_seconds': reindex_time,
                                'status': 'success'
                            })
                            
                            logger.info(f"REINDEX completed for {table} ({reindex_time}s)")
                            
                        except Exception as e:
                            logger.error(f"REINDEX failed for table {table}: {e}")
                            optimization_results['operations_performed'].append({
                                'operation': 'REINDEX',
                                'table': table,
                                'status': 'failed',
                                'error': str(e)
                            })
                    
                    # Update table statistics
                    logger.info("Updating database statistics")
                    try:
                        stats_start = time.time()
                        cur.execute("ANALYZE")
                        stats_time = round(time.time() - stats_start, 3)
                        
                        optimization_results['operations_performed'].append({
                            'operation': 'ANALYZE (global)',
                            'execution_time_seconds': stats_time,
                            'status': 'success'
                        })
                        
                        logger.info(f"Global ANALYZE completed ({stats_time}s)")
                        
                    except Exception as e:
                        logger.error(f"Global ANALYZE failed: {e}")
                        optimization_results['operations_performed'].append({
                            'operation': 'ANALYZE (global)',
                            'status': 'failed',
                            'error': str(e)
                        })
                    
                    # Collect post-optimization statistics
                    for table_stat in optimization_results['table_statistics']:
                        table = table_stat['table_name']
                        try:
                            cur.execute(f"""
                                SELECT 
                                    COUNT(*) as row_count,
                                    pg_size_pretty(pg_total_relation_size('{table}')) as total_size,
                                    pg_total_relation_size('{table}') as size_bytes
                                FROM {table}
                            """)
                            
                            result = cur.fetchone()
                            if result:
                                pre_size = table_stat['pre_optimization']['size_bytes']
                                post_size = result['size_bytes']
                                space_saved = pre_size - post_size
                                
                                table_stat['post_optimization'] = {
                                    'row_count': result['row_count'],
                                    'total_size': result['total_size'],
                                    'size_bytes': post_size,
                                    'space_saved_bytes': space_saved,
                                    'space_saved_mb': round(space_saved / 1024 / 1024, 2)
                                }
                        except Exception as e:
                            logger.warning(f"Could not get post-optimization statistics for table {table}: {e}")
                
                # Calculate summary statistics
                successful_ops = len([op for op in optimization_results['operations_performed'] if op['status'] == 'success'])
                failed_ops = len([op for op in optimization_results['operations_performed'] if op['status'] == 'failed'])
                total_space_saved = sum(
                    table.get('post_optimization', {}).get('space_saved_mb', 0) 
                    for table in optimization_results['table_statistics']
                )
                
                optimization_results.update({
                    'successful_operations': successful_ops,
                    'failed_operations': failed_ops,
                    'total_space_saved_mb': total_space_saved,
                    'execution_time_seconds': round(time.time() - optimization_start_time, 3),
                    'overall_status': 'success' if failed_ops == 0 else 'partial_success'
                })
                
                logger.info("Database optimization completed", extra={
                    'correlation_id': self.correlation_id,
                    'successful_operations': successful_ops,
                    'failed_operations': failed_ops,
                    'space_saved_mb': total_space_saved
                })
                
                return optimization_results
                
        except Exception as e:
            optimization_results.update({
                'execution_time_seconds': round(time.time() - optimization_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Database optimization failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def generate_maintenance_report(self) -> Dict[str, Any]:
        """Generate comprehensive maintenance status report.
        
        Returns:
            Dict with maintenance report
        """
        logger.info("Generating maintenance report", extra={
            'correlation_id': self.correlation_id
        })
        
        report_start_time = time.time()
        
        report_results = {
            'report_type': 'maintenance_status',
            'correlation_id': self.correlation_id,
            'report_timestamp': datetime.now().isoformat(),
            'system_status': {},
            'recommendations': []
        }
        
        try:
            # System storage status
            available_space = check_available_space_gb()
            storage_utilization = max(0, 100 - (available_space / self.config.min_free_space_gb * 100))
            
            report_results['system_status']['storage'] = {
                'available_space_gb': available_space,
                'minimum_required_gb': self.config.min_free_space_gb,
                'utilization_percent': storage_utilization,
                'status': 'healthy' if available_space >= self.config.min_free_space_gb else 'warning'
            }
            
            # Database table sizes
            with psycopg.connect(self.config.database_url) as conn:
                conn.row_factory = dict_row
                
                with conn.cursor() as cur:
                    # Get table sizes
                    cur.execute("""
                        SELECT 
                            schemaname,
                            tablename,
                            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                            pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                        FROM pg_tables 
                        WHERE schemaname = 'public' 
                        AND tablename LIKE '%usaspending%'
                        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                    """)
                    
                    table_sizes = cur.fetchall()
                    
                    report_results['system_status']['database_tables'] = [
                        {
                            'table_name': row['tablename'],
                            'size': row['size'],
                            'size_bytes': row['size_bytes']
                        }
                        for row in table_sizes
                    ]
                    
                    total_db_size = sum(row['size_bytes'] for row in table_sizes)
                    report_results['system_status']['total_database_size_mb'] = round(total_db_size / 1024 / 1024, 2)
                    
                    # Get oldest data dates
                    oldest_data_query = """
                        SELECT 
                            'prime_awards' as data_type,
                            MIN(etl_created_at) as oldest_date,
                            MAX(etl_created_at) as newest_date,
                            COUNT(*) as total_rows
                        FROM s1_raw_usaspending_prime_awards_slimv2
                        UNION ALL
                        SELECT 
                            'subawards' as data_type,
                            MIN(etl_created_at) as oldest_date,
                            MAX(etl_created_at) as newest_date,
                            COUNT(*) as total_rows
                        FROM s1_raw_usaspending_subawards_v2
                    """
                    
                    cur.execute(oldest_data_query)
                    data_ages = cur.fetchall()
                    
                    report_results['system_status']['data_age'] = [
                        {
                            'data_type': row['data_type'],
                            'oldest_date': row['oldest_date'].isoformat() if row['oldest_date'] else None,
                            'newest_date': row['newest_date'].isoformat() if row['newest_date'] else None,
                            'total_rows': row['total_rows'],
                            'age_days': (datetime.now() - row['oldest_date']).days if row['oldest_date'] else None
                        }
                        for row in data_ages
                    ]
            
            # Generate recommendations based on status
            if report_results['system_status']['storage']['status'] == 'warning':
                report_results['recommendations'].append({
                    'priority': 'high',
                    'category': 'storage',
                    'message': f"Low disk space: {available_space:.1f}GB available, consider cleanup",
                    'action': 'Run data cleanup with appropriate retention policy'
                })
            
            # Check for old data
            for data_age in report_results['system_status']['data_age']:
                if data_age['age_days'] and data_age['age_days'] > 90:
                    report_results['recommendations'].append({
                        'priority': 'medium',
                        'category': 'data_retention',
                        'message': f"Old {data_age['data_type']} data found ({data_age['age_days']} days old)",
                        'action': 'Consider running data cleanup to remove old records'
                    })
            
            # Check database size
            if report_results['system_status']['total_database_size_mb'] > 10000:  # 10GB
                report_results['recommendations'].append({
                    'priority': 'medium',
                    'category': 'database_optimization',
                    'message': f"Large database size ({report_results['system_status']['total_database_size_mb']:.1f}MB)",
                    'action': 'Consider database optimization and archival of old data'
                })
            
            if not report_results['recommendations']:
                report_results['recommendations'].append({
                    'priority': 'info',
                    'category': 'status',
                    'message': 'System maintenance status is healthy',
                    'action': 'Continue regular monitoring'
                })
            
            report_results.update({
                'total_recommendations': len(report_results['recommendations']),
                'high_priority_count': len([r for r in report_results['recommendations'] if r['priority'] == 'high']),
                'execution_time_seconds': round(time.time() - report_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info("Maintenance report completed", extra={
                'correlation_id': self.correlation_id,
                'total_recommendations': report_results['total_recommendations'],
                'high_priority_count': report_results['high_priority_count']
            })
            
            return report_results
            
        except Exception as e:
            report_results.update({
                'execution_time_seconds': round(time.time() - report_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Maintenance report failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise


def main():
    """CLI entry point for maintenance utilities."""
    parser = argparse.ArgumentParser(description="ETL Pipeline Maintenance Utilities")
    parser.add_argument('--operation', type=str, required=True,
                       choices=['cleanup-data', 'cleanup-archives', 'optimize-db', 'report'],
                       help='Maintenance operation to perform')
    parser.add_argument('--retention-days', type=int, default=30,
                       help='Data retention period in days (default: 30)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Simulate operation without making changes')
    parser.add_argument('--full-vacuum', action='store_true',
                       help='Perform VACUUM FULL (for optimize-db operation)')
    parser.add_argument('--output-file', type=str,
                       help='Save results to JSON file')
    
    args = parser.parse_args()
    
    try:
        # Initialize maintenance utilities
        maintenance = MaintenanceUtilities()
        
        print(f"🔧 ETL Pipeline Maintenance Utilities")
        print(f"📊 Correlation ID: {maintenance.correlation_id}")
        print(f"🛠️  Operation: {args.operation}")
        
        # Execute requested operation
        if args.operation == 'cleanup-data':
            print(f"\n🧹 Cleaning up data (retention: {args.retention_days} days, dry_run: {args.dry_run})")
            results = maintenance.cleanup_old_data(args.retention_days, args.dry_run)
            
            print(f"✅ Data cleanup completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['execution_time_seconds']}s")
            print(f"📊 Rows identified: {results['total_rows_identified']:,}")
            print(f"🗑️  Rows deleted: {results['total_rows_deleted']:,}")
            print(f"💾 Space freed: {results['space_freed_estimate_mb']:.1f}MB")
            
        elif args.operation == 'cleanup-archives':
            print(f"\n📁 Cleaning up archive files (retention: {args.retention_days} days, dry_run: {args.dry_run})")
            results = maintenance.cleanup_archive_files(args.retention_days, args.dry_run)
            
            print(f"✅ Archive cleanup completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['execution_time_seconds']}s")
            print(f"📊 Files identified: {results['files_identified']}")
            print(f"🗑️  Files deleted: {results['files_deleted']}")
            print(f"💾 Space freed: {results['space_freed_mb']:.1f}MB")
            
        elif args.operation == 'optimize-db':
            print(f"\n🚀 Optimizing database (full_vacuum: {args.full_vacuum})")
            results = maintenance.optimize_database(args.full_vacuum)
            
            print(f"✅ Database optimization completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['execution_time_seconds']}s")
            print(f"✅ Successful operations: {results['successful_operations']}")
            print(f"❌ Failed operations: {results['failed_operations']}")
            print(f"💾 Space saved: {results['total_space_saved_mb']:.1f}MB")
            
        elif args.operation == 'report':
            print(f"\n📋 Generating maintenance report")
            results = maintenance.generate_maintenance_report()
            
            print(f"✅ Maintenance report completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['execution_time_seconds']}s")
            
            # Print storage status
            storage = results['system_status']['storage']
            print(f"\n💾 Storage Status:")
            print(f"   Available: {storage['available_space_gb']:.1f}GB")
            print(f"   Status: {storage['status'].upper()}")
            print(f"   Utilization: {storage['utilization_percent']:.1f}%")
            
            # Print database status
            print(f"\n🗄️  Database Status:")
            print(f"   Total size: {results['system_status']['total_database_size_mb']:.1f}MB")
            print(f"   Tables: {len(results['system_status']['database_tables'])}")
            
            # Print recommendations
            print(f"\n💡 Recommendations ({results['total_recommendations']}):")
            for rec in results['recommendations']:
                priority_icon = "🔴" if rec['priority'] == 'high' else "🟡" if rec['priority'] == 'medium' else "ℹ️"
                print(f"   {priority_icon} [{rec['priority'].upper()}] {rec['message']}")
        
        # Save results if requested
        if args.output_file:
            output_path = Path(args.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"💾 Results saved to: {output_path}")
        
        # Exit with appropriate code based on results
        if results.get('overall_status') == 'failed':
            exit(1)
        elif results.get('overall_status') == 'partial_success':
            exit(2)
    
    except Exception as e:
        print(f"❌ Maintenance operation failed: {e}")
        logger.exception("Maintenance operation failed")
        exit(1)


if __name__ == '__main__':
    main()