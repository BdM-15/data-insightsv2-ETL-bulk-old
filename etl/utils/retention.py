"""
Data Retention Policies System

Implements automated data retention and archival policies for storage management.
Provides configurable retention rules, archival strategies, and cleanup automation
for all pipeline data layers.

Constitution adherence:
- SQL-first: Uses database queries for retention analysis and cleanup
- Fail-fast: Validates retention policies and fails on misconfiguration
- Storage-conscious: Core purpose is managing storage usage efficiently
- Modular: Standalone retention system with pluggable policies
"""

import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import psycopg

from ..config import Config
from .logging import get_logger
from .disk import DiskGuard


class RetentionPolicy:
    """
    Defines a data retention policy with rules for what to keep, archive, or delete.
    """
    
    def __init__(self, name: str, rules: Dict[str, Any]):
        self.name = name
        self.rules = rules
        self.logger = get_logger(f"RetentionPolicy.{name}")
        
        # Validate rules
        self._validate_rules()
        
    def _validate_rules(self):
        """Validate retention policy rules."""
        required_fields = ['table_pattern', 'retention_days']
        for field in required_fields:
            if field not in self.rules:
                raise ValueError(f"Retention policy '{self.name}' missing required field: {field}")
                
        if not isinstance(self.rules['retention_days'], int) or self.rules['retention_days'] < 0:
            raise ValueError(f"retention_days must be a non-negative integer")
            
    def matches_table(self, table_name: str) -> bool:
        """Check if this policy applies to the given table."""
        import re
        pattern = self.rules['table_pattern']
        return bool(re.match(pattern, table_name))
        
    def should_delete(self, record_date: datetime) -> bool:
        """Check if a record should be deleted based on its date."""
        cutoff = datetime.now() - timedelta(days=self.rules['retention_days'])
        return record_date < cutoff
        
    def should_archive(self, record_date: datetime) -> bool:
        """Check if a record should be archived based on its date."""
        if 'archive_days' not in self.rules:
            return False
            
        archive_cutoff = datetime.now() - timedelta(days=self.rules['archive_days'])
        delete_cutoff = datetime.now() - timedelta(days=self.rules['retention_days'])
        
        return archive_cutoff <= record_date < delete_cutoff


class DataRetentionManager:
    """
    Manages data retention policies and executes cleanup/archival operations.
    """
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.logger = get_logger(self.__class__.__name__)
        self._connection: Optional[psycopg.Connection] = None
        self.disk_guard = DiskGuard(self.config.work_dir)
        self.policies: List[RetentionPolicy] = []
        
        # Load default policies
        self._load_default_policies()
        
    def __enter__(self):
        """Context manager entry."""
        self._connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
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
                autocommit=False
            )
            self.logger.debug("Connected to database for retention management")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
            
    def _load_default_policies(self):
        """Load default retention policies."""
        default_policies = [
            {
                'name': 'raw_data_retention',
                'rules': {
                    'table_pattern': r's1_raw\..*',
                    'retention_days': 30,
                    'archive_days': 7,
                    'description': 'Raw API data - archive after 7 days, delete after 30 days'
                }
            },
            {
                'name': 'interim_data_retention',
                'rules': {
                    'table_pattern': r's2_interim\..*',
                    'retention_days': 90,
                    'archive_days': 30,
                    'description': 'Interim processed data - archive after 30 days, delete after 90 days'
                }
            },
            {
                'name': 'processed_data_retention',
                'rules': {
                    'table_pattern': r's3_processed\.awards',
                    'retention_days': 365,
                    'description': 'Core awards data - keep for 1 year'
                }
            },
            {
                'name': 'metrics_retention',
                'rules': {
                    'table_pattern': r's3_processed\.(pipeline_executions|stage_metrics|data_quality_metrics|resource_usage)',
                    'retention_days': 90,
                    'description': 'Pipeline metrics - keep for 90 days'
                }
            },
            {
                'name': 'archive_data_retention',
                'rules': {
                    'table_pattern': r's3_processed\..*_archive',
                    'retention_days': 2555,  # 7 years
                    'description': 'Archived data - keep for 7 years'
                }
            }
        ]
        
        for policy_config in default_policies:
            policy = RetentionPolicy(policy_config['name'], policy_config['rules'])
            self.policies.append(policy)
            self.logger.info(f"Loaded retention policy: {policy.name}")
            
    def add_policy(self, policy: RetentionPolicy):
        """Add a custom retention policy."""
        self.policies.append(policy)
        self.logger.info(f"Added custom retention policy: {policy.name}")
        
    def get_applicable_policies(self, table_name: str) -> List[RetentionPolicy]:
        """Get all policies that apply to the given table."""
        applicable = []
        for policy in self.policies:
            if policy.matches_table(table_name):
                applicable.append(policy)
        return applicable
        
    def analyze_retention_candidates(self) -> Dict[str, Any]:
        """
        Analyze database for retention candidates.
        
        Returns:
            Dict: Analysis results with candidates for archival/deletion
        """
        if not self._connection:
            self._connect()
            
        analysis = {
            'total_tables_analyzed': 0,
            'deletion_candidates': [],
            'archival_candidates': [],
            'storage_impact': {
                'deletion_mb': 0,
                'archival_mb': 0
            },
            'analyzed_at': datetime.now()
        }
        
        try:
            with self._connection.cursor() as cur:
                # Get all tables with date columns for analysis
                cur.execute("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                    FROM pg_tables 
                    WHERE schemaname IN ('s1_raw', 's2_interim', 's3_processed')
                    ORDER BY schemaname, tablename
                """)
                
                tables = cur.fetchall()
                analysis['total_tables_analyzed'] = len(tables)
                
                for schema, table, size_bytes in tables:
                    full_table_name = f"{schema}.{table}"
                    applicable_policies = self.get_applicable_policies(full_table_name)
                    
                    if not applicable_policies:
                        continue
                        
                    policy = applicable_policies[0]  # Use first matching policy
                    size_mb = size_bytes / (1024 * 1024) if size_bytes else 0
                    
                    # Analyze records with date columns
                    date_column = self._get_date_column(schema, table)
                    if not date_column:
                        continue
                        
                    # Count deletion candidates
                    deletion_cutoff = datetime.now() - timedelta(days=policy.rules['retention_days'])
                    cur.execute(f"""
                        SELECT COUNT(*), 
                               MIN({date_column}) as oldest_date,
                               MAX({date_column}) as newest_date
                        FROM {schema}.{table}
                        WHERE {date_column} < %s
                    """, (deletion_cutoff,))
                    
                    deletion_result = cur.fetchone()
                    deletion_count = deletion_result[0] if deletion_result[0] else 0
                    
                    if deletion_count > 0:
                        analysis['deletion_candidates'].append({
                            'table': full_table_name,
                            'policy': policy.name,
                            'record_count': deletion_count,
                            'size_mb': size_mb,
                            'oldest_date': deletion_result[1],
                            'cutoff_date': deletion_cutoff
                        })
                        analysis['storage_impact']['deletion_mb'] += size_mb
                        
                    # Count archival candidates if policy supports archival
                    if 'archive_days' in policy.rules:
                        archive_cutoff = datetime.now() - timedelta(days=policy.rules['archive_days'])
                        cur.execute(f"""
                            SELECT COUNT(*)
                            FROM {schema}.{table}
                            WHERE {date_column} >= %s AND {date_column} < %s
                        """, (deletion_cutoff, archive_cutoff))
                        
                        archival_result = cur.fetchone()
                        archival_count = archival_result[0] if archival_result[0] else 0
                        
                        if archival_count > 0:
                            analysis['archival_candidates'].append({
                                'table': full_table_name,
                                'policy': policy.name,
                                'record_count': archival_count,
                                'size_mb': size_mb * (archival_count / (deletion_count + archival_count)),
                                'archive_cutoff': archive_cutoff
                            })
                            analysis['storage_impact']['archival_mb'] += size_mb * 0.5  # Estimate
                            
        except Exception as e:
            self.logger.error(f"Failed to analyze retention candidates: {e}")
            raise
            
        return analysis
        
    def _get_date_column(self, schema: str, table: str) -> Optional[str]:
        """
        Identify the primary date column for retention analysis.
        
        Args:
            schema: Database schema name
            table: Table name
            
        Returns:
            Optional[str]: Name of the date column to use for retention
        """
        # Define preferred date columns by table pattern
        date_columns = {
            'api_responses': 'downloaded_at',
            'awards': 'last_modified_date',
            'pipeline_executions': 'start_time',
            'stage_metrics': 'recorded_at',
            'data_quality_metrics': 'recorded_at',
            'resource_usage': 'recorded_at'
        }
        
        # Check if we have a known date column for this table
        for pattern, column in date_columns.items():
            if pattern in table:
                return column
                
        # Fallback: look for common date column names
        common_date_columns = [
            'created_at', 'updated_at', 'timestamp', 'date_created',
            'last_modified', 'processed_at', 'ingested_at'
        ]
        
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s 
                      AND table_name = %s
                      AND data_type IN ('timestamp', 'timestamp with time zone', 'date')
                """, (schema, table))
                
                available_columns = [row[0] for row in cur.fetchall()]
                
                # Return first match from common date columns
                for column in common_date_columns:
                    if column in available_columns:
                        return column
                        
                # Return first available date column
                if available_columns:
                    return available_columns[0]
                    
        except Exception as e:
            self.logger.warning(f"Could not identify date column for {schema}.{table}: {e}")
            
        return None
        
    def execute_retention_policy(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        Execute retention policies (deletion and archival).
        
        Args:
            dry_run: If True, only analyze what would be done without making changes
            
        Returns:
            Dict: Results of retention execution
        """
        if not self._connection:
            self._connect()
            
        results = {
            'dry_run': dry_run,
            'deleted_tables': [],
            'archived_tables': [],
            'errors': [],
            'storage_freed_mb': 0,
            'executed_at': datetime.now()
        }
        
        try:
            # Get retention analysis
            analysis = self.analyze_retention_candidates()
            
            # Process deletion candidates
            for candidate in analysis['deletion_candidates']:
                try:
                    if dry_run:
                        self.logger.info(f"DRY RUN: Would delete {candidate['record_count']} records from {candidate['table']}")
                        results['deleted_tables'].append({
                            'table': candidate['table'],
                            'records': candidate['record_count'],
                            'size_mb': candidate['size_mb'],
                            'dry_run': True
                        })
                    else:
                        deleted_count = self._delete_old_records(
                            candidate['table'],
                            candidate['cutoff_date']
                        )
                        results['deleted_tables'].append({
                            'table': candidate['table'],
                            'records': deleted_count,
                            'size_mb': candidate['size_mb'],
                            'dry_run': False
                        })
                        results['storage_freed_mb'] += candidate['size_mb']
                        
                except Exception as e:
                    error_msg = f"Failed to process deletion for {candidate['table']}: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
                    
            # Process archival candidates
            for candidate in analysis['archival_candidates']:
                try:
                    if dry_run:
                        self.logger.info(f"DRY RUN: Would archive {candidate['record_count']} records from {candidate['table']}")
                        results['archived_tables'].append({
                            'table': candidate['table'],
                            'records': candidate['record_count'],
                            'size_mb': candidate['size_mb'],
                            'dry_run': True
                        })
                    else:
                        archived_count = self._archive_old_records(
                            candidate['table'],
                            candidate['archive_cutoff']
                        )
                        results['archived_tables'].append({
                            'table': candidate['table'],
                            'records': archived_count,
                            'size_mb': candidate['size_mb'],
                            'dry_run': False
                        })
                        
                except Exception as e:
                    error_msg = f"Failed to process archival for {candidate['table']}: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
                    
            if not dry_run:
                self._connection.commit()
                self.logger.info(f"Retention policy execution completed. Freed {results['storage_freed_mb']:.2f} MB")
            else:
                self.logger.info("Dry run completed. No changes made.")
                
        except Exception as e:
            if not dry_run and self._connection:
                self._connection.rollback()
            self.logger.error(f"Retention policy execution failed: {e}")
            results['errors'].append(str(e))
            raise
            
        return results
        
    def _delete_old_records(self, table_name: str, cutoff_date: datetime) -> int:
        """
        Delete old records from a table.
        
        Args:
            table_name: Full table name (schema.table)
            cutoff_date: Delete records older than this date
            
        Returns:
            int: Number of records deleted
        """
        schema, table = table_name.split('.')
        date_column = self._get_date_column(schema, table)
        
        if not date_column:
            raise ValueError(f"No date column found for retention on {table_name}")
            
        with self._connection.cursor() as cur:
            cur.execute(f"""
                DELETE FROM {table_name}
                WHERE {date_column} < %s
            """, (cutoff_date,))
            
            deleted_count = cur.rowcount
            self.logger.info(f"Deleted {deleted_count} old records from {table_name}")
            return deleted_count
            
    def _archive_old_records(self, table_name: str, archive_cutoff: datetime) -> int:
        """
        Archive old records to an archive table.
        
        Args:
            table_name: Full table name (schema.table)
            archive_cutoff: Archive records older than this date
            
        Returns:
            int: Number of records archived
        """
        schema, table = table_name.split('.')
        archive_table_name = f"{schema}.{table}_archive"
        date_column = self._get_date_column(schema, table)
        
        if not date_column:
            raise ValueError(f"No date column found for archival on {table_name}")
            
        with self._connection.cursor() as cur:
            # Create archive table if it doesn't exist
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {archive_table_name} 
                (LIKE {table_name} INCLUDING ALL)
            """)
            
            # Move old records to archive
            cur.execute(f"""
                WITH archived_data AS (
                    DELETE FROM {table_name}
                    WHERE {date_column} < %s
                    RETURNING *
                )
                INSERT INTO {archive_table_name}
                SELECT * FROM archived_data
            """, (archive_cutoff,))
            
            archived_count = cur.rowcount
            self.logger.info(f"Archived {archived_count} old records from {table_name}")
            return archived_count
            
    def cleanup_file_storage(self, retention_days: int = 30) -> Dict[str, Any]:
        """
        Clean up old files in the work directory.
        
        Args:
            retention_days: Delete files older than this many days
            
        Returns:
            Dict: Cleanup results
        """
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        results = {
            'deleted_files': [],
            'storage_freed_mb': 0,
            'errors': []
        }
        
        try:
            work_dir = Path(self.config.work_dir)
            
            for file_path in work_dir.rglob('*'):
                if file_path.is_file():
                    try:
                        # Check file modification time
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        
                        if file_mtime < cutoff_date:
                            file_size_mb = file_path.stat().st_size / (1024 * 1024)
                            file_path.unlink()
                            
                            results['deleted_files'].append({
                                'path': str(file_path),
                                'size_mb': file_size_mb,
                                'modified_date': file_mtime
                            })
                            results['storage_freed_mb'] += file_size_mb
                            
                    except Exception as e:
                        error_msg = f"Failed to process file {file_path}: {e}"
                        results['errors'].append(error_msg)
                        
            self.logger.info(f"File cleanup completed. Deleted {len(results['deleted_files'])} files, freed {results['storage_freed_mb']:.2f} MB")
            
        except Exception as e:
            self.logger.error(f"File cleanup failed: {e}")
            results['errors'].append(str(e))
            
        return results