"""
Duplicate Diagnostics Module

Analyzes and reports on deduplication effectiveness, record precedence,
and data quality metrics for the processed layer tables.

Constitution v1.9.0 | Task: T039
Dependencies: s3_processed tables, deduplication audit fields
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date, timedelta
from uuid import uuid4
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import get_config
from ..utils.logging import get_logger

logger = get_logger(__name__)

@dataclass
class DeduplicationSummary:
    """Summary statistics for deduplication analysis."""
    
    entity_type: str
    total_records: int
    canonical_records: int
    duplicate_records: int
    duplicate_groups: int
    max_duplicates_per_group: int
    avg_duplicates_per_group: float
    deduplication_ratio: float  # canonical / total
    
    @property
    def efficiency_percentage(self) -> float:
        """Calculate deduplication efficiency as percentage."""
        if self.total_records == 0:
            return 0.0
        return round((1 - self.canonical_records / self.total_records) * 100, 2)

@dataclass 
class PrecedenceAnalysis:
    """Analysis of precedence rule effectiveness."""
    
    entity_type: str
    precedence_rule_violations: int
    inconsistent_rankings: int
    missing_precedence_fields: int
    null_last_modified_dates: int
    future_dated_records: int
    precedence_rule_effectiveness: float  # % of correct precedence applications
    
    @property
    def has_issues(self) -> bool:
        """Check if precedence analysis revealed issues."""
        return (self.precedence_rule_violations > 0 or 
                self.inconsistent_rankings > 0 or 
                self.missing_precedence_fields > 0)

class DuplicateDiagnostics:
    """Analyze and report on deduplication effectiveness and data quality."""
    
    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        """Initialize duplicate diagnostics.
        
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
        
        logger.info("DuplicateDiagnostics initialized", extra={
            'correlation_id': self.correlation_id
        })
    
    def _get_connection(self) -> psycopg.Connection:
        """Get database connection with retry logic."""
        return psycopg.connect(**self.connection_params)
    
    def _execute_diagnostic_query(self, connection: psycopg.Connection, 
                                query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """Execute a diagnostic query and return results.
        
        Args:
            connection: Database connection
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of result dictionaries
        """
        try:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query, params or ())
                return cursor.fetchall()
                
        except Exception as e:
            logger.error("Diagnostic query execution failed", extra={
                'correlation_id': self.correlation_id,
                'query_preview': query[:200] + "..." if len(query) > 200 else query,
                'error': str(e)
            })
            raise
    
    def analyze_deduplication_summary(self, table_name: str) -> DeduplicationSummary:
        """Analyze deduplication effectiveness for a processed table.
        
        Args:
            table_name: Name of processed table (without schema)
            
        Returns:
            DeduplicationSummary with analysis results
        """
        logger.info(f"Analyzing deduplication summary for {table_name}", extra={
            'correlation_id': self.correlation_id,
            'table_name': table_name
        })
        
        full_table_name = f"capture_insights.{table_name}"
        
        summary_query = f"""
        WITH dedup_stats AS (
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE is_canonical = true) as canonical_records,
                COUNT(*) FILTER (WHERE dedupe_total_duplicates > 1) as duplicate_records,
                COUNT(DISTINCT CASE 
                    WHEN dedupe_total_duplicates > 1 THEN 
                        CASE 
                            WHEN '{table_name}' LIKE '%prime_awards%' THEN contract_transaction_unique_key
                            ELSE subaward_report_key
                        END
                END) as duplicate_groups,
                MAX(dedupe_total_duplicates) as max_duplicates_per_group,
                AVG(dedupe_total_duplicates) FILTER (WHERE dedupe_total_duplicates > 1) as avg_duplicates_per_group
            FROM {full_table_name}
        )
        SELECT 
            *,
            CASE 
                WHEN total_records > 0 THEN canonical_records::float / total_records 
                ELSE 0 
            END as deduplication_ratio
        FROM dedup_stats;
        """
        
        with self._get_connection() as conn:
            results = self._execute_diagnostic_query(conn, summary_query)
            
            if results:
                row = results[0]
                return DeduplicationSummary(
                    entity_type=table_name,
                    total_records=row['total_records'] or 0,
                    canonical_records=row['canonical_records'] or 0,
                    duplicate_records=row['duplicate_records'] or 0,
                    duplicate_groups=row['duplicate_groups'] or 0,
                    max_duplicates_per_group=row['max_duplicates_per_group'] or 0,
                    avg_duplicates_per_group=round(row['avg_duplicates_per_group'] or 0, 2),
                    deduplication_ratio=round(row['deduplication_ratio'] or 0, 4)
                )
            else:
                return DeduplicationSummary(
                    entity_type=table_name,
                    total_records=0, canonical_records=0, duplicate_records=0,
                    duplicate_groups=0, max_duplicates_per_group=0,
                    avg_duplicates_per_group=0.0, deduplication_ratio=0.0
                )
    
    def analyze_precedence_rules(self, table_name: str) -> PrecedenceAnalysis:
        """Analyze precedence rule effectiveness for a processed table.
        
        Args:
            table_name: Name of processed table (without schema)
            
        Returns:
            PrecedenceAnalysis with rule effectiveness results
        """
        logger.info(f"Analyzing precedence rules for {table_name}", extra={
            'correlation_id': self.correlation_id,
            'table_name': table_name
        })
        
        full_table_name = f"capture_insights.{table_name}"
        
        # Determine primary key column based on table type
        primary_key_col = (
            'contract_transaction_unique_key' if 'prime_awards' in table_name 
            else 'subaward_report_key'
        )
        
        precedence_query = f"""
        WITH precedence_analysis AS (
            -- Check for canonical consistency violations
            SELECT 
                COUNT(*) FILTER (
                    WHERE (dedupe_rank = 1 AND is_canonical = false) 
                       OR (dedupe_rank > 1 AND is_canonical = true)
                ) as precedence_rule_violations,
                
                -- Check for inconsistent rankings within duplicate groups
                COUNT(*) FILTER (
                    WHERE dedupe_rank < 1 OR dedupe_rank > dedupe_total_duplicates
                ) as inconsistent_rankings,
                
                -- Check for missing precedence fields
                COUNT(*) FILTER (
                    WHERE last_modified_date IS NULL 
                      AND action_date IS NULL 
                      AND source_ingestion_ts IS NULL
                ) as missing_precedence_fields,
                
                -- Count null last_modified_date values
                COUNT(*) FILTER (WHERE last_modified_date IS NULL) as null_last_modified_dates,
                
                -- Count future-dated records (potential data quality issue)
                COUNT(*) FILTER (
                    WHERE last_modified_date > now() 
                       OR action_date > CURRENT_DATE
                ) as future_dated_records,
                
                COUNT(*) as total_records
            FROM {full_table_name}
        ),
        duplicate_group_violations AS (
            -- Find cases where non-canonical record is newer than canonical
            SELECT COUNT(*) as precedence_violations_in_groups
            FROM {full_table_name} p1
            JOIN {full_table_name} p2 
              ON p1.{primary_key_col} = p2.{primary_key_col}
            WHERE p1.is_canonical = true 
              AND p2.is_canonical = false
              AND (
                  p2.last_modified_date > p1.last_modified_date
                  OR (p2.last_modified_date = p1.last_modified_date 
                      AND p2.action_date > p1.action_date)
                  OR (p2.last_modified_date = p1.last_modified_date 
                      AND p2.action_date = p1.action_date
                      AND p2.source_ingestion_ts > p1.source_ingestion_ts)
              )
        )
        SELECT 
            pa.*,
            dgv.precedence_violations_in_groups,
            CASE 
                WHEN pa.total_records > 0 THEN 
                    1.0 - (pa.precedence_rule_violations + dgv.precedence_violations_in_groups)::float / pa.total_records
                ELSE 1.0 
            END as precedence_rule_effectiveness
        FROM precedence_analysis pa
        CROSS JOIN duplicate_group_violations dgv;
        """
        
        with self._get_connection() as conn:
            results = self._execute_diagnostic_query(conn, precedence_query)
            
            if results:
                row = results[0]
                return PrecedenceAnalysis(
                    entity_type=table_name,
                    precedence_rule_violations=row['precedence_rule_violations'] or 0,
                    inconsistent_rankings=row['inconsistent_rankings'] or 0,
                    missing_precedence_fields=row['missing_precedence_fields'] or 0,
                    null_last_modified_dates=row['null_last_modified_dates'] or 0,
                    future_dated_records=row['future_dated_records'] or 0,
                    precedence_rule_effectiveness=round(row['precedence_rule_effectiveness'] or 0, 4)
                )
            else:
                return PrecedenceAnalysis(
                    entity_type=table_name,
                    precedence_rule_violations=0, inconsistent_rankings=0,
                    missing_precedence_fields=0, null_last_modified_dates=0,
                    future_dated_records=0, precedence_rule_effectiveness=1.0
                )
    
    def get_duplicate_group_details(self, table_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get detailed information about duplicate groups.
        
        Args:
            table_name: Name of processed table (without schema)
            limit: Maximum number of duplicate groups to analyze
            
        Returns:
            List of dictionaries with duplicate group details
        """
        logger.info(f"Getting duplicate group details for {table_name}", extra={
            'correlation_id': self.correlation_id,
            'table_name': table_name,
            'limit': limit
        })
        
        full_table_name = f"capture_insights.{table_name}"
        
        # Determine columns based on table type
        if 'prime_awards' in table_name:
            key_column = 'contract_transaction_unique_key'
            additional_cols = """
                contract_award_unique_key,
                action_date,
                federal_action_obligation,
                recipient_name,
                awarding_sub_agency_name
            """
        else:
            key_column = 'subaward_report_key'
            additional_cols = """
                prime_award_unique_key,
                subaward_number,
                subaward_amount,
                sub_recipient_name,
                prime_recipient_name
            """
        
        duplicate_groups_query = f"""
        WITH duplicate_groups AS (
            SELECT {key_column}
            FROM {full_table_name}
            WHERE dedupe_total_duplicates > 1
            GROUP BY {key_column}
            ORDER BY MAX(dedupe_total_duplicates) DESC
            LIMIT {limit}
        )
        SELECT 
            dg.{key_column} as business_key,
            p.dedupe_rank,
            p.dedupe_total_duplicates,
            p.is_canonical,
            p.dedupe_precedence_reason,
            p.last_modified_date,
            p.source_ingestion_ts,
            {additional_cols}
        FROM duplicate_groups dg
        JOIN {full_table_name} p ON dg.{key_column} = p.{key_column}
        ORDER BY dg.{key_column}, p.dedupe_rank;
        """
        
        with self._get_connection() as conn:
            return self._execute_diagnostic_query(conn, duplicate_groups_query)
    
    def analyze_temporal_patterns(self, table_name: str) -> Dict[str, Any]:
        """Analyze temporal patterns in duplicate records.
        
        Args:
            table_name: Name of processed table (without schema)
            
        Returns:
            Dict with temporal pattern analysis
        """
        logger.info(f"Analyzing temporal patterns for {table_name}", extra={
            'correlation_id': self.correlation_id,
            'table_name': table_name
        })
        
        full_table_name = f"capture_insights.{table_name}"
        
        temporal_query = f"""
        WITH temporal_stats AS (
            SELECT 
                DATE_TRUNC('month', last_modified_date) as modification_month,
                DATE_TRUNC('month', source_ingestion_ts) as ingestion_month,
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE dedupe_total_duplicates > 1) as duplicate_records,
                AVG(dedupe_total_duplicates) as avg_duplicates_per_record,
                COUNT(DISTINCT CASE 
                    WHEN dedupe_total_duplicates > 1 THEN 
                        CASE 
                            WHEN '{table_name}' LIKE '%prime_awards%' THEN contract_transaction_unique_key
                            ELSE subaward_report_key
                        END
                END) as unique_duplicate_groups
            FROM {full_table_name}
            WHERE last_modified_date IS NOT NULL
            GROUP BY DATE_TRUNC('month', last_modified_date), DATE_TRUNC('month', source_ingestion_ts)
        ),
        ingestion_lag AS (
            SELECT 
                AVG(EXTRACT(EPOCH FROM (source_ingestion_ts - last_modified_date)) / 86400) as avg_ingestion_lag_days,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (source_ingestion_ts - last_modified_date)) / 86400) as median_ingestion_lag_days,
                MAX(EXTRACT(EPOCH FROM (source_ingestion_ts - last_modified_date)) / 86400) as max_ingestion_lag_days
            FROM {full_table_name}
            WHERE last_modified_date IS NOT NULL 
              AND source_ingestion_ts IS NOT NULL
              AND source_ingestion_ts >= last_modified_date
        )
        SELECT 
            json_agg(
                json_build_object(
                    'modification_month', ts.modification_month,
                    'ingestion_month', ts.ingestion_month,
                    'total_records', ts.total_records,
                    'duplicate_records', ts.duplicate_records,
                    'avg_duplicates_per_record', ts.avg_duplicates_per_record,
                    'unique_duplicate_groups', ts.unique_duplicate_groups
                ) ORDER BY ts.modification_month
            ) as monthly_patterns,
            json_build_object(
                'avg_ingestion_lag_days', il.avg_ingestion_lag_days,
                'median_ingestion_lag_days', il.median_ingestion_lag_days,
                'max_ingestion_lag_days', il.max_ingestion_lag_days
            ) as ingestion_lag_stats
        FROM temporal_stats ts
        CROSS JOIN ingestion_lag il;
        """
        
        with self._get_connection() as conn:
            results = self._execute_diagnostic_query(conn, temporal_query)
            
            if results and results[0]:
                return {
                    'entity_type': table_name,
                    'monthly_patterns': results[0]['monthly_patterns'] or [],
                    'ingestion_lag_stats': results[0]['ingestion_lag_stats'] or {}
                }
            else:
                return {
                    'entity_type': table_name,
                    'monthly_patterns': [],
                    'ingestion_lag_stats': {}
                }
    
    def generate_data_quality_report(self, table_name: str) -> Dict[str, Any]:
        """Generate comprehensive data quality report for a processed table.
        
        Args:
            table_name: Name of processed table (without schema)
            
        Returns:
            Dict with comprehensive data quality metrics
        """
        logger.info(f"Generating data quality report for {table_name}", extra={
            'correlation_id': self.correlation_id,
            'table_name': table_name
        })
        
        full_table_name = f"capture_insights.{table_name}"
        
        quality_query = f"""
        WITH quality_metrics AS (
            SELECT 
                COUNT(*) as total_records,
                
                -- Completeness metrics
                COUNT(*) FILTER (WHERE last_modified_date IS NOT NULL) as records_with_last_modified,
                COUNT(*) FILTER (WHERE action_date IS NOT NULL) as records_with_action_date,
                COUNT(*) FILTER (WHERE semantic_description IS NOT NULL AND semantic_description != '') as records_with_semantic_description,
                
                -- Canonical record metrics
                COUNT(*) FILTER (WHERE is_canonical = true) as canonical_records,
                COUNT(*) FILTER (WHERE is_canonical = true AND semantic_description IS NOT NULL) as canonical_with_descriptions,
                
                -- Financial data quality (for applicable tables)
                COUNT(*) FILTER (
                    WHERE is_canonical = true 
                      AND (
                          ('{table_name}' LIKE '%prime_awards%' AND federal_action_obligation IS NOT NULL)
                          OR ('{table_name}' LIKE '%subawards%' AND subaward_amount IS NOT NULL)
                      )
                ) as canonical_with_financial_data,
                
                -- Recipient data quality
                COUNT(*) FILTER (
                    WHERE is_canonical = true 
                      AND (
                          ('{table_name}' LIKE '%prime_awards%' AND recipient_uei IS NOT NULL)
                          OR ('{table_name}' LIKE '%subawards%' AND sub_recipient_uei IS NOT NULL)
                      )
                ) as canonical_with_recipient_uei,
                
                -- Date consistency checks
                COUNT(*) FILTER (
                    WHERE action_date > CURRENT_DATE
                ) as future_action_dates,
                
                COUNT(*) FILTER (
                    WHERE last_modified_date > now()
                ) as future_last_modified_dates,
                
                -- Duplicate consistency
                COUNT(*) FILTER (
                    WHERE dedupe_rank = 1 AND is_canonical = false
                ) as rank1_non_canonical,
                
                COUNT(*) FILTER (
                    WHERE dedupe_rank > 1 AND is_canonical = true
                ) as non_rank1_canonical
            FROM {full_table_name}
        )
        SELECT *,
            -- Calculate percentages
            CASE WHEN total_records > 0 THEN records_with_last_modified::float / total_records * 100 ELSE 0 END as pct_with_last_modified,
            CASE WHEN total_records > 0 THEN records_with_action_date::float / total_records * 100 ELSE 0 END as pct_with_action_date,
            CASE WHEN total_records > 0 THEN records_with_semantic_description::float / total_records * 100 ELSE 0 END as pct_with_semantic_description,
            CASE WHEN canonical_records > 0 THEN canonical_with_descriptions::float / canonical_records * 100 ELSE 0 END as pct_canonical_with_descriptions,
            CASE WHEN canonical_records > 0 THEN canonical_with_financial_data::float / canonical_records * 100 ELSE 0 END as pct_canonical_with_financial,
            CASE WHEN canonical_records > 0 THEN canonical_with_recipient_uei::float / canonical_records * 100 ELSE 0 END as pct_canonical_with_recipient_uei
        FROM quality_metrics;
        """
        
        with self._get_connection() as conn:
            results = self._execute_diagnostic_query(conn, quality_query)
            
            if results and results[0]:
                row = results[0]
                return {
                    'entity_type': table_name,
                    'total_records': row['total_records'] or 0,
                    'canonical_records': row['canonical_records'] or 0,
                    'completeness_metrics': {
                        'last_modified_date': {
                            'count': row['records_with_last_modified'] or 0,
                            'percentage': round(row['pct_with_last_modified'] or 0, 2)
                        },
                        'action_date': {
                            'count': row['records_with_action_date'] or 0,
                            'percentage': round(row['pct_with_action_date'] or 0, 2)
                        },
                        'semantic_description': {
                            'count': row['records_with_semantic_description'] or 0,
                            'percentage': round(row['pct_with_semantic_description'] or 0, 2)
                        }
                    },
                    'canonical_quality_metrics': {
                        'with_descriptions': {
                            'count': row['canonical_with_descriptions'] or 0,
                            'percentage': round(row['pct_canonical_with_descriptions'] or 0, 2)
                        },
                        'with_financial_data': {
                            'count': row['canonical_with_financial_data'] or 0,
                            'percentage': round(row['pct_canonical_with_financial'] or 0, 2)
                        },
                        'with_recipient_uei': {
                            'count': row['canonical_with_recipient_uei'] or 0,
                            'percentage': round(row['pct_canonical_with_recipient_uei'] or 0, 2)
                        }
                    },
                    'data_consistency_issues': {
                        'future_action_dates': row['future_action_dates'] or 0,
                        'future_last_modified_dates': row['future_last_modified_dates'] or 0,
                        'rank1_non_canonical': row['rank1_non_canonical'] or 0,
                        'non_rank1_canonical': row['non_rank1_canonical'] or 0
                    }
                }
            else:
                return {
                    'entity_type': table_name,
                    'total_records': 0,
                    'canonical_records': 0,
                    'completeness_metrics': {},
                    'canonical_quality_metrics': {},
                    'data_consistency_issues': {}
                }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def run_comprehensive_diagnostics(self) -> Dict[str, Any]:
        """Run comprehensive duplicate diagnostics for all processed tables.
        
        Returns:
            Dict with comprehensive diagnostic results
        """
        logger.info("Running comprehensive duplicate diagnostics", extra={
            'correlation_id': self.correlation_id,
            'operation': 'run_comprehensive_diagnostics'
        })
        
        start_time = time.time()
        
        tables_to_analyze = [
            's3_processed_usaspending_prime_awards',
            's3_processed_usaspending_subawards'
        ]
        
        results = {
            'correlation_id': self.correlation_id,
            'operation': 'comprehensive_diagnostics',
            'start_time': start_time,
            'analysis_results': {},
            'summary': {},
            'recommendations': []
        }
        
        try:
            for table_name in tables_to_analyze:
                logger.info(f"Analyzing table: {table_name}")
                
                table_results = {
                    'deduplication_summary': None,
                    'precedence_analysis': None,
                    'temporal_patterns': None,
                    'data_quality_report': None,
                    'sample_duplicate_groups': []
                }
                
                try:
                    # Deduplication summary
                    table_results['deduplication_summary'] = self.analyze_deduplication_summary(table_name)
                    
                    # Precedence analysis
                    table_results['precedence_analysis'] = self.analyze_precedence_rules(table_name)
                    
                    # Temporal patterns
                    table_results['temporal_patterns'] = self.analyze_temporal_patterns(table_name)
                    
                    # Data quality report
                    table_results['data_quality_report'] = self.generate_data_quality_report(table_name)
                    
                    # Sample duplicate groups (top 10)
                    table_results['sample_duplicate_groups'] = self.get_duplicate_group_details(table_name, 10)
                    
                except Exception as e:
                    logger.error(f"Failed to analyze {table_name}: {e}")
                    table_results['error'] = str(e)
                
                results['analysis_results'][table_name] = table_results
            
            # Generate summary and recommendations
            results['summary'] = self._generate_diagnostic_summary(results['analysis_results'])
            results['recommendations'] = self._generate_recommendations(results['analysis_results'])
            
            results.update({
                'status': 'completed',
                'execution_time_seconds': round(time.time() - start_time, 3)
            })
            
            logger.info("Comprehensive duplicate diagnostics completed", extra={
                'correlation_id': self.correlation_id,
                'execution_time': results['execution_time_seconds']
            })
            
            return results
            
        except Exception as e:
            results.update({
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'execution_time_seconds': round(time.time() - start_time, 3)
            })
            
            logger.error("Comprehensive duplicate diagnostics failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def _generate_diagnostic_summary(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics from analysis results."""
        
        summary = {
            'total_tables_analyzed': len(analysis_results),
            'total_records': 0,
            'total_canonical_records': 0,
            'total_duplicate_groups': 0,
            'overall_deduplication_ratio': 0.0,
            'tables_with_issues': 0,
            'avg_precedence_effectiveness': 0.0
        }
        
        precedence_scores = []
        
        for table_name, table_results in analysis_results.items():
            if 'error' not in table_results:
                dedup_summary = table_results.get('deduplication_summary')
                precedence_analysis = table_results.get('precedence_analysis')
                
                if dedup_summary:
                    summary['total_records'] += dedup_summary.total_records
                    summary['total_canonical_records'] += dedup_summary.canonical_records
                    summary['total_duplicate_groups'] += dedup_summary.duplicate_groups
                
                if precedence_analysis:
                    precedence_scores.append(precedence_analysis.precedence_rule_effectiveness)
                    if precedence_analysis.has_issues:
                        summary['tables_with_issues'] += 1
        
        if summary['total_records'] > 0:
            summary['overall_deduplication_ratio'] = round(
                summary['total_canonical_records'] / summary['total_records'], 4
            )
        
        if precedence_scores:
            summary['avg_precedence_effectiveness'] = round(
                sum(precedence_scores) / len(precedence_scores), 4
            )
        
        return summary
    
    def _generate_recommendations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on analysis results."""
        
        recommendations = []
        
        for table_name, table_results in analysis_results.items():
            if 'error' not in table_results:
                dedup_summary = table_results.get('deduplication_summary')
                precedence_analysis = table_results.get('precedence_analysis')
                data_quality = table_results.get('data_quality_report')
                
                entity_type = 'prime awards' if 'prime_awards' in table_name else 'subawards'
                
                # Deduplication recommendations
                if dedup_summary and dedup_summary.efficiency_percentage > 50:
                    recommendations.append(
                        f"High duplication rate ({dedup_summary.efficiency_percentage}%) detected in {entity_type}. "
                        f"Consider investigating data sources for duplicate submissions."
                    )
                
                # Precedence rule recommendations  
                if precedence_analysis and precedence_analysis.precedence_rule_effectiveness < 0.95:
                    recommendations.append(
                        f"Precedence rule effectiveness is {precedence_analysis.precedence_rule_effectiveness:.1%} for {entity_type}. "
                        f"Review precedence logic and data quality."
                    )
                
                if precedence_analysis and precedence_analysis.null_last_modified_dates > 1000:
                    recommendations.append(
                        f"High number ({precedence_analysis.null_last_modified_dates:,}) of missing last_modified_date values in {entity_type}. "
                        f"Consider improving data acquisition to capture modification timestamps."
                    )
                
                # Data quality recommendations
                if data_quality:
                    semantic_desc_pct = data_quality.get('completeness_metrics', {}).get('semantic_description', {}).get('percentage', 0)
                    if semantic_desc_pct < 90:
                        recommendations.append(
                            f"Semantic description completeness is {semantic_desc_pct}% for {entity_type}. "
                            f"Review description cleaning logic and source data quality."
                        )
                    
                    consistency_issues = data_quality.get('data_consistency_issues', {})
                    total_issues = sum(consistency_issues.values())
                    if total_issues > 0:
                        recommendations.append(
                            f"Found {total_issues} data consistency issues in {entity_type}. "
                            f"Review data validation rules and source data quality."
                        )
        
        if not recommendations:
            recommendations.append("No significant issues detected. Deduplication and data quality appear healthy.")
        
        return recommendations


def main():
    """CLI entry point for duplicate diagnostics."""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Analyze deduplication effectiveness and data quality")
    parser.add_argument('command', choices=['summary', 'precedence', 'quality', 'comprehensive'], 
                       help='Type of analysis to run')
    parser.add_argument('--table', choices=['prime_awards', 'subawards', 'all'], default='all',
                       help='Table to analyze (default: all)')
    parser.add_argument('--output-format', choices=['text', 'json'], default='text',
                       help='Output format (default: text)')
    parser.add_argument('--correlation-id', type=str,
                       help='Correlation ID for tracking (auto-generated if not provided)')
    
    args = parser.parse_args()
    
    # Initialize diagnostics
    diagnostics = DuplicateDiagnostics()
    if args.correlation_id:
        diagnostics.correlation_id = args.correlation_id
    
    table_mapping = {
        'prime_awards': 's3_processed_usaspending_prime_awards',
        'subawards': 's3_processed_usaspending_subawards'
    }
    
    try:
        if args.command == 'comprehensive':
            result = diagnostics.run_comprehensive_diagnostics()
            
            if args.output_format == 'json':
                print(json.dumps(result, indent=2, default=str))
            else:
                print(f"🔍 Comprehensive Duplicate Diagnostics Report")
                print(f"🔗 Correlation ID: {result['correlation_id']}")
                print(f"⏱️  Execution time: {result['execution_time_seconds']}s")
                
                summary = result['summary']
                print(f"\n📊 Summary:")
                print(f"   Total records: {summary['total_records']:,}")
                print(f"   Canonical records: {summary['total_canonical_records']:,}")
                print(f"   Deduplication ratio: {summary['overall_deduplication_ratio']:.1%}")
                print(f"   Average precedence effectiveness: {summary['avg_precedence_effectiveness']:.1%}")
                print(f"   Tables with issues: {summary['tables_with_issues']}/{summary['total_tables_analyzed']}")
                
                print(f"\n💡 Recommendations:")
                for i, rec in enumerate(result['recommendations'], 1):
                    print(f"   {i}. {rec}")
        
        else:
            # Individual analysis commands
            tables_to_analyze = [table_mapping[args.table]] if args.table != 'all' else list(table_mapping.values())
            
            for table_name in tables_to_analyze:
                if args.command == 'summary':
                    result = diagnostics.analyze_deduplication_summary(table_name)
                    
                    if args.output_format == 'json':
                        print(json.dumps(result.__dict__, indent=2))
                    else:
                        entity_type = 'Prime Awards' if 'prime_awards' in table_name else 'Subawards'
                        print(f"📊 Deduplication Summary - {entity_type}")
                        print(f"   Total records: {result.total_records:,}")
                        print(f"   Canonical records: {result.canonical_records:,}")
                        print(f"   Duplicate groups: {result.duplicate_groups:,}")
                        print(f"   Deduplication efficiency: {result.efficiency_percentage}%")
                        print(f"   Max duplicates per group: {result.max_duplicates_per_group}")
                
                elif args.command == 'precedence':
                    result = diagnostics.analyze_precedence_rules(table_name)
                    
                    if args.output_format == 'json':
                        print(json.dumps(result.__dict__, indent=2))
                    else:
                        entity_type = 'Prime Awards' if 'prime_awards' in table_name else 'Subawards'
                        print(f"🔍 Precedence Analysis - {entity_type}")
                        print(f"   Rule effectiveness: {result.precedence_rule_effectiveness:.1%}")
                        print(f"   Rule violations: {result.precedence_rule_violations}")
                        print(f"   Inconsistent rankings: {result.inconsistent_rankings}")
                        print(f"   Missing precedence fields: {result.missing_precedence_fields}")
                        print(f"   Null last_modified_date: {result.null_last_modified_dates}")
                
                elif args.command == 'quality':
                    result = diagnostics.generate_data_quality_report(table_name)
                    
                    if args.output_format == 'json':
                        print(json.dumps(result, indent=2))
                    else:
                        entity_type = 'Prime Awards' if 'prime_awards' in table_name else 'Subawards'
                        print(f"📋 Data Quality Report - {entity_type}")
                        print(f"   Total records: {result['total_records']:,}")
                        print(f"   Canonical records: {result['canonical_records']:,}")
                        
                        completeness = result['completeness_metrics']
                        print(f"   Semantic descriptions: {completeness.get('semantic_description', {}).get('percentage', 0)}%")
                        print(f"   Last modified dates: {completeness.get('last_modified_date', {}).get('percentage', 0)}%")
                        
                        issues = result['data_consistency_issues']
                        total_issues = sum(issues.values())
                        print(f"   Data consistency issues: {total_issues}")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        logger.exception("Duplicate diagnostics failed")
        exit(1)


if __name__ == '__main__':
    main()