"""
Pipeline Monitoring Dashboard

Provides real-time monitoring, metrics collection, and alerting
for ETL pipeline operations and system health.

Constitution v1.9.0 | Task: T044
Dependencies: All ETL modules, metrics collection
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
import psycopg
from psycopg.rows import dict_row

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.config import get_config
from etl.utils.logging import get_logger
from etl.utils.disk import check_available_space_gb
from etl.staging.progress import ProgressRecorder
from etl.staging.watermark import WatermarkManager

logger = get_logger(__name__)

class MonitoringDashboard:
    """Real-time monitoring dashboard for ETL pipeline operations."""
    
    def __init__(self):
        """Initialize monitoring dashboard."""
        self.config = get_config()
        self.correlation_id = str(uuid4())
        self.progress_recorder = ProgressRecorder()
        self.watermark_manager = WatermarkManager("prime_awards_incremental")
        
        logger.info("MonitoringDashboard initialized", extra={
            'correlation_id': self.correlation_id
        })
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline operational status.
        
        Returns:
            Dict with pipeline status information
        """
        logger.info("Getting pipeline status", extra={
            'correlation_id': self.correlation_id
        })
        
        status_start_time = time.time()
        
        try:
            # Get watermark information
            watermark_info = self.watermark_manager.get_current_watermark()
            
            # Get recent progress information
            recent_progress = self.progress_recorder.get_recent_pipeline_activity(days_back=7)
            
            # Get current pipeline status
            current_status = self.progress_recorder.get_pipeline_status("prime_awards_incremental")
            
            pipeline_status = {
                'correlation_id': self.correlation_id,
                'timestamp': datetime.now().isoformat(),
                'watermark': {
                    'last_modified_to': watermark_info.last_modified_to.isoformat() if watermark_info.last_modified_to else None,
                    'overlap_days': watermark_info.overlap_days,
                    'created_at': watermark_info.created_at.isoformat() if watermark_info.created_at else None,
                    'days_since_last_update': (datetime.now() - watermark_info.last_modified_to).days if watermark_info.last_modified_to else None
                },
                'current_status': current_status,
                'recent_activity': recent_progress,
                'collection_time_seconds': round(time.time() - status_start_time, 3)
            }
            
            # Determine health status
            days_stale = pipeline_status['watermark']['days_since_last_update'] or 999
            
            if days_stale <= 1:
                health_status = 'healthy'
            elif days_stale <= 3:
                health_status = 'warning'
            else:
                health_status = 'stale'
            
            pipeline_status['health_status'] = health_status
            
            logger.info("Pipeline status collected", extra={
                'correlation_id': self.correlation_id,
                'health_status': health_status,
                'days_stale': days_stale
            })
            
            return pipeline_status
            
        except Exception as e:
            logger.error("Failed to get pipeline status", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            return {
                'correlation_id': self.correlation_id,
                'timestamp': datetime.now().isoformat(),
                'health_status': 'error',
                'error': str(e),
                'collection_time_seconds': round(time.time() - status_start_time, 3)
            }
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system resource metrics.
        
        Returns:
            Dict with system metrics
        """
        logger.info("Collecting system metrics", extra={
            'correlation_id': self.correlation_id
        })
        
        metrics_start_time = time.time()
        
        try:
            # Disk space metrics
            available_space = check_available_space_gb()
            disk_metrics = {
                'available_space_gb': available_space,
                'minimum_required_gb': self.config.min_free_space_gb,
                'utilization_percent': max(0, 100 - (available_space / self.config.min_free_space_gb * 100)),
                'status': 'healthy' if available_space >= self.config.min_free_space_gb else 'warning'
            }
            
            # Database metrics
            with psycopg.connect(self.config.database_url) as conn:
                conn.row_factory = dict_row
                
                with conn.cursor() as cur:
                    # Database size and connection info
                    cur.execute("""
                        SELECT 
                            pg_database_size(current_database()) as db_size_bytes,
                            pg_size_pretty(pg_database_size(current_database())) as db_size_pretty,
                            (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
                            (SELECT count(*) FROM pg_stat_activity) as total_connections
                    """)
                    
                    db_info = cur.fetchone()
                    
                    # Table-level metrics
                    cur.execute("""
                        SELECT 
                            schemaname,
                            tablename,
                            n_tup_ins as inserts,
                            n_tup_upd as updates,
                            n_tup_del as deletes,
                            n_live_tup as live_rows,
                            n_dead_tup as dead_rows,
                            last_vacuum,
                            last_autovacuum,
                            last_analyze,
                            last_autoanalyze,
                            pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                        FROM pg_stat_user_tables 
                        WHERE tablename LIKE '%usaspending%'
                        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                    """)
                    
                    table_stats = cur.fetchall()
                    
                    db_metrics = {
                        'database_size_bytes': db_info['db_size_bytes'],
                        'database_size_pretty': db_info['db_size_pretty'],
                        'active_connections': db_info['active_connections'],
                        'total_connections': db_info['total_connections'],
                        'table_statistics': [
                            {
                                'table_name': row['tablename'],
                                'live_rows': row['live_rows'],
                                'dead_rows': row['dead_rows'],
                                'size_bytes': row['size_bytes'],
                                'size_mb': round(row['size_bytes'] / 1024 / 1024, 2),
                                'inserts': row['inserts'],
                                'updates': row['updates'],
                                'deletes': row['deletes'],
                                'last_vacuum': row['last_vacuum'].isoformat() if row['last_vacuum'] else None,
                                'last_analyze': row['last_analyze'].isoformat() if row['last_analyze'] else None
                            }
                            for row in table_stats
                        ],
                        'total_live_rows': sum(row['live_rows'] or 0 for row in table_stats),
                        'total_dead_rows': sum(row['dead_rows'] or 0 for row in table_stats)
                    }
            
            system_metrics = {
                'correlation_id': self.correlation_id,
                'timestamp': datetime.now().isoformat(),
                'disk': disk_metrics,
                'database': db_metrics,
                'collection_time_seconds': round(time.time() - metrics_start_time, 3)
            }
            
            logger.info("System metrics collected", extra={
                'correlation_id': self.correlation_id,
                'db_size_mb': round(db_info['db_size_bytes'] / 1024 / 1024, 2),
                'disk_available_gb': available_space
            })
            
            return system_metrics
            
        except Exception as e:
            logger.error("Failed to collect system metrics", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            return {
                'correlation_id': self.correlation_id,
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'collection_time_seconds': round(time.time() - metrics_start_time, 3)
            }
    
    def get_data_quality_metrics(self, days_back: int = 7) -> Dict[str, Any]:
        """Get data quality metrics for recent data.
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            Dict with data quality metrics
        """
        logger.info(f"Collecting data quality metrics ({days_back} days)", extra={
            'correlation_id': self.correlation_id,
            'days_back': days_back
        })
        
        quality_start_time = time.time()
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            with psycopg.connect(self.config.database_url) as conn:
                conn.row_factory = dict_row
                
                with conn.cursor() as cur:
                    # Recent data volume metrics
                    cur.execute("""
                        SELECT 
                            'prime_awards' as data_type,
                            COUNT(*) as total_records,
                            COUNT(DISTINCT award_id_piid) as unique_awards,
                            MIN(etl_created_at) as earliest_date,
                            MAX(etl_created_at) as latest_date
                        FROM s1_raw_usaspending_prime_awards_slimv2 
                        WHERE etl_created_at >= %s
                        UNION ALL
                        SELECT 
                            'subawards' as data_type,
                            COUNT(*) as total_records,
                            COUNT(DISTINCT subaward_number) as unique_awards,
                            MIN(etl_created_at) as earliest_date,
                            MAX(etl_created_at) as latest_date
                        FROM s1_raw_usaspending_subawards_v2 
                        WHERE etl_created_at >= %s
                    """, (cutoff_date, cutoff_date))
                    
                    recent_data = cur.fetchall()
                    
                    # Data quality checks
                    quality_checks = []
                    
                    # Prime awards quality checks
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(CASE WHEN award_id_piid IS NULL OR award_id_piid = '' THEN 1 END) as missing_award_id,
                            COUNT(CASE WHEN total_obligation_amount IS NULL THEN 1 END) as missing_amount,
                            COUNT(CASE WHEN awarding_agency_name IS NULL OR awarding_agency_name = '' THEN 1 END) as missing_agency,
                            COUNT(CASE WHEN description IS NULL OR description = '' THEN 1 END) as missing_description
                        FROM s1_raw_usaspending_prime_awards_slimv2 
                        WHERE etl_created_at >= %s
                    """, (cutoff_date,))
                    
                    prime_quality = cur.fetchone()
                    
                    if prime_quality['total_records'] > 0:
                        quality_checks.append({
                            'data_type': 'prime_awards',
                            'total_records': prime_quality['total_records'],
                            'quality_metrics': {
                                'missing_award_id_pct': round((prime_quality['missing_award_id'] / prime_quality['total_records']) * 100, 2),
                                'missing_amount_pct': round((prime_quality['missing_amount'] / prime_quality['total_records']) * 100, 2),
                                'missing_agency_pct': round((prime_quality['missing_agency'] / prime_quality['total_records']) * 100, 2),
                                'missing_description_pct': round((prime_quality['missing_description'] / prime_quality['total_records']) * 100, 2)
                            }
                        })
                    
                    # Subawards quality checks
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(CASE WHEN subaward_number IS NULL OR subaward_number = '' THEN 1 END) as missing_subaward_number,
                            COUNT(CASE WHEN subaward_amount IS NULL THEN 1 END) as missing_amount,
                            COUNT(CASE WHEN sub_awardee_or_recipient_legal_entity_name IS NULL OR sub_awardee_or_recipient_legal_entity_name = '' THEN 1 END) as missing_recipient
                        FROM s1_raw_usaspending_subawards_v2 
                        WHERE etl_created_at >= %s
                    """, (cutoff_date,))
                    
                    subaward_quality = cur.fetchone()
                    
                    if subaward_quality['total_records'] > 0:
                        quality_checks.append({
                            'data_type': 'subawards',
                            'total_records': subaward_quality['total_records'],
                            'quality_metrics': {
                                'missing_subaward_number_pct': round((subaward_quality['missing_subaward_number'] / subaward_quality['total_records']) * 100, 2),
                                'missing_amount_pct': round((subaward_quality['missing_amount'] / subaward_quality['total_records']) * 100, 2),
                                'missing_recipient_pct': round((subaward_quality['missing_recipient'] / subaward_quality['total_records']) * 100, 2)
                            }
                        })
                    
                    # Processed layer metrics
                    cur.execute("""
                        SELECT 
                            'processed_canonical' as layer,
                            COUNT(*) as total_records,
                            COUNT(CASE WHEN award_type = 'prime_award' THEN 1 END) as prime_awards_count,
                            COUNT(CASE WHEN award_type = 'subaward' THEN 1 END) as subawards_count,
                            AVG(CASE WHEN total_amount IS NOT NULL THEN total_amount END) as avg_amount,
                            SUM(CASE WHEN total_amount IS NOT NULL THEN total_amount END) as total_amount_sum
                        FROM s3_processed_canonical_prime_awards 
                        WHERE etl_created_at >= %s
                        UNION ALL
                        SELECT 
                            'processed_subawards' as layer,
                            COUNT(*) as total_records,
                            0 as prime_awards_count,
                            COUNT(*) as subawards_count,
                            AVG(CASE WHEN subaward_amount IS NOT NULL THEN subaward_amount END) as avg_amount,
                            SUM(CASE WHEN subaward_amount IS NOT NULL THEN subaward_amount END) as total_amount_sum
                        FROM s3_processed_canonical_subawards 
                        WHERE etl_created_at >= %s
                    """, (cutoff_date, cutoff_date))
                    
                    processed_metrics = cur.fetchall()
            
            data_quality_metrics = {
                'correlation_id': self.correlation_id,
                'timestamp': datetime.now().isoformat(),
                'analysis_period': {
                    'days_back': days_back,
                    'cutoff_date': cutoff_date.isoformat()
                },
                'recent_data_volume': [
                    {
                        'data_type': row['data_type'],
                        'total_records': row['total_records'],
                        'unique_awards': row['unique_awards'],
                        'earliest_date': row['earliest_date'].isoformat() if row['earliest_date'] else None,
                        'latest_date': row['latest_date'].isoformat() if row['latest_date'] else None,
                        'records_per_day': round(row['total_records'] / days_back, 1) if row['total_records'] > 0 else 0
                    }
                    for row in recent_data
                ],
                'quality_checks': quality_checks,
                'processed_layer_metrics': [
                    {
                        'layer': row['layer'],
                        'total_records': row['total_records'],
                        'prime_awards_count': row['prime_awards_count'],
                        'subawards_count': row['subawards_count'],
                        'average_amount': float(row['avg_amount']) if row['avg_amount'] else None,
                        'total_amount_sum': float(row['total_amount_sum']) if row['total_amount_sum'] else None
                    }
                    for row in processed_metrics
                ],
                'collection_time_seconds': round(time.time() - quality_start_time, 3)
            }
            
            # Calculate overall quality score
            total_quality_issues = 0
            total_records_checked = 0
            
            for check in quality_checks:
                total_records_checked += check['total_records']
                for metric_name, metric_value in check['quality_metrics'].items():
                    if metric_value > 5.0:  # Consider >5% missing data as an issue
                        total_quality_issues += (metric_value / 100) * check['total_records']
            
            if total_records_checked > 0:
                quality_score = max(0, 100 - ((total_quality_issues / total_records_checked) * 100))
                data_quality_metrics['overall_quality_score'] = round(quality_score, 2)
                
                if quality_score >= 95:
                    data_quality_metrics['quality_status'] = 'excellent'
                elif quality_score >= 85:
                    data_quality_metrics['quality_status'] = 'good'
                elif quality_score >= 70:
                    data_quality_metrics['quality_status'] = 'fair'
                else:
                    data_quality_metrics['quality_status'] = 'poor'
            else:
                data_quality_metrics['overall_quality_score'] = None
                data_quality_metrics['quality_status'] = 'no_data'
            
            logger.info("Data quality metrics collected", extra={
                'correlation_id': self.correlation_id,
                'quality_score': data_quality_metrics.get('overall_quality_score'),
                'quality_status': data_quality_metrics.get('quality_status')
            })
            
            return data_quality_metrics
            
        except Exception as e:
            logger.error("Failed to collect data quality metrics", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            return {
                'correlation_id': self.correlation_id,
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'collection_time_seconds': round(time.time() - quality_start_time, 3)
            }
    
    def generate_dashboard_report(self, include_quality_metrics: bool = True) -> Dict[str, Any]:
        """Generate comprehensive dashboard report.
        
        Args:
            include_quality_metrics: Whether to include data quality analysis
            
        Returns:
            Dict with complete dashboard report
        """
        logger.info("Generating dashboard report", extra={
            'correlation_id': self.correlation_id,
            'include_quality_metrics': include_quality_metrics
        })
        
        report_start_time = time.time()
        
        dashboard_report = {
            'report_type': 'monitoring_dashboard',
            'correlation_id': self.correlation_id,
            'timestamp': datetime.now().isoformat(),
            'sections': {}
        }
        
        try:
            # Section 1: Pipeline status
            logger.info("Collecting pipeline status")
            dashboard_report['sections']['pipeline_status'] = self.get_pipeline_status()
            
            # Section 2: System metrics
            logger.info("Collecting system metrics")
            dashboard_report['sections']['system_metrics'] = self.get_system_metrics()
            
            # Section 3: Data quality metrics (optional)
            if include_quality_metrics:
                logger.info("Collecting data quality metrics")
                dashboard_report['sections']['data_quality_metrics'] = self.get_data_quality_metrics()
            
            # Generate executive summary
            pipeline_health = dashboard_report['sections']['pipeline_status'].get('health_status', 'unknown')
            disk_status = dashboard_report['sections']['system_metrics']['disk'].get('status', 'unknown')
            quality_status = dashboard_report['sections'].get('data_quality_metrics', {}).get('quality_status', 'not_analyzed')
            
            # Determine overall system health
            if any(status in ['error', 'failed'] for status in [pipeline_health, disk_status]):
                overall_health = 'critical'
            elif any(status in ['warning', 'stale', 'degraded'] for status in [pipeline_health, disk_status]):
                overall_health = 'warning'
            elif quality_status in ['poor', 'fair']:
                overall_health = 'warning'
            else:
                overall_health = 'healthy'
            
            # Generate alerts
            alerts = []
            
            if pipeline_health == 'stale':
                days_stale = dashboard_report['sections']['pipeline_status']['watermark'].get('days_since_last_update', 0)
                alerts.append({
                    'severity': 'warning',
                    'category': 'pipeline',
                    'message': f'Pipeline data is stale ({days_stale} days since last update)',
                    'action': 'Check incremental pipeline execution'
                })
            
            if disk_status == 'warning':
                available_gb = dashboard_report['sections']['system_metrics']['disk'].get('available_space_gb', 0)
                alerts.append({
                    'severity': 'warning',
                    'category': 'storage',
                    'message': f'Low disk space ({available_gb:.1f}GB available)',
                    'action': 'Run maintenance cleanup or increase storage'
                })
            
            if quality_status in ['poor', 'fair']:
                quality_score = dashboard_report['sections'].get('data_quality_metrics', {}).get('overall_quality_score', 0)
                alerts.append({
                    'severity': 'warning',
                    'category': 'data_quality',
                    'message': f'Data quality issues detected (score: {quality_score:.1f}%)',
                    'action': 'Review data quality metrics and investigate sources'
                })
            
            if not alerts:
                alerts.append({
                    'severity': 'info',
                    'category': 'status',
                    'message': 'All systems operational',
                    'action': 'Continue monitoring'
                })
            
            dashboard_report.update({
                'executive_summary': {
                    'overall_health': overall_health,
                    'pipeline_status': pipeline_health,
                    'system_status': disk_status,
                    'data_quality_status': quality_status,
                    'total_alerts': len([a for a in alerts if a['severity'] != 'info']),
                    'last_updated': datetime.now().isoformat()
                },
                'alerts': alerts,
                'total_execution_time_seconds': round(time.time() - report_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info("Dashboard report completed", extra={
                'correlation_id': self.correlation_id,
                'overall_health': overall_health,
                'total_alerts': len(alerts) - 1,  # Exclude info alerts
                'execution_time': dashboard_report['total_execution_time_seconds']
            })
            
            return dashboard_report
            
        except Exception as e:
            dashboard_report.update({
                'total_execution_time_seconds': round(time.time() - report_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Dashboard report failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise


def format_dashboard_output(report: Dict[str, Any]) -> str:
    """Format dashboard report for console display.
    
    Args:
        report: Dashboard report dictionary
        
    Returns:
        Formatted string for console output
    """
    output = []
    
    # Header
    output.append("=" * 80)
    output.append("📊 ETL PIPELINE MONITORING DASHBOARD")
    output.append("=" * 80)
    output.append(f"Report Time: {report.get('timestamp', 'N/A')}")
    output.append(f"Correlation ID: {report.get('correlation_id', 'N/A')}")
    output.append("")
    
    # Executive Summary
    if 'executive_summary' in report:
        summary = report['executive_summary']
        health_icon = {"healthy": "✅", "warning": "⚠️", "critical": "🔴"}.get(summary.get('overall_health'), "❓")
        
        output.append("📋 EXECUTIVE SUMMARY")
        output.append("-" * 40)
        output.append(f"Overall Health: {health_icon} {summary.get('overall_health', 'unknown').upper()}")
        output.append(f"Pipeline Status: {summary.get('pipeline_status', 'unknown').upper()}")
        output.append(f"System Status: {summary.get('system_status', 'unknown').upper()}")
        output.append(f"Data Quality: {summary.get('data_quality_status', 'unknown').upper()}")
        output.append(f"Active Alerts: {summary.get('total_alerts', 0)}")
        output.append("")
    
    # Alerts
    if 'alerts' in report and report['alerts']:
        output.append("🚨 ALERTS & NOTIFICATIONS")
        output.append("-" * 40)
        for alert in report['alerts']:
            severity_icon = {"warning": "⚠️", "error": "🔴", "info": "ℹ️"}.get(alert.get('severity'), "❓")
            output.append(f"{severity_icon} [{alert.get('severity', 'unknown').upper()}] {alert.get('message', 'No message')}")
            output.append(f"   Action: {alert.get('action', 'No action specified')}")
        output.append("")
    
    # Pipeline Status
    if 'sections' in report and 'pipeline_status' in report['sections']:
        pipeline = report['sections']['pipeline_status']
        output.append("🔄 PIPELINE STATUS")
        output.append("-" * 40)
        
        if 'watermark' in pipeline:
            watermark = pipeline['watermark']
            output.append(f"Last Watermark: {watermark.get('last_modified_to', 'N/A')}")
            output.append(f"Days Since Update: {watermark.get('days_since_last_update', 'N/A')}")
        
        if 'recent_activity' in pipeline:
            activity = pipeline['recent_activity']
            output.append(f"Recent Runs: {len(activity) if isinstance(activity, list) else 'N/A'}")
        
        output.append(f"Health Status: {pipeline.get('health_status', 'unknown').upper()}")
        output.append("")
    
    # System Metrics
    if 'sections' in report and 'system_metrics' in report['sections']:
        system = report['sections']['system_metrics']
        output.append("💻 SYSTEM METRICS")
        output.append("-" * 40)
        
        if 'disk' in system:
            disk = system['disk']
            output.append(f"Available Space: {disk.get('available_space_gb', 'N/A')}GB")
            output.append(f"Disk Utilization: {disk.get('utilization_percent', 'N/A'):.1f}%")
            output.append(f"Disk Status: {disk.get('status', 'unknown').upper()}")
        
        if 'database' in system:
            db = system['database']
            output.append(f"Database Size: {db.get('database_size_pretty', 'N/A')}")
            output.append(f"Active Connections: {db.get('active_connections', 'N/A')}")
            output.append(f"Live Rows: {db.get('total_live_rows', 'N/A'):,}")
        
        output.append("")
    
    # Data Quality
    if 'sections' in report and 'data_quality_metrics' in report['sections']:
        quality = report['sections']['data_quality_metrics']
        output.append("📈 DATA QUALITY METRICS")
        output.append("-" * 40)
        
        output.append(f"Quality Score: {quality.get('overall_quality_score', 'N/A')}%")
        output.append(f"Quality Status: {quality.get('quality_status', 'unknown').upper()}")
        
        if 'recent_data_volume' in quality:
            for data in quality['recent_data_volume']:
                output.append(f"{data['data_type'].title()}: {data.get('total_records', 0):,} records")
        
        output.append("")
    
    # Footer
    output.append("-" * 80)
    output.append(f"Report generated in {report.get('total_execution_time_seconds', 0):.3f}s")
    output.append("=" * 80)
    
    return "\n".join(output)


def main():
    """CLI entry point for monitoring dashboard."""
    parser = argparse.ArgumentParser(description="ETL Pipeline Monitoring Dashboard")
    parser.add_argument('--mode', type=str, 
                       choices=['status', 'metrics', 'quality', 'full'],
                       default='full',
                       help='Dashboard mode (default: full)')
    parser.add_argument('--output-file', type=str,
                       help='Save report to JSON file')
    parser.add_argument('--watch', action='store_true',
                       help='Watch mode - refresh every 30 seconds')
    parser.add_argument('--refresh-interval', type=int, default=30,
                       help='Refresh interval in seconds for watch mode (default: 30)')
    
    args = parser.parse_args()
    
    try:
        # Initialize dashboard
        dashboard = MonitoringDashboard()
        
        def generate_report():
            """Generate appropriate report based on mode."""
            if args.mode == 'status':
                return dashboard.get_pipeline_status()
            elif args.mode == 'metrics':
                return dashboard.get_system_metrics()
            elif args.mode == 'quality':
                return dashboard.get_data_quality_metrics()
            else:  # full
                return dashboard.generate_dashboard_report()
        
        def display_report(report):
            """Display report to console."""
            if args.mode == 'full':
                print(format_dashboard_output(report))
            else:
                print(json.dumps(report, indent=2, default=str))
        
        if args.watch:
            print(f"🔍 ETL Pipeline Monitoring Dashboard - Watch Mode")
            print(f"Mode: {args.mode} | Refresh: {args.refresh_interval}s")
            print(f"Press Ctrl+C to exit")
            print("=" * 80)
            
            try:
                while True:
                    report = generate_report()
                    
                    # Clear screen (basic version)
                    print("\033[H\033[J", end="")
                    
                    display_report(report)
                    
                    # Save to file if requested
                    if args.output_file:
                        output_path = Path(args.output_file)
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(output_path, 'w') as f:
                            json.dump(report, f, indent=2, default=str)
                    
                    time.sleep(args.refresh_interval)
                    
            except KeyboardInterrupt:
                print("\n\n👋 Monitoring stopped by user")
        else:
            # Single report mode
            print(f"📊 ETL Pipeline Monitoring Dashboard - {args.mode.title()} Mode")
            
            report = generate_report()
            display_report(report)
            
            # Save to file if requested
            if args.output_file:
                output_path = Path(args.output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                
                print(f"\n💾 Report saved to: {output_path}")
            
            # Exit with appropriate code based on health
            if hasattr(report, 'get'):
                overall_health = report.get('executive_summary', {}).get('overall_health', 'unknown')
                if overall_health == 'critical':
                    exit(1)
                elif overall_health == 'warning':
                    exit(2)
    
    except Exception as e:
        print(f"❌ Dashboard failed: {e}")
        logger.exception("Dashboard execution failed")
        exit(1)


if __name__ == '__main__':
    main()