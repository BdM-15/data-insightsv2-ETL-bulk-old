"""
Pipeline Metrics Collection System

Provides comprehensive metrics collection for monitoring pipeline performance,
data quality, and operational health. Designed for PostgreSQL backend with
JSON logging and dashboard integration.

Constitution adherence:
- SQL-first: Uses database for metrics storage and aggregation
- Fail-fast: Validates inputs and fails on configuration errors
- Storage-conscious: Efficient aggregation and retention policies
- Modular: Standalone metrics collection independent of other components
"""

import json
import logging
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import psycopg

from ..config import Config
from .logging import get_logger


class MetricsCollector:
    """
    Collects and stores pipeline metrics for monitoring and analysis.
    
    Tracks:
    - Pipeline execution times and throughput
    - Data quality metrics (record counts, error rates)
    - Resource utilization (disk usage, API calls)
    - System health indicators
    """
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.logger = get_logger(self.__class__.__name__)
        self._connection: Optional[psycopg.Connection] = None
        
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
        """Establish database connection for metrics storage."""
        try:
            self._connection = psycopg.connect(
                host=self.config.database_host,
                port=self.config.database_port,
                dbname=self.config.database_name,
                user=self.config.database_user,
                password=self.config.database_password,
                autocommit=True
            )
            self.logger.debug("Connected to database for metrics collection")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
            
    def record_pipeline_start(self, pipeline_name: str, pipeline_type: str, 
                             params: Optional[Dict[str, Any]] = None) -> str:
        """
        Record the start of a pipeline execution.
        
        Args:
            pipeline_name: Name of the pipeline (e.g., 'historical', 'incremental')
            pipeline_type: Type of pipeline ('full', 'incremental', 'diagnostic')
            params: Optional parameters passed to the pipeline
            
        Returns:
            str: Unique execution ID for tracking this run
        """
        if not self._connection:
            self._connect()
            
        execution_id = f"{pipeline_name}_{int(time.time())}"
        
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO s3_processed.pipeline_executions (
                        execution_id, pipeline_name, pipeline_type, status,
                        start_time, parameters, created_at
                    ) VALUES (%s, %s, %s, 'running', %s, %s, NOW())
                """, (
                    execution_id,
                    pipeline_name,
                    pipeline_type,
                    datetime.now(),
                    json.dumps(params or {})
                ))
                
            self.logger.info(f"Pipeline execution started: {execution_id}")
            return execution_id
            
        except Exception as e:
            self.logger.error(f"Failed to record pipeline start: {e}")
            raise
            
    def record_pipeline_end(self, execution_id: str, status: str,
                           records_processed: Optional[int] = None,
                           error_message: Optional[str] = None):
        """
        Record the completion of a pipeline execution.
        
        Args:
            execution_id: Execution ID from pipeline start
            status: Final status ('completed', 'failed', 'cancelled')
            records_processed: Number of records processed
            error_message: Error message if status is 'failed'
        """
        if not self._connection:
            self._connect()
            
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    UPDATE s3_processed.pipeline_executions
                    SET status = %s,
                        end_time = %s,
                        duration_seconds = EXTRACT(EPOCH FROM (%s - start_time)),
                        records_processed = %s,
                        error_message = %s,
                        updated_at = NOW()
                    WHERE execution_id = %s
                """, (
                    status,
                    datetime.now(),
                    datetime.now(),
                    records_processed,
                    error_message,
                    execution_id
                ))
                
            self.logger.info(f"Pipeline execution completed: {execution_id} ({status})")
            
        except Exception as e:
            self.logger.error(f"Failed to record pipeline end: {e}")
            raise
            
    def record_stage_metrics(self, execution_id: str, stage_name: str,
                            metrics: Dict[str, Any]):
        """
        Record metrics for a specific pipeline stage.
        
        Args:
            execution_id: Execution ID from pipeline start
            stage_name: Name of the pipeline stage (e.g., 'acquisition', 'staging')
            metrics: Dictionary of metrics to record
        """
        if not self._connection:
            self._connect()
            
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO s3_processed.stage_metrics (
                        execution_id, stage_name, metrics_data, recorded_at
                    ) VALUES (%s, %s, %s, NOW())
                """, (
                    execution_id,
                    stage_name,
                    json.dumps(metrics)
                ))
                
            self.logger.debug(f"Stage metrics recorded: {stage_name} for {execution_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to record stage metrics: {e}")
            raise
            
    def record_data_quality_metrics(self, execution_id: str, table_name: str,
                                   metrics: Dict[str, Any]):
        """
        Record data quality metrics for a specific table.
        
        Args:
            execution_id: Execution ID from pipeline start
            table_name: Name of the table being analyzed
            metrics: Dictionary of quality metrics (nulls, duplicates, etc.)
        """
        if not self._connection:
            self._connect()
            
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO s3_processed.data_quality_metrics (
                        execution_id, table_name, quality_metrics, recorded_at
                    ) VALUES (%s, %s, %s, NOW())
                """, (
                    execution_id,
                    table_name,
                    json.dumps(metrics)
                ))
                
            self.logger.debug(f"Data quality metrics recorded: {table_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to record data quality metrics: {e}")
            raise
            
    def record_resource_usage(self, execution_id: str, resource_type: str,
                             usage_data: Dict[str, Any]):
        """
        Record resource usage metrics.
        
        Args:
            execution_id: Execution ID from pipeline start
            resource_type: Type of resource ('disk', 'memory', 'api_calls')
            usage_data: Dictionary of usage metrics
        """
        if not self._connection:
            self._connect()
            
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    INSERT INTO s3_processed.resource_usage (
                        execution_id, resource_type, usage_data, recorded_at
                    ) VALUES (%s, %s, %s, NOW())
                """, (
                    execution_id,
                    resource_type,
                    json.dumps(usage_data)
                ))
                
            self.logger.debug(f"Resource usage recorded: {resource_type}")
            
        except Exception as e:
            self.logger.error(f"Failed to record resource usage: {e}")
            raise
            
    def get_pipeline_summary(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get pipeline execution summary for the last N days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            List[Dict]: Summary of pipeline executions
        """
        if not self._connection:
            self._connect()
            
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    SELECT 
                        pipeline_name,
                        pipeline_type,
                        COUNT(*) as total_runs,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_runs,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                        AVG(duration_seconds) as avg_duration_seconds,
                        SUM(COALESCE(records_processed, 0)) as total_records_processed
                    FROM s3_processed.pipeline_executions
                    WHERE start_time >= NOW() - INTERVAL '%s days'
                    GROUP BY pipeline_name, pipeline_type
                    ORDER BY pipeline_name, pipeline_type
                """, (days,))
                
                return [dict(row) for row in cur.fetchall()]
                
        except Exception as e:
            self.logger.error(f"Failed to get pipeline summary: {e}")
            raise
            
    def get_recent_failures(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get recent pipeline failures for alerting.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List[Dict]: Recent failed executions
        """
        if not self._connection:
            self._connect()
            
        try:
            with self._connection.cursor() as cur:
                cur.execute("""
                    SELECT 
                        execution_id,
                        pipeline_name,
                        pipeline_type,
                        start_time,
                        end_time,
                        error_message
                    FROM s3_processed.pipeline_executions
                    WHERE status = 'failed'
                      AND start_time >= NOW() - INTERVAL '%s hours'
                    ORDER BY start_time DESC
                """, (hours,))
                
                return [dict(row) for row in cur.fetchall()]
                
        except Exception as e:
            self.logger.error(f"Failed to get recent failures: {e}")
            raise
            
    def cleanup_old_metrics(self, retention_days: int = 90):
        """
        Clean up old metrics data to manage storage.
        
        Args:
            retention_days: Number of days of metrics to retain
        """
        if not self._connection:
            self._connect()
            
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        try:
            with self._connection.cursor() as cur:
                # Clean up pipeline executions
                cur.execute("""
                    DELETE FROM s3_processed.pipeline_executions
                    WHERE start_time < %s
                """, (cutoff_date,))
                
                deleted_executions = cur.rowcount
                
                # Clean up stage metrics
                cur.execute("""
                    DELETE FROM s3_processed.stage_metrics
                    WHERE recorded_at < %s
                """, (cutoff_date,))
                
                deleted_stages = cur.rowcount
                
                # Clean up data quality metrics
                cur.execute("""
                    DELETE FROM s3_processed.data_quality_metrics
                    WHERE recorded_at < %s
                """, (cutoff_date,))
                
                deleted_quality = cur.rowcount
                
                # Clean up resource usage
                cur.execute("""
                    DELETE FROM s3_processed.resource_usage
                    WHERE recorded_at < %s
                """, (cutoff_date,))
                
                deleted_resources = cur.rowcount
                
            self.logger.info(
                f"Cleaned up metrics older than {retention_days} days: "
                f"{deleted_executions} executions, {deleted_stages} stages, "
                f"{deleted_quality} quality metrics, {deleted_resources} resource metrics"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old metrics: {e}")
            raise


class PipelineTimer:
    """
    Context manager for timing pipeline operations and automatically
    recording metrics.
    """
    
    def __init__(self, metrics_collector: MetricsCollector, 
                 execution_id: str, stage_name: str):
        self.metrics_collector = metrics_collector
        self.execution_id = execution_id
        self.stage_name = stage_name
        self.start_time = None
        self.logger = get_logger(f"PipelineTimer.{stage_name}")
        
    def __enter__(self):
        """Start timing."""
        self.start_time = time.time()
        self.logger.info(f"Started timing stage: {self.stage_name}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and record metrics."""
        duration = time.time() - self.start_time
        
        metrics = {
            'duration_seconds': duration,
            'stage_name': self.stage_name,
            'success': exc_type is None
        }
        
        if exc_type is not None:
            metrics['error'] = str(exc_val)
            metrics['error_type'] = exc_type.__name__
            
        try:
            self.metrics_collector.record_stage_metrics(
                self.execution_id, 
                self.stage_name, 
                metrics
            )
            self.logger.info(
                f"Completed timing stage: {self.stage_name} "
                f"({duration:.2f}s, success={exc_type is None})"
            )
        except Exception as e:
            self.logger.error(f"Failed to record stage timing: {e}")


def create_metrics_tables(connection: psycopg.Connection):
    """
    Create the metrics tables if they don't exist.
    
    Args:
        connection: PostgreSQL connection
    """
    logger = get_logger("metrics.create_tables")
    
    try:
        with connection.cursor() as cur:
            # Pipeline executions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS s3_processed.pipeline_executions (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(255) UNIQUE NOT NULL,
                    pipeline_name VARCHAR(100) NOT NULL,
                    pipeline_type VARCHAR(50) NOT NULL,
                    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    duration_seconds NUMERIC(10,2),
                    records_processed BIGINT,
                    parameters JSONB,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Stage metrics table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS s3_processed.stage_metrics (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(255) NOT NULL,
                    stage_name VARCHAR(100) NOT NULL,
                    metrics_data JSONB NOT NULL,
                    recorded_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (execution_id) REFERENCES s3_processed.pipeline_executions(execution_id) ON DELETE CASCADE
                )
            """)
            
            # Data quality metrics table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS s3_processed.data_quality_metrics (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(255) NOT NULL,
                    table_name VARCHAR(100) NOT NULL,
                    quality_metrics JSONB NOT NULL,
                    recorded_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (execution_id) REFERENCES s3_processed.pipeline_executions(execution_id) ON DELETE CASCADE
                )
            """)
            
            # Resource usage table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS s3_processed.resource_usage (
                    id SERIAL PRIMARY KEY,
                    execution_id VARCHAR(255) NOT NULL,
                    resource_type VARCHAR(50) NOT NULL,
                    usage_data JSONB NOT NULL,
                    recorded_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (execution_id) REFERENCES s3_processed.pipeline_executions(execution_id) ON DELETE CASCADE
                )
            """)
            
            # Create indexes for performance
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_executions_start_time ON s3_processed.pipeline_executions(start_time)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status ON s3_processed.pipeline_executions(status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_stage_metrics_execution_id ON s3_processed.stage_metrics(execution_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_data_quality_execution_id ON s3_processed.data_quality_metrics(execution_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_resource_usage_execution_id ON s3_processed.resource_usage(execution_id)")
            
        connection.commit()
        logger.info("Metrics tables created successfully")
        
    except Exception as e:
        logger.error(f"Failed to create metrics tables: {e}")
        raise