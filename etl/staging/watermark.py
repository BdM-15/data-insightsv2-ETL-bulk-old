"""
Watermark management for incremental ETL processing.

This module manages data freshness watermarks to support incremental processing
and overlap detection, ensuring efficient and complete data coverage.
"""

import logging
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime, date, timedelta
from dataclasses import dataclass
import psycopg
from psycopg.rows import dict_row

from ..config import get_config
from ..utils.logging import get_logger
from ..staging.raw_loader import get_db_connection

logger = get_logger(__name__)


class WatermarkError(Exception):
    """Raised when watermark operations fail."""
    pass


@dataclass
class WatermarkInfo:
    """
    Represents a pipeline watermark record.
    
    Attributes:
        pipeline_name: Name of the ETL pipeline
        last_modified_to: Last processed timestamp (high watermark)
        overlap_days: Number of overlap days for incremental processing
        updated_at: When this watermark was last updated
    """
    pipeline_name: str
    last_modified_to: datetime
    overlap_days: int
    updated_at: Optional[datetime] = None
    
    @property
    def incremental_start_date(self) -> date:
        """
        Calculate the start date for incremental processing.
        
        Returns overlap_days before last_modified_to to catch late updates.
        """
        return (self.last_modified_to - timedelta(days=self.overlap_days)).date()
    
    @property
    def days_since_last_update(self) -> int:
        """Calculate days since last watermark update."""
        if self.updated_at:
            return (datetime.now() - self.updated_at).days
        return 0
    
    def get_incremental_window(self, end_date: Optional[date] = None) -> Tuple[date, date]:
        """
        Get date window for incremental processing.
        
        Args:
            end_date: End date for processing (defaults to today)
            
        Returns:
            Tuple of (start_date, end_date) for incremental processing
        """
        start_date = self.incremental_start_date
        if end_date is None:
            end_date = date.today()
        
        return start_date, end_date


class WatermarkManager:
    """
    Manages refresh watermarks for incremental ETL processing.
    
    Features:
    - Track high watermarks for each pipeline
    - Calculate incremental processing windows with overlap
    - Handle first-time runs (no existing watermark)
    - Support different overlap strategies (daily vs monthly)
    - Atomic watermark updates after successful processing
    """
    
    def __init__(self, pipeline_name: Optional[str] = None):
        """
        Initialize watermark manager.
        
        Args:
            pipeline_name: Pipeline name (defaults to config.pipeline_name)
        """
        config = get_config()
        self.pipeline_name = pipeline_name or config.pipeline_name
        self.default_overlap_days = config.current_days_lookback
        
        logger.debug(f"Initialized WatermarkManager for pipeline '{self.pipeline_name}'")
    
    def get_watermark(self) -> Optional[WatermarkInfo]:
        """
        Get current watermark for the pipeline.
        
        Returns:
            WatermarkInfo if watermark exists, None for first-time runs
            
        Raises:
            WatermarkError: If database query fails
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT pipeline_name, last_modified_to, overlap_days, updated_at
                    FROM capture_insights.meta_refresh_watermarks
                    WHERE pipeline_name = %s
                    """, (self.pipeline_name,))
                    
                    row = cursor.fetchone()
                    if row:
                        return WatermarkInfo(
                            pipeline_name=row['pipeline_name'],
                            last_modified_to=row['last_modified_to'],
                            overlap_days=row['overlap_days'],
                            updated_at=row['updated_at']
                        )
                    return None
        
        except Exception as e:
            logger.error(f"Failed to get watermark: {e}")
            raise WatermarkError(f"Watermark query failed: {e}")
    
    def set_watermark(
        self,
        last_modified_to: datetime,
        overlap_days: Optional[int] = None
    ) -> WatermarkInfo:
        """
        Set or update watermark for the pipeline.
        
        Args:
            last_modified_to: New high watermark timestamp
            overlap_days: Override overlap days (defaults to config value)
            
        Returns:
            Updated WatermarkInfo
            
        Raises:
            WatermarkError: If update fails
        """
        if overlap_days is None:
            overlap_days = self.default_overlap_days
        
        logger.info(f"Setting watermark for {self.pipeline_name}: {last_modified_to} (overlap: {overlap_days} days)")
        
        try:
            with get_db_connection() as conn:
                with conn.transaction():
                    with conn.cursor() as cursor:
                        # Use UPSERT (INSERT ... ON CONFLICT)
                        cursor.execute("""
                        INSERT INTO capture_insights.meta_refresh_watermarks 
                        (pipeline_name, last_modified_to, overlap_days, updated_at)
                        VALUES (%s, %s, %s, now())
                        ON CONFLICT (pipeline_name) DO UPDATE SET
                            last_modified_to = EXCLUDED.last_modified_to,
                            overlap_days = EXCLUDED.overlap_days,
                            updated_at = now()
                        """, (self.pipeline_name, last_modified_to, overlap_days))
            
            # Return updated watermark info
            return WatermarkInfo(
                pipeline_name=self.pipeline_name,
                last_modified_to=last_modified_to,
                overlap_days=overlap_days,
                updated_at=datetime.now()
            )
        
        except Exception as e:
            logger.error(f"Failed to set watermark: {e}")
            raise WatermarkError(f"Watermark update failed: {e}")
    
    def get_incremental_window(self, end_date: Optional[date] = None) -> Tuple[date, date]:
        """
        Get incremental processing window based on current watermark.
        
        Args:
            end_date: End date for processing (defaults to today)
            
        Returns:
            Tuple of (start_date, end_date) for incremental processing
            
        Raises:
            WatermarkError: If watermark operations fail
        """
        watermark = self.get_watermark()
        
        if watermark is None:
            # First-time run - use a reasonable default lookback
            if end_date is None:
                end_date = date.today()
            start_date = end_date - timedelta(days=30)  # 30-day default for first run
            
            logger.info(f"No existing watermark, using default window: {start_date} to {end_date}")
            return start_date, end_date
        
        # Use existing watermark
        return watermark.get_incremental_window(end_date)
    
    def advance_watermark(self, new_timestamp: datetime) -> WatermarkInfo:
        """
        Advance watermark to a new timestamp (only if newer).
        
        Args:
            new_timestamp: New timestamp to advance to
            
        Returns:
            Updated WatermarkInfo
            
        Raises:
            WatermarkError: If advance operation fails
        """
        current_watermark = self.get_watermark()
        
        if current_watermark and new_timestamp <= current_watermark.last_modified_to:
            logger.warning(f"New timestamp {new_timestamp} is not newer than current watermark {current_watermark.last_modified_to}")
            return current_watermark
        
        # Preserve existing overlap_days or use default
        overlap_days = current_watermark.overlap_days if current_watermark else self.default_overlap_days
        
        return self.set_watermark(new_timestamp, overlap_days)
    
    def update_overlap_days(self, overlap_days: int) -> WatermarkInfo:
        """
        Update overlap days for existing watermark.
        
        Args:
            overlap_days: New overlap days value
            
        Returns:
            Updated WatermarkInfo
            
        Raises:
            WatermarkError: If update fails
        """
        current_watermark = self.get_watermark()
        
        if current_watermark is None:
            raise WatermarkError("Cannot update overlap days: no existing watermark")
        
        return self.set_watermark(current_watermark.last_modified_to, overlap_days)
    
    def calculate_processing_lag(self) -> Optional[int]:
        """
        Calculate processing lag in days (current watermark vs today).
        
        Returns:
            Number of days behind current date, None if no watermark
        """
        watermark = self.get_watermark()
        
        if watermark is None:
            return None
        
        today = date.today()
        watermark_date = watermark.last_modified_to.date()
        
        return (today - watermark_date).days
    
    def is_incremental_feasible(self, max_lag_days: int = 30) -> bool:
        """
        Check if incremental processing is feasible or if full refresh is needed.
        
        Args:
            max_lag_days: Maximum acceptable lag for incremental processing
            
        Returns:
            True if incremental processing is recommended
        """
        lag = self.calculate_processing_lag()
        
        if lag is None:
            # No watermark - first run, incremental not applicable
            return False
        
        return lag <= max_lag_days
    
    def get_watermark_status(self) -> Dict[str, Any]:
        """
        Get comprehensive watermark status information.
        
        Returns:
            Dictionary with watermark status and metadata
        """
        watermark = self.get_watermark()
        
        if watermark is None:
            return {
                'pipeline_name': self.pipeline_name,
                'has_watermark': False,
                'first_run': True,
                'incremental_feasible': False
            }
        
        lag_days = self.calculate_processing_lag()
        incremental_start, incremental_end = watermark.get_incremental_window()
        
        return {
            'pipeline_name': self.pipeline_name,
            'has_watermark': True,
            'first_run': False,
            'last_modified_to': watermark.last_modified_to.isoformat(),
            'overlap_days': watermark.overlap_days,
            'updated_at': watermark.updated_at.isoformat() if watermark.updated_at else None,
            'processing_lag_days': lag_days,
            'days_since_update': watermark.days_since_last_update,
            'incremental_window_start': incremental_start.isoformat(),
            'incremental_window_end': incremental_end.isoformat(),
            'incremental_feasible': self.is_incremental_feasible(),
            'incremental_window_days': (incremental_end - incremental_start).days + 1
        }
    
    def reset_watermark(self) -> bool:
        """
        Reset (delete) watermark for the pipeline.
        
        Used for forcing full refresh or resetting after errors.
        
        Returns:
            True if watermark was deleted, False if none existed
            
        Raises:
            WatermarkError: If deletion fails
        """
        logger.warning(f"Resetting watermark for pipeline '{self.pipeline_name}'")
        
        try:
            with get_db_connection() as conn:
                with conn.transaction():
                    with conn.cursor() as cursor:
                        cursor.execute("""
                        DELETE FROM capture_insights.meta_refresh_watermarks
                        WHERE pipeline_name = %s
                        """, (self.pipeline_name,))
                        
                        deleted_rows = cursor.rowcount
                        
                        if deleted_rows > 0:
                            logger.info(f"Watermark reset successfully for '{self.pipeline_name}'")
                            return True
                        else:
                            logger.info(f"No watermark found to reset for '{self.pipeline_name}'")
                            return False
        
        except Exception as e:
            logger.error(f"Failed to reset watermark: {e}")
            raise WatermarkError(f"Watermark reset failed: {e}")
    
    @staticmethod
    def list_all_watermarks() -> List[WatermarkInfo]:
        """
        List all pipeline watermarks.
        
        Returns:
            List of WatermarkInfo objects for all pipelines
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT pipeline_name, last_modified_to, overlap_days, updated_at
                    FROM capture_insights.meta_refresh_watermarks
                    ORDER BY pipeline_name
                    """)
                    
                    return [
                        WatermarkInfo(
                            pipeline_name=row['pipeline_name'],
                            last_modified_to=row['last_modified_to'],
                            overlap_days=row['overlap_days'],
                            updated_at=row['updated_at']
                        )
                        for row in cursor.fetchall()
                    ]
        
        except Exception as e:
            logger.error(f"Failed to list watermarks: {e}")
            raise WatermarkError(f"Watermark listing failed: {e}")


# Convenience functions for common operations
def get_incremental_window(pipeline_name: Optional[str] = None, end_date: Optional[date] = None) -> Tuple[date, date]:
    """
    Get incremental processing window for a pipeline.
    
    Args:
        pipeline_name: Pipeline name (defaults to config)
        end_date: End date (defaults to today)
        
    Returns:
        Tuple of (start_date, end_date)
    """
    manager = WatermarkManager(pipeline_name)
    return manager.get_incremental_window(end_date)


def advance_pipeline_watermark(new_timestamp: datetime, pipeline_name: Optional[str] = None) -> WatermarkInfo:
    """
    Advance watermark for a pipeline.
    
    Args:
        new_timestamp: New watermark timestamp
        pipeline_name: Pipeline name (defaults to config)
        
    Returns:
        Updated WatermarkInfo
    """
    manager = WatermarkManager(pipeline_name)
    return manager.advance_watermark(new_timestamp)