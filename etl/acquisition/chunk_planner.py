"""
Chunk planning for USASpending data acquisition.

This module handles date window planning for both historical and incremental ETL modes.
It creates efficient date chunks that respect API limits and overlap requirements.
"""

import logging
from typing import List, Tuple, Optional, Iterator
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from enum import Enum
import uuid

from ..config import get_config
from ..utils.logging import get_logger

logger = get_logger(__name__)


class ChunkMode(Enum):
    """Chunk planning modes."""
    HISTORICAL = "historical"
    INCREMENTAL = "incremental"


@dataclass
class DateChunk:
    """
    Represents a date window for API requests.
    
    Attributes:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)  
        chunk_id: Unique identifier for tracking
        mode: Processing mode (historical/incremental)
        overlap_days: Number of overlap days with previous chunk
        expected_size_hint: Rough estimate of expected records (for logging)
    """
    start_date: date
    end_date: date
    chunk_id: str
    mode: ChunkMode
    overlap_days: int = 0
    expected_size_hint: Optional[str] = None
    
    @property
    def days_span(self) -> int:
        """Number of days in this chunk (inclusive)."""
        return (self.end_date - self.start_date).days + 1
    
    @property
    def api_date_range(self) -> Tuple[str, str]:
        """Format dates for USASpending API (YYYY-MM-DD)."""
        return self.start_date.isoformat(), self.end_date.isoformat()
    
    def __str__(self) -> str:
        return f"DateChunk({self.start_date} to {self.end_date}, {self.days_span} days, mode={self.mode.value})"


class ChunkPlanValidationError(Exception):
    """Raised when chunk plan validation fails."""
    pass


class USASpendingChunkPlanner:
    """
    Plans date chunks for USASpending data acquisition.
    
    Features:
    - Historical mode: Process large date ranges in configurable chunks
    - Incremental mode: Process recent data with overlap to catch late updates
    - Intelligent overlap handling for data consistency
    - Configurable chunk sizes and overlap periods
    """
    
    def __init__(self, chunk_days: Optional[int] = None, current_days_lookback: Optional[int] = None):
        """
        Initialize chunk planner.
        
        Args:
            chunk_days: Days per chunk (defaults to config.chunk_days)
            current_days_lookback: Lookback for incremental mode (defaults to config.current_days_lookback)
        """
        config = get_config()
        self.chunk_days = chunk_days or config.chunk_days
        self.current_days_lookback = current_days_lookback or config.current_days_lookback
        
        logger.debug(f"Initialized ChunkPlanner: chunk_days={self.chunk_days}, lookback={self.current_days_lookback}")
    
    def plan_historical_chunks(
        self, 
        start_date: date, 
        end_date: date, 
        overlap_days: int = 1
    ) -> List[DateChunk]:
        """
        Plan chunks for historical data processing.
        
        Args:
            start_date: Start of historical period (inclusive)
            end_date: End of historical period (inclusive)
            overlap_days: Days to overlap between adjacent chunks
            
        Returns:
            List of DateChunk objects covering the full period
            
        Raises:
            ChunkPlanValidationError: If date range or parameters are invalid
        """
        # Validation
        if start_date > end_date:
            raise ChunkPlanValidationError(f"start_date ({start_date}) must be <= end_date ({end_date})")
        
        if overlap_days < 0 or overlap_days >= self.chunk_days:
            raise ChunkPlanValidationError(f"overlap_days ({overlap_days}) must be >= 0 and < chunk_days ({self.chunk_days})")
        
        total_days = (end_date - start_date).days + 1
        logger.info(f"Planning historical chunks: {start_date} to {end_date} ({total_days} days), {self.chunk_days}-day chunks, {overlap_days}-day overlap")
        
        chunks = []
        current_start = start_date
        chunk_number = 1
        
        while current_start <= end_date:
            # Calculate chunk end (don't exceed overall end_date)
            current_end = min(current_start + timedelta(days=self.chunk_days - 1), end_date)
            
            # Generate unique chunk ID
            chunk_id = f"hist_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{chunk_number:03d}_{uuid.uuid4().hex[:8]}"
            
            # Size hint for logging
            chunk_days_actual = (current_end - current_start).days + 1
            size_hint = f"~{chunk_days_actual} days"
            
            chunk = DateChunk(
                start_date=current_start,
                end_date=current_end,
                chunk_id=chunk_id,
                mode=ChunkMode.HISTORICAL,
                overlap_days=overlap_days if chunk_number > 1 else 0,
                expected_size_hint=size_hint
            )
            
            chunks.append(chunk)
            
            # Move to next chunk start (subtract overlap)
            next_start = current_end + timedelta(days=1) - timedelta(days=overlap_days)
            
            # Prevent infinite loop if overlap >= chunk size
            if next_start <= current_start:
                break
                
            current_start = next_start
            chunk_number += 1
        
        logger.info(f"Generated {len(chunks)} historical chunks covering {total_days} days")
        return chunks
    
    def plan_incremental_chunks(
        self, 
        base_date: Optional[date] = None,
        overlap_days: int = 2
    ) -> List[DateChunk]:
        """
        Plan chunks for incremental data processing.
        
        Incremental mode processes recent data with lookback to catch late updates.
        
        Args:
            base_date: Reference date (defaults to today)
            overlap_days: Days to overlap for catching late updates
            
        Returns:
            List of DateChunk objects for incremental processing
        """
        if base_date is None:
            base_date = date.today()
        
        # Calculate incremental window
        end_date = base_date
        start_date = base_date - timedelta(days=self.current_days_lookback - 1)
        
        logger.info(f"Planning incremental chunks: {start_date} to {end_date} ({self.current_days_lookback} days lookback)")
        
        # For incremental, typically use a single chunk unless the window is very large
        total_days = (end_date - start_date).days + 1
        
        if total_days <= self.chunk_days:
            # Single chunk for small incremental windows
            chunk_id = f"incr_{base_date.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"
            
            chunk = DateChunk(
                start_date=start_date,
                end_date=end_date,
                chunk_id=chunk_id,
                mode=ChunkMode.INCREMENTAL,
                overlap_days=overlap_days,
                expected_size_hint=f"~{total_days} days incremental"
            )
            
            chunks = [chunk]
        else:
            # Break large incremental windows into chunks
            chunks = self.plan_historical_chunks(start_date, end_date, overlap_days)
            # Update mode to incremental
            for chunk in chunks:
                chunk.mode = ChunkMode.INCREMENTAL
                chunk.chunk_id = chunk.chunk_id.replace("hist_", "incr_")
        
        logger.info(f"Generated {len(chunks)} incremental chunks")
        return chunks
    
    def plan_smart_chunks(
        self, 
        start_date: date, 
        end_date: date,
        mode: ChunkMode = ChunkMode.HISTORICAL
    ) -> List[DateChunk]:
        """
        Intelligently plan chunks based on date range size and mode.
        
        Args:
            start_date: Start date
            end_date: End date  
            mode: Processing mode
            
        Returns:
            List of optimally-sized DateChunk objects
        """
        total_days = (end_date - start_date).days + 1
        
        if mode == ChunkMode.INCREMENTAL or total_days <= self.chunk_days:
            # Use incremental logic for small ranges
            return self.plan_incremental_chunks(base_date=end_date)
        else:
            # Use historical chunking for large ranges
            # Adjust overlap based on range size (more overlap for larger ranges)
            overlap_days = min(3, max(1, total_days // 100))
            return self.plan_historical_chunks(start_date, end_date, overlap_days)
    
    def get_chunk_summary(self, chunks: List[DateChunk]) -> dict:
        """
        Generate summary statistics for a chunk plan.
        
        Args:
            chunks: List of DateChunk objects
            
        Returns:
            Dictionary with summary statistics
        """
        if not chunks:
            return {"total_chunks": 0, "total_days": 0, "date_range": None}
        
        min_date = min(chunk.start_date for chunk in chunks)
        max_date = max(chunk.end_date for chunk in chunks)
        total_days = (max_date - min_date).days + 1
        
        # Calculate effective coverage (accounting for overlaps)
        unique_days = set()
        for chunk in chunks:
            current_date = chunk.start_date
            while current_date <= chunk.end_date:
                unique_days.add(current_date)
                current_date += timedelta(days=1)
        
        return {
            "total_chunks": len(chunks),
            "total_days": total_days,
            "effective_days": len(unique_days),
            "date_range": f"{min_date} to {max_date}",
            "avg_chunk_size": sum(chunk.days_span for chunk in chunks) / len(chunks),
            "modes": list(set(chunk.mode.value for chunk in chunks)),
            "total_overlap_days": sum(chunk.overlap_days for chunk in chunks)
        }
    
    def validate_chunk_plan(self, chunks: List[DateChunk]) -> None:
        """
        Validate a chunk plan for consistency and completeness.
        
        Args:
            chunks: List of DateChunk objects to validate
            
        Raises:
            ChunkPlanValidationError: If plan is invalid
        """
        if not chunks:
            raise ChunkPlanValidationError("Chunk plan is empty")
        
        # Sort chunks by start date for validation
        sorted_chunks = sorted(chunks, key=lambda c: c.start_date)
        
        # Check for gaps or excessive overlaps
        for i in range(1, len(sorted_chunks)):
            prev_chunk = sorted_chunks[i - 1]
            curr_chunk = sorted_chunks[i]
            
            # Gap detection
            expected_start = prev_chunk.end_date + timedelta(days=1) - timedelta(days=curr_chunk.overlap_days)
            if curr_chunk.start_date > expected_start:
                gap_days = (curr_chunk.start_date - expected_start).days
                raise ChunkPlanValidationError(f"Gap of {gap_days} days between chunks {i-1} and {i}")
            
            # Excessive overlap detection
            if curr_chunk.start_date < prev_chunk.start_date:
                raise ChunkPlanValidationError(f"Chunk {i} starts before previous chunk {i-1}")
        
        logger.debug(f"Validated chunk plan: {len(chunks)} chunks covering {self.get_chunk_summary(chunks)['date_range']}")
    
    @staticmethod
    def chunk_iterator(chunks: List[DateChunk]) -> Iterator[DateChunk]:
        """
        Iterate through chunks in chronological order.
        
        Args:
            chunks: List of DateChunk objects
            
        Yields:
            DateChunk objects in start_date order
        """
        for chunk in sorted(chunks, key=lambda c: c.start_date):
            yield chunk