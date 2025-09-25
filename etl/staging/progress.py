"""
Progress tracking for ETL chunk processing.

This module manages the meta_chunk_progress table to track the status and progress
of individual chunk processing operations throughout the ETL pipeline.
"""

import logging
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime, date
from enum import Enum
from dataclasses import dataclass
import psycopg
from psycopg.rows import dict_row

from ..config import get_config
from ..utils.logging import get_logger
from ..acquisition.chunk_planner import DateChunk
from ..staging.raw_loader import get_db_connection

logger = get_logger(__name__)


class ChunkStatus(Enum):
    """Chunk processing status values."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    SUCCESS = "success"
    FAILED = "failed"


class ProgressRecorderError(Exception):
    """Raised when progress recording operations fail."""
    pass


@dataclass
class ChunkProgressRecord:
    """
    Represents a chunk progress record in the database.
    
    Attributes:
        id: Database record ID (None for new records)
        pipeline_name: Name of the ETL pipeline
        window_start: Start date of the chunk window
        window_end: End date of the chunk window  
        chunk_index: Index of chunk within the processing batch
        job_id: UUID identifying the overall job/batch
        correlation_id: UUID for this specific chunk
        status: Current processing status
        rows_staged: Number of rows staged to raw tables
        rows_deduped: Number of rows after deduplication
        archive_path_rel: Relative path to archive file
        archive_sha256: SHA256 hash of archive
        error_class: Exception class name (if failed)
        error_message: Error message (if failed)
        created_at: Record creation timestamp
        updated_at: Last update timestamp
    """
    pipeline_name: str
    window_start: date
    window_end: date
    chunk_index: int
    job_id: uuid.UUID
    correlation_id: uuid.UUID
    status: ChunkStatus
    rows_staged: Optional[int] = None
    rows_deduped: Optional[int] = None
    archive_path_rel: Optional[str] = None
    archive_sha256: Optional[str] = None
    error_class: Optional[str] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    id: Optional[int] = None
    
    @classmethod
    def from_chunk(
        cls, 
        chunk: DateChunk, 
        job_id: uuid.UUID, 
        chunk_index: int,
        pipeline_name: Optional[str] = None
    ) -> 'ChunkProgressRecord':
        """
        Create progress record from DateChunk.
        
        Args:
            chunk: DateChunk object
            job_id: UUID for the overall processing job
            chunk_index: Index of this chunk in the batch
            pipeline_name: Override pipeline name (defaults to config)
            
        Returns:
            ChunkProgressRecord initialized from chunk data
        """
        config = get_config()
        
        # Extract correlation_id from chunk_id if possible
        correlation_id = uuid.uuid4()
        if '_' in chunk.chunk_id:
            try:
                correlation_id = uuid.UUID(chunk.chunk_id.split('_')[-1])
            except ValueError:
                pass  # Use generated UUID
        
        return cls(
            pipeline_name=pipeline_name or config.pipeline_name,
            window_start=chunk.start_date,
            window_end=chunk.end_date,
            chunk_index=chunk_index,
            job_id=job_id,
            correlation_id=correlation_id,
            status=ChunkStatus.PENDING
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            'pipeline_name': self.pipeline_name,
            'window_start': self.window_start,
            'window_end': self.window_end,
            'chunk_index': self.chunk_index,
            'job_id': str(self.job_id),
            'correlation_id': str(self.correlation_id),
            'status': self.status.value,
            'rows_staged': self.rows_staged,
            'rows_deduped': self.rows_deduped,
            'archive_path_rel': self.archive_path_rel,
            'archive_sha256': self.archive_sha256,
            'error_class': self.error_class,
            'error_message': self.error_message
        }


class ProgressRecorder:
    """
    Manages chunk progress tracking in the meta_chunk_progress table.
    
    Features:
    - Initialize progress records for new chunks
    - Update status transitions (pending -> in_progress -> success/failed)
    - Track row counts and processing statistics
    - Record error information for failed chunks
    - Query progress status for monitoring and recovery
    """
    
    def __init__(self, pipeline_name: Optional[str] = None):
        """
        Initialize progress recorder.
        
        Args:
            pipeline_name: Override pipeline name (defaults to config)
        """
        config = get_config()
        self.pipeline_name = pipeline_name or config.pipeline_name
        
        logger.debug(f"Initialized ProgressRecorder for pipeline '{self.pipeline_name}'")
    
    def initialize_batch(self, chunks: List[DateChunk], job_id: Optional[uuid.UUID] = None) -> uuid.UUID:
        """
        Initialize progress records for a batch of chunks.
        
        Args:
            chunks: List of DateChunk objects to initialize
            job_id: Optional job UUID (generates new if None)
            
        Returns:
            UUID of the job/batch
            
        Raises:
            ProgressRecorderError: If initialization fails
        """
        if job_id is None:
            job_id = uuid.uuid4()
        
        logger.info(f"Initializing progress tracking for {len(chunks)} chunks (job {job_id})")
        
        try:
            with get_db_connection() as conn:
                with conn.transaction():
                    for i, chunk in enumerate(chunks):
                        record = ChunkProgressRecord.from_chunk(
                            chunk, job_id, i, self.pipeline_name
                        )
                        self._insert_progress_record(conn, record)
            
            logger.info(f"Successfully initialized progress for {len(chunks)} chunks")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to initialize batch progress: {e}")
            raise ProgressRecorderError(f"Batch initialization failed: {e}")
    
    def update_chunk_status(
        self, 
        correlation_id: uuid.UUID, 
        status: ChunkStatus,
        rows_staged: Optional[int] = None,
        rows_deduped: Optional[int] = None,
        archive_path_rel: Optional[str] = None,
        archive_sha256: Optional[str] = None,
        error_class: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Update chunk processing status and metadata.
        
        Args:
            correlation_id: UUID of the chunk to update
            status: New processing status
            rows_staged: Number of rows staged (optional)
            rows_deduped: Number of rows after deduplication (optional)
            archive_path_rel: Relative archive path (optional)
            archive_sha256: Archive SHA256 hash (optional)
            error_class: Exception class name for failures (optional)
            error_message: Error message for failures (optional)
            
        Returns:
            True if record was updated, False if not found
            
        Raises:
            ProgressRecorderError: If update operation fails
        """
        logger.debug(f"Updating chunk {correlation_id} status to {status.value}")
        
        # Build update fields dynamically
        update_fields = {'status': status.value, 'updated_at': datetime.now()}
        
        if rows_staged is not None:
            update_fields['rows_staged'] = rows_staged
        if rows_deduped is not None:
            update_fields['rows_deduped'] = rows_deduped
        if archive_path_rel is not None:
            update_fields['archive_path_rel'] = archive_path_rel
        if archive_sha256 is not None:
            update_fields['archive_sha256'] = archive_sha256
        if error_class is not None:
            update_fields['error_class'] = error_class
        if error_message is not None:
            update_fields['error_message'] = error_message
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Build dynamic UPDATE query
                    set_clauses = [f"{field} = %({field})s" for field in update_fields.keys()]
                    update_sql = f"""
                    UPDATE capture_insights.meta_chunk_progress 
                    SET {', '.join(set_clauses)}
                    WHERE correlation_id = %(correlation_id)s 
                    AND pipeline_name = %(pipeline_name)s
                    """
                    
                    update_params = {
                        **update_fields,
                        'correlation_id': str(correlation_id),
                        'pipeline_name': self.pipeline_name
                    }
                    
                    cursor.execute(update_sql, update_params)
                    updated_rows = cursor.rowcount
                    
                    if updated_rows > 0:
                        logger.debug(f"Updated chunk {correlation_id} status to {status.value}")
                        return True
                    else:
                        logger.warning(f"No progress record found for chunk {correlation_id}")
                        return False
        
        except Exception as e:
            logger.error(f"Failed to update chunk status: {e}")
            raise ProgressRecorderError(f"Status update failed: {e}")
    
    def mark_chunk_in_progress(self, correlation_id: uuid.UUID) -> bool:
        """Mark chunk as in progress."""
        return self.update_chunk_status(correlation_id, ChunkStatus.IN_PROGRESS)
    
    def mark_chunk_success(
        self, 
        correlation_id: uuid.UUID, 
        rows_staged: Optional[int] = None,
        rows_deduped: Optional[int] = None,
        archive_path_rel: Optional[str] = None,
        archive_sha256: Optional[str] = None
    ) -> bool:
        """Mark chunk as successfully completed."""
        return self.update_chunk_status(
            correlation_id, 
            ChunkStatus.SUCCESS,
            rows_staged=rows_staged,
            rows_deduped=rows_deduped,
            archive_path_rel=archive_path_rel,
            archive_sha256=archive_sha256
        )
    
    def mark_chunk_failed(
        self, 
        correlation_id: uuid.UUID, 
        error: Exception,
        rows_staged: Optional[int] = None
    ) -> bool:
        """Mark chunk as failed with error information."""
        return self.update_chunk_status(
            correlation_id,
            ChunkStatus.FAILED,
            rows_staged=rows_staged,
            error_class=error.__class__.__name__,
            error_message=str(error)
        )
    
    def get_chunk_progress(self, correlation_id: uuid.UUID) -> Optional[ChunkProgressRecord]:
        """
        Get progress record for a specific chunk.
        
        Args:
            correlation_id: UUID of the chunk
            
        Returns:
            ChunkProgressRecord if found, None otherwise
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT * FROM capture_insights.meta_chunk_progress 
                    WHERE correlation_id = %s AND pipeline_name = %s
                    """, (str(correlation_id), self.pipeline_name))
                    
                    row = cursor.fetchone()
                    if row:
                        return self._row_to_record(row)
                    return None
        
        except Exception as e:
            logger.error(f"Failed to get chunk progress: {e}")
            raise ProgressRecorderError(f"Progress query failed: {e}")
    
    def get_job_progress(self, job_id: uuid.UUID) -> List[ChunkProgressRecord]:
        """
        Get progress records for all chunks in a job.
        
        Args:
            job_id: UUID of the job
            
        Returns:
            List of ChunkProgressRecord objects
        """
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT * FROM capture_insights.meta_chunk_progress 
                    WHERE job_id = %s AND pipeline_name = %s
                    ORDER BY chunk_index
                    """, (str(job_id), self.pipeline_name))
                    
                    return [self._row_to_record(row) for row in cursor.fetchall()]
        
        except Exception as e:
            logger.error(f"Failed to get job progress: {e}")
            raise ProgressRecorderError(f"Job progress query failed: {e}")
    
    def get_job_summary(self, job_id: uuid.UUID) -> Dict[str, Any]:
        """
        Get summary statistics for a job.
        
        Args:
            job_id: UUID of the job
            
        Returns:
            Dictionary with job summary statistics
        """
        records = self.get_job_progress(job_id)
        
        if not records:
            return {'job_id': str(job_id), 'total_chunks': 0, 'status_counts': {}}
        
        status_counts = {}
        total_rows_staged = 0
        total_rows_deduped = 0
        
        for record in records:
            status = record.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
            
            if record.rows_staged:
                total_rows_staged += record.rows_staged
            if record.rows_deduped:
                total_rows_deduped += record.rows_deduped
        
        return {
            'job_id': str(job_id),
            'pipeline_name': self.pipeline_name,
            'total_chunks': len(records),
            'status_counts': status_counts,
            'total_rows_staged': total_rows_staged,
            'total_rows_deduped': total_rows_deduped,
            'completion_rate': status_counts.get('success', 0) / len(records) * 100,
            'failure_rate': status_counts.get('failed', 0) / len(records) * 100
        }
    
    def _insert_progress_record(self, conn: psycopg.Connection, record: ChunkProgressRecord) -> None:
        """Insert a new progress record."""
        with conn.cursor() as cursor:
            insert_sql = """
            INSERT INTO capture_insights.meta_chunk_progress 
            (pipeline_name, window_start, window_end, chunk_index, job_id, correlation_id, status)
            VALUES (%(pipeline_name)s, %(window_start)s, %(window_end)s, 
                   %(chunk_index)s, %(job_id)s, %(correlation_id)s, %(status)s)
            """
            
            cursor.execute(insert_sql, record.to_dict())
    
    def _row_to_record(self, row: Dict[str, Any]) -> ChunkProgressRecord:
        """Convert database row to ChunkProgressRecord."""
        return ChunkProgressRecord(
            id=row['id'],
            pipeline_name=row['pipeline_name'],
            window_start=row['window_start'],
            window_end=row['window_end'],
            chunk_index=row['chunk_index'],
            job_id=uuid.UUID(row['job_id']),
            correlation_id=uuid.UUID(row['correlation_id']),
            status=ChunkStatus(row['status']),
            rows_staged=row.get('rows_staged'),
            rows_deduped=row.get('rows_deduped'),
            archive_path_rel=row.get('archive_path_rel'),
            archive_sha256=row.get('archive_sha256'),
            error_class=row.get('error_class'),
            error_message=row.get('error_message'),
            created_at=row.get('created_at'),
            updated_at=row.get('updated_at')
        )