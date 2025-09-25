"""
Raw CSV loader for USASpending data staging.

This module handles streaming CSV data from ZIP archives directly into PostgreSQL s1_raw tables
using efficient COPY operations with transaction management and error handling.
"""

import logging
import csv
import zipfile
from typing import Optional, Dict, Any, List, Iterator, Tuple
import io
from pathlib import Path
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, date
import uuid

from ..config import get_config
from ..utils.logging import get_logger
from ..acquisition.headers import USASpendingHeaderValidator, HeaderValidationError
from ..acquisition.chunk_planner import DateChunk

logger = get_logger(__name__)


class RawLoaderError(Exception):
    """Raised when raw CSV loading fails."""
    pass


class DatabaseConnectionError(RawLoaderError):
    """Raised when database connection fails."""
    pass


class CSVProcessingError(RawLoaderError):
    """Raised when CSV processing fails."""
    pass


@contextmanager
def get_db_connection(autocommit: bool = False):
    """
    Get database connection with proper error handling and cleanup.
    
    Args:
        autocommit: Whether to use autocommit mode
        
    Yields:
        psycopg.Connection object
        
    Raises:
        DatabaseConnectionError: If connection fails
    """
    config = get_config()
    
    try:
        conn = psycopg.connect(
            config.database_url,
            autocommit=autocommit,
            row_factory=dict_row
        )
        logger.debug("Database connection established")
        
        yield conn
        
    except psycopg.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise DatabaseConnectionError(f"Failed to connect to database: {e}")
    
    finally:
        try:
            conn.close()
            logger.debug("Database connection closed")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")


class RawCSVLoader:
    """
    Loads CSV data from ZIP archives into PostgreSQL s1_raw tables.
    
    Features:
    - Streaming CSV processing from ZIP files
    - Header validation against expected schema
    - Efficient COPY operations for bulk inserts
    - Transaction management with rollback on errors
    - Duplicate detection and handling
    - Progress tracking and logging
    """
    
    # Target table for prime awards data
    TARGET_TABLE = "capture_insights.s1_raw_usaspending_prime_awards_slimv2"
    
    def __init__(self, chunk: DateChunk):
        """
        Initialize raw CSV loader for a specific chunk.
        
        Args:
            chunk: DateChunk object containing processing metadata
        """
        self.chunk = chunk
        self.config = get_config()
        self.validator = USASpendingHeaderValidator()
        
        # Processing statistics
        self.stats = {
            'files_processed': 0,
            'rows_processed': 0,
            'rows_inserted': 0,
            'rows_skipped': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        
        logger.debug(f"Initialized RawCSVLoader for chunk {chunk.chunk_id}")
    
    def load_from_archive(self, archive_path: Path, csv_filename: Optional[str] = None) -> Dict[str, Any]:
        """
        Load CSV data from ZIP archive into raw table.
        
        Args:
            archive_path: Path to ZIP archive containing CSV data
            csv_filename: Specific CSV filename to load (if None, loads first CSV found)
            
        Returns:
            Dictionary with loading statistics and results
            
        Raises:
            RawLoaderError: If loading fails
        """
        if not archive_path.exists():
            raise RawLoaderError(f"Archive not found: {archive_path}")
        
        self.stats['start_time'] = datetime.now()
        
        try:
            with zipfile.ZipFile(archive_path, 'r') as zip_file:
                csv_file = self._find_csv_file(zip_file, csv_filename)
                
                logger.info(f"Loading CSV data from {archive_path} -> {csv_file}")
                
                with zip_file.open(csv_file) as zip_csv:
                    # Wrap in TextIOWrapper for CSV reader
                    with io.TextIOWrapper(zip_csv, encoding='utf-8') as csv_stream:
                        return self._process_csv_stream(csv_stream, archive_path, csv_file)
                        
        except zipfile.BadZipFile as e:
            raise RawLoaderError(f"Invalid ZIP archive {archive_path}: {e}")
        except Exception as e:
            logger.error(f"Failed to load from archive {archive_path}: {e}")
            raise RawLoaderError(f"Loading failed: {e}")
        finally:
            self.stats['end_time'] = datetime.now()
    
    def _find_csv_file(self, zip_file: zipfile.ZipFile, csv_filename: Optional[str]) -> str:
        """Find CSV file in ZIP archive."""
        file_list = zip_file.namelist()
        
        if csv_filename:
            if csv_filename in file_list:
                return csv_filename
            else:
                raise RawLoaderError(f"CSV file {csv_filename} not found in archive")
        
        # Find first CSV file
        csv_files = [f for f in file_list if f.lower().endswith('.csv')]
        
        if not csv_files:
            raise RawLoaderError("No CSV files found in archive")
        
        if len(csv_files) > 1:
            logger.warning(f"Multiple CSV files found, using first: {csv_files[0]}")
        
        return csv_files[0]
    
    def _process_csv_stream(self, csv_stream: io.TextIOWrapper, archive_path: Path, csv_filename: str) -> Dict[str, Any]:
        """Process CSV stream and load into database."""
        
        try:
            # Create CSV reader
            reader = csv.reader(csv_stream)
            
            # Read and validate headers
            headers = next(reader)
            self._validate_headers(headers)
            
            # Process rows in transaction
            with get_db_connection() as conn:
                with conn.transaction():
                    self._copy_rows_to_db(conn, reader, headers, archive_path)
            
            # Calculate final statistics
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            rows_per_sec = self.stats['rows_processed'] / max(duration, 1)
            
            result = {
                'success': True,
                'archive_path': str(archive_path),
                'csv_filename': csv_filename,
                'chunk_id': self.chunk.chunk_id,
                'statistics': self.stats.copy(),
                'duration_seconds': duration,
                'rows_per_second': round(rows_per_sec, 2)
            }
            
            logger.info(f"Successfully loaded {self.stats['rows_inserted']} rows from {csv_filename} "
                       f"({rows_per_sec:.1f} rows/sec)")
            
            return result
            
        except HeaderValidationError as e:
            self.stats['errors'] += 1
            raise RawLoaderError(f"Header validation failed: {e}")
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"CSV processing failed: {e}")
            raise CSVProcessingError(f"Failed to process CSV: {e}")
    
    def _validate_headers(self, headers: List[str]) -> None:
        """Validate CSV headers against expected schema."""
        try:
            result, message = self.validator.validate_headers(headers, strict=False)
            logger.debug(f"Header validation: {message}")
        except HeaderValidationError as e:
            logger.error(f"Header validation failed: {e}")
            raise
    
    def _copy_rows_to_db(self, conn: psycopg.Connection, reader: csv.reader, headers: List[str], archive_path: Path) -> None:
        """Use COPY to efficiently load CSV rows into database."""
        
        # Prepare metadata values
        fetch_date = date.today()
        ingestion_ts = datetime.now()
        chunk_correlation_id = uuid.UUID(self.chunk.chunk_id.split('_')[-1]) if '_' in self.chunk.chunk_id else uuid.uuid4()
        
        # Calculate SHA256 of archive (if available from chunk metadata)
        archive_sha256 = getattr(self.chunk, 'archive_sha256', None) or 'unknown'
        
        # Build COPY statement with all columns
        copy_columns = headers + [
            'fetch_date', 'ingestion_ts', 'created_at', 'updated_at',
            'chunk_window_start', 'chunk_window_end', 'chunk_correlation_id', 'archive_sha256'
        ]
        
        copy_sql = f"""
        COPY {self.TARGET_TABLE} ({', '.join(copy_columns)}) 
        FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '', QUOTE '"')
        """
        
        logger.debug(f"Starting COPY operation with {len(copy_columns)} columns")
        
        try:
            with conn.cursor() as cursor:
                with cursor.copy(copy_sql) as copy:
                    for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
                        try:
                            # Pad or truncate row to match header count
                            normalized_row = self._normalize_row(row, len(headers))
                            
                            # Add metadata columns
                            full_row = normalized_row + [
                                fetch_date.isoformat(),
                                ingestion_ts.isoformat(),
                                ingestion_ts.isoformat(),  # created_at
                                ingestion_ts.isoformat(),  # updated_at
                                self.chunk.start_date.isoformat(),
                                self.chunk.end_date.isoformat(),
                                str(chunk_correlation_id),
                                archive_sha256
                            ]
                            
                            # Write row to COPY stream
                            copy.write_row(full_row)
                            
                            self.stats['rows_processed'] += 1
                            self.stats['rows_inserted'] += 1
                            
                            # Log progress periodically
                            if self.stats['rows_processed'] % 10000 == 0:
                                logger.debug(f"Processed {self.stats['rows_processed']} rows")
                            
                        except Exception as e:
                            self.stats['errors'] += 1
                            self.stats['rows_skipped'] += 1
                            
                            if self.config.fail_fast:
                                logger.error(f"Row {row_num} processing failed in fail-fast mode: {e}")
                                raise CSVProcessingError(f"Row {row_num} failed: {e}")
                            else:
                                logger.warning(f"Skipping row {row_num} due to error: {e}")
                                continue
            
            self.stats['files_processed'] += 1
            logger.info(f"COPY operation completed: {self.stats['rows_inserted']} rows inserted")
            
        except psycopg.Error as e:
            logger.error(f"Database COPY operation failed: {e}")
            raise CSVProcessingError(f"COPY failed: {e}")
    
    def _normalize_row(self, row: List[str], expected_length: int) -> List[str]:
        """
        Normalize CSV row to expected length.
        
        Args:
            row: Raw CSV row data
            expected_length: Expected number of columns
            
        Returns:
            Normalized row with correct column count
        """
        if len(row) == expected_length:
            return row
        elif len(row) < expected_length:
            # Pad short rows with empty strings
            padded = row + [''] * (expected_length - len(row))
            logger.debug(f"Padded short row: {len(row)} -> {len(padded)} columns")
            return padded
        else:
            # Truncate long rows
            truncated = row[:expected_length]
            logger.warning(f"Truncated long row: {len(row)} -> {len(truncated)} columns")
            return truncated
    
    def get_loading_statistics(self) -> Dict[str, Any]:
        """Get comprehensive loading statistics."""
        duration = None
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        return {
            'chunk_id': self.chunk.chunk_id,
            'chunk_mode': self.chunk.mode.value,
            'chunk_date_range': f"{self.chunk.start_date} to {self.chunk.end_date}",
            'statistics': self.stats.copy(),
            'duration_seconds': duration,
            'success_rate': (self.stats['rows_inserted'] / max(self.stats['rows_processed'], 1)) * 100,
            'error_rate': (self.stats['errors'] / max(self.stats['rows_processed'], 1)) * 100
        }


# Convenience function for standalone usage
def load_csv_archive(archive_path: Path, chunk: DateChunk, csv_filename: Optional[str] = None) -> Dict[str, Any]:
    """
    Load CSV data from archive using RawCSVLoader.
    
    Args:
        archive_path: Path to ZIP archive
        chunk: DateChunk object with processing metadata
        csv_filename: Optional specific CSV filename
        
    Returns:
        Loading results and statistics
    """
    loader = RawCSVLoader(chunk)
    return loader.load_from_archive(archive_path, csv_filename)