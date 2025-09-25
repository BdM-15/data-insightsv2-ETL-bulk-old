"""Archive manager for USASpending ETL pipeline.

This module handles:
1. Path planning for archive files and metadata sidecars
2. ZIP file integrity verification
3. Metadata sidecar JSON creation and validation
4. Archive retention and cleanup
5. Disk space management integration

The archive manager works closely with the API client to manage downloaded
files and create comprehensive metadata for tracking and auditing.

Usage:
    manager = ArchiveManager(config)
    archive_info = manager.create_archive_metadata(
        job_result=api_result,
        chunk_info=chunk_data,
        csv_validation=header_info
    )
"""

import json
import platform
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
import shutil
import tempfile
import hashlib

from etl.config import ETLConfig
from etl.utils.logging import get_logger
from etl.utils.disk import get_free_space_gb
from etl.utils.hash import compute_file_sha256


class ArchiveError(Exception):
    """Base exception for archive-related errors."""
    pass


class MetadataValidationError(ArchiveError):
    """Raised when metadata fails schema validation."""
    pass


class IntegrityError(ArchiveError):
    """Raised when file integrity checks fail."""
    pass


class ArchiveManager:
    """Manages archive files and their metadata sidecars."""
    
    def __init__(self, config: ETLConfig):
        """Initialize the archive manager.
        
        Args:
            config: ETL configuration
        """
        self.config = config
        self.logger = get_logger(__name__)
        
        # Ensure download directory exists
        self.download_dir = Path(self.config.download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
    
    def create_archive_metadata(
        self,
        job_result: 'BulkJobResult',  # Forward reference to avoid circular imports
        chunk_info: Dict[str, Any],
        csv_validation: Dict[str, Any],
        timing: Dict[str, datetime],
        system_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create complete metadata sidecar for an archive.
        
        Args:
            job_result: Result from API client containing download info
            chunk_info: Chunk window and indexing information
            csv_validation: CSV header validation results
            timing: Timing information for various phases
            system_info: Optional system information override
            
        Returns:
            Complete metadata dictionary conforming to schema
            
        Raises:
            MetadataValidationError: If metadata fails schema validation
        """
        self.logger.debug(
            "Creating archive metadata",
            extra={
                "correlation_id": job_result.correlation_id,
                "job_id": job_result.job_id,
                "event": "metadata_create_start"
            }
        )
        
        # Gather system information
        if system_info is None:
            system_info = self._gather_system_info()
        
        # Build complete metadata structure
        metadata = {
            "schema_version": "1.0.0",
            "job_id": job_result.job_id,
            "correlation_id": job_result.correlation_id,
            
            "request": {
                "endpoint": "https://api.usaspending.gov/api/v2/bulk_download/awards/",
                "payload_hash": job_result.request_payload_hash,
                "date_from": chunk_info["window_start"],
                "date_to": chunk_info["window_end"],
                "date_type": chunk_info.get("date_type", "action_date"),
                "award_type_scope": chunk_info.get("award_type", "prime")
            },
            
            "response": {
                "status_url": job_result.status_url,
                "file_url": timing.get("download_url", ""),
                "http_status": 200,  # Successful completion assumed
                "received_bytes": job_result.zip_size_bytes
            },
            
            "file": {
                "archive_path_rel": str(job_result.download_path.relative_to(self.download_dir)),
                "archive_sha256": job_result.zip_sha256,
                "csv_expected_headers": csv_validation.get("expected_headers", []),
                "csv_header_present": csv_validation.get("header_present", False),
                "extracted_rows": csv_validation.get("row_count"),
                "extracted_csv_bytes": csv_validation.get("csv_size_bytes")
            },
            
            "timing": {
                "requested_at": timing.get("requested_at", datetime.now(timezone.utc)).isoformat(),
                "ready_at": timing.get("ready_at", datetime.now(timezone.utc)).isoformat(),
                "download_started_at": timing.get("download_started_at", datetime.now(timezone.utc)).isoformat(),
                "download_completed_at": timing.get("download_completed_at", datetime.now(timezone.utc)).isoformat()
            },
            
            "chunk": {
                "window_start": chunk_info["window_start"],
                "window_end": chunk_info["window_end"],
                "chunk_index": chunk_info.get("chunk_index", 0),
                "chunk_span_days": chunk_info.get("chunk_span_days", self.config.chunk_days)
            },
            
            "integrity": {
                "zip_size_bytes": job_result.zip_size_bytes,
                "checksum_verified": True  # Will be set after verification
            },
            
            "system": system_info,
            
            "fail_fast_triggered": False  # Will be set by fail-fast controller
        }
        
        # Validate metadata against schema (basic validation)
        self._validate_metadata_basic(metadata)
        
        self.logger.info(
            "Archive metadata created successfully",
            extra={
                "correlation_id": job_result.correlation_id,
                "job_id": job_result.job_id,
                "archive_path": str(job_result.download_path),
                "metadata_size_keys": len(metadata),
                "event": "metadata_created"
            }
        )
        
        return metadata
    
    def save_metadata_sidecar(
        self,
        metadata: Dict[str, Any],
        archive_path: Path,
        correlation_id: str
    ) -> Path:
        """Save metadata sidecar JSON file alongside archive.
        
        Args:
            metadata: Complete metadata dictionary
            archive_path: Path to the archive ZIP file
            correlation_id: Correlation ID for logging
            
        Returns:
            Path to the created sidecar file
            
        Raises:
            ArchiveError: If sidecar creation fails
        """
        # Generate sidecar filename: <archive_base>.metadata.json
        archive_base = archive_path.stem  # filename without .zip extension
        sidecar_path = archive_path.parent / f"{archive_base}.metadata.json"
        
        try:
            # Write metadata with pretty formatting for readability
            with open(sidecar_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, sort_keys=True)
                
        except OSError as e:
            raise ArchiveError(f"Failed to write metadata sidecar: {e}") from e
        
        self.logger.info(
            "Metadata sidecar saved",
            extra={
                "correlation_id": correlation_id,
                "archive_path": str(archive_path),
                "sidecar_path": str(sidecar_path),
                "sidecar_size_bytes": sidecar_path.stat().st_size,
                "event": "sidecar_saved"
            }
        )
        
        return sidecar_path
    
    def verify_archive_integrity(
        self,
        archive_path: Path,
        expected_sha256: str,
        correlation_id: str
    ) -> bool:
        """Verify archive file integrity using SHA256 checksum.
        
        Args:
            archive_path: Path to the archive file
            expected_sha256: Expected SHA256 hash (hex string)
            correlation_id: Correlation ID for logging
            
        Returns:
            True if integrity verified, False otherwise
            
        Raises:
            IntegrityError: If integrity check fails critically
        """
        self.logger.debug(
            "Starting integrity verification",
            extra={
                "correlation_id": correlation_id,
                "archive_path": str(archive_path),
                "expected_sha256": expected_sha256,
                "event": "integrity_check_start"
            }
        )
        
        if not archive_path.exists():
            raise IntegrityError(f"Archive file not found: {archive_path}")
        
        try:
            # Compute actual hash
            actual_sha256 = compute_file_sha256(archive_path)
            
            # Compare hashes
            integrity_ok = actual_sha256.lower() == expected_sha256.lower()
            
            if integrity_ok:
                self.logger.info(
                    "Archive integrity verified successfully",
                    extra={
                        "correlation_id": correlation_id,
                        "archive_path": str(archive_path),
                        "sha256": actual_sha256,
                        "event": "integrity_verified"
                    }
                )
            else:
                self.logger.error(
                    "Archive integrity check FAILED",
                    extra={
                        "correlation_id": correlation_id,
                        "archive_path": str(archive_path),
                        "expected_sha256": expected_sha256,
                        "actual_sha256": actual_sha256,
                        "event": "integrity_failed"
                    }
                )
                
            return integrity_ok
            
        except Exception as e:
            raise IntegrityError(f"Integrity verification failed: {e}") from e
    
    def extract_csv_preview(
        self,
        archive_path: Path,
        correlation_id: str,
        max_preview_rows: int = 10
    ) -> Dict[str, Any]:
        """Extract CSV preview from ZIP archive for validation.
        
        Args:
            archive_path: Path to the ZIP archive
            correlation_id: Correlation ID for logging
            max_preview_rows: Maximum rows to extract for preview
            
        Returns:
            Dictionary with CSV info: headers, row_count, preview_rows, csv_size_bytes
            
        Raises:
            ArchiveError: If extraction fails
        """
        self.logger.debug(
            "Extracting CSV preview",
            extra={
                "correlation_id": correlation_id,
                "archive_path": str(archive_path),
                "max_preview_rows": max_preview_rows,
                "event": "csv_preview_start"
            }
        )
        
        try:
            with zipfile.ZipFile(archive_path, 'r') as zf:
                # Find CSV file (should be only one or the largest)
                csv_files = [name for name in zf.namelist() if name.endswith('.csv')]
                
                if not csv_files:
                    raise ArchiveError(f"No CSV files found in archive: {archive_path}")
                
                # Use first CSV file (or could pick largest)
                csv_filename = csv_files[0]
                
                self.logger.debug(
                    "Found CSV file in archive",
                    extra={
                        "correlation_id": correlation_id,
                        "csv_filename": csv_filename,
                        "total_csv_files": len(csv_files),
                        "event": "csv_found"
                    }
                )
                
                # Get CSV file info
                csv_info = zf.getinfo(csv_filename)
                csv_size_bytes = csv_info.file_size
                
                # Extract and read CSV header + preview rows
                with zf.open(csv_filename, 'r') as csv_file:
                    # Read as text (assuming UTF-8)
                    lines = []
                    for i, line_bytes in enumerate(csv_file):
                        if i >= max_preview_rows + 1:  # +1 for header
                            break
                        try:
                            line = line_bytes.decode('utf-8').strip()
                            lines.append(line)
                        except UnicodeDecodeError:
                            # Try latin-1 fallback
                            line = line_bytes.decode('latin-1').strip()
                            lines.append(line)
                
                # Parse header and preview
                headers = []
                preview_rows = []
                
                if lines:
                    # First line is header
                    header_line = lines[0]
                    headers = [col.strip().strip('"') for col in header_line.split(',')]
                    
                    # Rest are data rows
                    preview_rows = lines[1:]
                
                # Count total rows (approximate - would need full extraction for exact count)
                # For now, we'll set this as None and let the staging loader count precisely
                estimated_rows = None
                
                result = {
                    "csv_filename": csv_filename,
                    "headers": headers,
                    "header_count": len(headers),
                    "row_count": estimated_rows,  # Will be counted during staging
                    "preview_rows": preview_rows,
                    "csv_size_bytes": csv_size_bytes,
                    "encoding_detected": "utf-8"  # Simplified assumption
                }
                
                self.logger.info(
                    "CSV preview extracted successfully",
                    extra={
                        "correlation_id": correlation_id,
                        "csv_filename": csv_filename,
                        "header_count": len(headers),
                        "preview_row_count": len(preview_rows),
                        "csv_size_bytes": csv_size_bytes,
                        "event": "csv_preview_complete"
                    }
                )
                
                return result
                
        except zipfile.BadZipFile as e:
            raise ArchiveError(f"Invalid ZIP file: {e}") from e
        except Exception as e:
            raise ArchiveError(f"CSV preview extraction failed: {e}") from e
    
    def plan_archive_path(
        self,
        job_id: str,
        correlation_id: str,
        award_type: str = "prime",
        date_from: str = None,
        date_to: str = None
    ) -> Path:
        """Plan archive file path with organized structure.
        
        Args:
            job_id: Unique job identifier
            correlation_id: Correlation ID for logging
            award_type: Type of awards (prime/sub)
            date_from: Start date for organizational structure
            date_to: End date for organizational structure
            
        Returns:
            Planned path for the archive file
        """
        # Organize by award type and optionally by date
        if date_from and date_to:
            # Extract year-month for organization
            year_month = date_from[:7]  # YYYY-MM from YYYY-MM-DD
            subdir = self.download_dir / award_type / year_month
        else:
            subdir = self.download_dir / award_type
            
        subdir.mkdir(parents=True, exist_ok=True)
        
        # Use job_id as filename, ensure .zip extension
        filename = job_id if job_id.endswith('.zip') else f"{job_id}.zip"
        planned_path = subdir / filename
        
        self.logger.debug(
            "Archive path planned",
            extra={
                "correlation_id": correlation_id,
                "job_id": job_id,
                "planned_path": str(planned_path),
                "award_type": award_type,
                "event": "path_planned"
            }
        )
        
        return planned_path
    
    def cleanup_old_archives(
        self,
        retention_days: Optional[int] = None,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Clean up old archive files based on retention policy.
        
        Args:
            retention_days: Days to retain (uses config default if None)
            dry_run: If True, only report what would be deleted
            
        Returns:
            Cleanup summary with counts and freed space
        """
        if retention_days is None:
            retention_days = self.config.archive_retention_days
            
        self.logger.info(
            "Starting archive cleanup",
            extra={
                "retention_days": retention_days,
                "dry_run": dry_run,
                "download_dir": str(self.download_dir),
                "event": "cleanup_start"
            }
        )
        
        # Find old files
        cutoff_time = datetime.now().timestamp() - (retention_days * 24 * 3600)
        
        files_to_remove = []
        total_size_to_free = 0
        
        # Scan for ZIP and metadata files
        for file_path in self.download_dir.rglob("*"):
            if file_path.is_file() and file_path.suffix in ('.zip', '.json'):
                if file_path.stat().st_mtime < cutoff_time:
                    files_to_remove.append(file_path)
                    total_size_to_free += file_path.stat().st_size
        
        # Perform cleanup if not dry run
        files_removed = 0
        actual_size_freed = 0
        
        if not dry_run:
            for file_path in files_to_remove:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    files_removed += 1
                    actual_size_freed += file_size
                    
                    self.logger.debug(
                        "Archive file removed",
                        extra={
                            "file_path": str(file_path),
                            "file_size_bytes": file_size,
                            "event": "file_removed"
                        }
                    )
                    
                except OSError as e:
                    self.logger.warning(
                        "Failed to remove archive file",
                        extra={
                            "file_path": str(file_path),
                            "error": str(e),
                            "event": "remove_failed"
                        }
                    )
        
        summary = {
            "retention_days": retention_days,
            "files_found": len(files_to_remove),
            "files_removed": files_removed,
            "size_to_free_bytes": total_size_to_free,
            "size_freed_bytes": actual_size_freed,
            "size_freed_gb": round(actual_size_freed / (1024**3), 2),
            "dry_run": dry_run
        }
        
        self.logger.info(
            "Archive cleanup completed",
            extra={
                **summary,
                "event": "cleanup_complete"
            }
        )
        
        return summary
    
    def _gather_system_info(self) -> Dict[str, Any]:
        """Gather system information for metadata."""
        free_gb_pre = get_free_space_gb(self.download_dir)
        
        return {
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "platform": f"{platform.system()} {platform.release()}",
            "free_gb_pre": round(free_gb_pre, 2),
            "free_gb_post": None  # Will be updated after processing
        }
    
    def _validate_metadata_basic(self, metadata: Dict[str, Any]) -> None:
        """Perform basic metadata validation.
        
        Args:
            metadata: Metadata dictionary to validate
            
        Raises:
            MetadataValidationError: If validation fails
        """
        required_keys = [
            "schema_version", "job_id", "correlation_id",
            "request", "response", "file", "timing", 
            "chunk", "integrity", "system", "fail_fast_triggered"
        ]
        
        missing_keys = [key for key in required_keys if key not in metadata]
        if missing_keys:
            raise MetadataValidationError(f"Missing required keys: {missing_keys}")
        
        # Validate UUID format (basic check)
        import uuid
        try:
            uuid.UUID(metadata["job_id"])
            uuid.UUID(metadata["correlation_id"])
        except ValueError as e:
            raise MetadataValidationError(f"Invalid UUID format: {e}")
        
        # Validate SHA256 format
        sha256_pattern = r'^[a-f0-9]{64}$'
        import re
        if not re.match(sha256_pattern, metadata["file"]["archive_sha256"]):
            raise MetadataValidationError("Invalid SHA256 format in file.archive_sha256")


# Utility functions for common operations

def create_complete_archive_record(
    job_result: 'BulkJobResult',
    chunk_info: Dict[str, Any],
    csv_validation: Dict[str, Any],
    timing: Dict[str, datetime],
    config: ETLConfig,
    correlation_id: str
) -> tuple[Path, Dict[str, Any]]:
    """Complete archive processing: create metadata and save sidecar.
    
    This is a convenience function that combines metadata creation,
    integrity verification, and sidecar saving.
    
    Args:
        job_result: API client result
        chunk_info: Chunk information
        csv_validation: CSV validation results
        timing: Timing information
        config: ETL configuration
        correlation_id: Correlation ID
        
    Returns:
        Tuple of (sidecar_path, metadata_dict)
    """
    manager = ArchiveManager(config)
    
    # Create metadata
    metadata = manager.create_archive_metadata(
        job_result=job_result,
        chunk_info=chunk_info,
        csv_validation=csv_validation,
        timing=timing
    )
    
    # Verify integrity
    integrity_ok = manager.verify_archive_integrity(
        archive_path=job_result.download_path,
        expected_sha256=job_result.zip_sha256,
        correlation_id=correlation_id
    )
    
    # Update metadata with verification result
    metadata["integrity"]["checksum_verified"] = integrity_ok
    
    # Save sidecar
    sidecar_path = manager.save_metadata_sidecar(
        metadata=metadata,
        archive_path=job_result.download_path,
        correlation_id=correlation_id
    )
    
    return sidecar_path, metadata