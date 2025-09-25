"""USASpending API client for bulk download operations.

This module handles:
1. POST job submission to USASpending bulk download endpoint
2. Polling for job completion with exponential backoff
3. Streaming download of result ZIP files
4. Retry logic with tenacity for resilience
5. Correlation ID tracking for observability

Usage:
    client = USASpendingAPIClient(config)
    job_result = client.fetch_awards_bulk(
        date_from="2024-01-01",
        date_to="2024-01-07",
        date_type="action_date",
        award_type="prime"
    )
    # job_result contains download_path, metadata, and job info
"""

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union, Literal
import json
import hashlib

import requests
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential, 
    retry_if_exception_type,
    after_log
)

from etl.config import ETLConfig
from etl.utils.logging import get_logger


# Type aliases for clarity
DateType = Literal["action_date", "last_modified_date"] 
AwardType = Literal["prime", "sub"]
JobStatus = Literal["pending", "ready", "failed"]


@dataclass
class BulkJobRequest:
    """Represents a bulk download job request."""
    date_from: str  # YYYY-MM-DD format
    date_to: str    # YYYY-MM-DD format
    date_type: DateType
    award_type: AwardType
    correlation_id: str
    job_id: str = None  # Set after successful POST


@dataclass
class BulkJobResult:
    """Result of a completed bulk download job."""
    job_id: str
    correlation_id: str
    download_path: Path
    zip_size_bytes: int
    zip_sha256: str
    request_payload_hash: str
    status_url: str
    poll_attempts: int
    total_wait_seconds: float


class USASpendingAPIError(Exception):
    """Base exception for USASpending API errors."""
    pass


class JobSubmissionError(USASpendingAPIError):
    """Raised when job submission (POST) fails."""
    pass


class JobPollingError(USASpendingAPIError):
    """Raised when job polling encounters errors."""
    pass


class JobTimeoutError(USASpendingAPIError):
    """Raised when job exceeds maximum wait time."""
    pass


class DownloadError(USASpendingAPIError):
    """Raised when file download fails."""
    pass


class USASpendingAPIClient:
    """Client for USASpending bulk download API operations."""
    
    # API constants
    BASE_URL = "https://api.usaspending.gov/api/v2"
    BULK_AWARDS_ENDPOINT = "/bulk_download/awards/"
    
    def __init__(self, config: ETLConfig):
        """Initialize the API client.
        
        Args:
            config: ETL configuration containing timeouts, retries, etc.
        """
        self.config = config
        self.logger = get_logger(__name__)
        self.session = requests.Session()
        
        # Configure session defaults
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": f"USASpending-ETL/1.0 (data-insights-etl)",
        })
        
        # Timeouts: (connect_timeout, read_timeout)
        self.timeout = (10, 300)  # 10s connect, 5min read
        
    def fetch_awards_bulk(
        self,
        date_from: str,
        date_to: str,
        date_type: DateType,
        award_type: AwardType,
        correlation_id: Optional[str] = None
    ) -> BulkJobResult:
        """Fetch awards data via bulk download API.
        
        This is the main entry point that orchestrates:
        1. Job submission (POST)
        2. Status polling 
        3. File download
        4. Integrity verification
        
        Args:
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD) 
            date_type: Date field to filter on
            award_type: Type of awards (prime or sub)
            correlation_id: Optional correlation ID for tracking
            
        Returns:
            BulkJobResult with download path and metadata
            
        Raises:
            JobSubmissionError: If POST request fails
            JobTimeoutError: If job doesn't complete within max_wait_seconds
            DownloadError: If file download fails
            USASpendingAPIError: For other API-related errors
        """
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
            
        # Create job request
        request = BulkJobRequest(
            date_from=date_from,
            date_to=date_to,
            date_type=date_type,
            award_type=award_type,
            correlation_id=correlation_id
        )
        
        self.logger.info(
            "Starting bulk awards fetch",
            extra={
                "correlation_id": correlation_id,
                "date_from": date_from,
                "date_to": date_to,
                "date_type": date_type,
                "award_type": award_type,
                "event": "bulk_fetch_start"
            }
        )
        
        start_time = time.time()
        
        try:
            # Step 1: Submit job
            job_id, status_url = self._submit_job(request)
            request.job_id = job_id
            
            # Step 2: Poll until ready
            poll_attempts = self._poll_until_ready(job_id, status_url, correlation_id)
            
            # Step 3: Get download URL
            download_url = self._get_download_url(status_url, correlation_id)
            
            # Step 4: Download file
            download_path, zip_size, zip_sha256 = self._download_file(
                download_url, job_id, correlation_id
            )
            
            total_wait_seconds = time.time() - start_time
            
            # Step 5: Create payload hash for metadata
            request_payload_hash = self._compute_request_hash(request)
            
            result = BulkJobResult(
                job_id=job_id,
                correlation_id=correlation_id,
                download_path=download_path,
                zip_size_bytes=zip_size,
                zip_sha256=zip_sha256,
                request_payload_hash=request_payload_hash,
                status_url=status_url,
                poll_attempts=poll_attempts,
                total_wait_seconds=total_wait_seconds
            )
            
            self.logger.info(
                "Bulk awards fetch completed successfully",
                extra={
                    "correlation_id": correlation_id,
                    "job_id": job_id,
                    "download_path": str(download_path),
                    "zip_size_bytes": zip_size,
                    "poll_attempts": poll_attempts,
                    "total_wait_seconds": round(total_wait_seconds, 2),
                    "event": "bulk_fetch_success"
                }
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                "Bulk awards fetch failed",
                extra={
                    "correlation_id": correlation_id,
                    "error_class": e.__class__.__name__,
                    "error_message": str(e),
                    "event": "bulk_fetch_error"
                }
            )
            raise
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=64),
        retry=retry_if_exception_type((requests.RequestException, requests.HTTPError)),
        after=after_log(logging.getLogger(__name__), logging.WARNING)
    )
    def _submit_job(self, request: BulkJobRequest) -> tuple[str, str]:
        """Submit bulk download job to USASpending API.
        
        Args:
            request: Job request details
            
        Returns:
            Tuple of (job_id, status_url)
            
        Raises:
            JobSubmissionError: If submission fails after retries
        """
        url = f"{self.BASE_URL}{self.BULK_AWARDS_ENDPOINT}"
        
        # Build payload according to USASpending API spec
        payload = {
            "award_levels": [request.award_type + "s"],  # "primes" or "subs"
            "filters": {
                "date_type": request.date_type,
                "date_range": {
                    "start_date": request.date_from,
                    "end_date": request.date_to
                }
            },
            "file_format": "csv"
        }
        
        self.logger.debug(
            "Submitting bulk download job",
            extra={
                "correlation_id": request.correlation_id,
                "url": url,
                "payload": payload,
                "event": "job_submit"
            }
        )
        
        try:
            response = self.session.post(
                url, 
                json=payload, 
                timeout=self.timeout
            )
            response.raise_for_status()
            
        except requests.RequestException as e:
            raise JobSubmissionError(f"Failed to submit bulk download job: {e}") from e
            
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            raise JobSubmissionError(f"Invalid JSON response: {e}") from e
            
        # Extract job ID and status URL from response
        if "file_name" not in result:
            raise JobSubmissionError(f"No file_name in response: {result}")
            
        job_id = result["file_name"]
        status_url = result.get("status_url")
        
        if not status_url:
            # Construct status URL if not provided
            status_url = f"{self.BASE_URL}/bulk_download/status/?file_name={job_id}"
            
        self.logger.info(
            "Bulk download job submitted successfully",
            extra={
                "correlation_id": request.correlation_id,
                "job_id": job_id,
                "status_url": status_url,
                "event": "job_submitted"
            }
        )
        
        return job_id, status_url
    
    def _poll_until_ready(
        self, 
        job_id: str, 
        status_url: str, 
        correlation_id: str
    ) -> int:
        """Poll job status until ready or timeout.
        
        Args:
            job_id: Job identifier
            status_url: URL to poll for status
            correlation_id: Correlation ID for tracking
            
        Returns:
            Number of poll attempts made
            
        Raises:
            JobTimeoutError: If job exceeds max wait time
            JobPollingError: If polling encounters errors
        """
        start_time = time.time()
        poll_attempts = 0
        poll_interval = 5  # Start with 5 second polling
        max_poll_interval = 60  # Cap at 60 seconds
        
        self.logger.info(
            "Starting job status polling",
            extra={
                "correlation_id": correlation_id,
                "job_id": job_id,
                "max_wait_seconds": self.config.max_wait_seconds,
                "event": "poll_start"
            }
        )
        
        while time.time() - start_time < self.config.max_wait_seconds:
            poll_attempts += 1
            
            try:
                status = self._check_job_status(status_url, correlation_id)
                
                if status == "ready":
                    self.logger.info(
                        "Job completed successfully",
                        extra={
                            "correlation_id": correlation_id,
                            "job_id": job_id,
                            "poll_attempts": poll_attempts,
                            "elapsed_seconds": round(time.time() - start_time, 2),
                            "event": "job_ready"
                        }
                    )
                    return poll_attempts
                    
                elif status == "failed":
                    raise JobPollingError(f"Job {job_id} failed on server side")
                    
                elif status == "pending":
                    self.logger.debug(
                        "Job still pending, waiting before next poll",
                        extra={
                            "correlation_id": correlation_id,
                            "job_id": job_id,
                            "poll_attempt": poll_attempts,
                            "poll_interval": poll_interval,
                            "event": "job_pending"
                        }
                    )
                    
                    time.sleep(poll_interval)
                    
                    # Exponential backoff with cap
                    poll_interval = min(poll_interval * 2, max_poll_interval)
                    
                else:
                    raise JobPollingError(f"Unknown job status: {status}")
                    
            except requests.RequestException as e:
                # Log but continue polling - transient network errors
                self.logger.warning(
                    "Polling request failed, will retry",
                    extra={
                        "correlation_id": correlation_id,
                        "job_id": job_id,
                        "poll_attempt": poll_attempts,
                        "error": str(e),
                        "event": "poll_error"
                    }
                )
                time.sleep(poll_interval)
        
        # Timeout exceeded
        elapsed = time.time() - start_time
        raise JobTimeoutError(
            f"Job {job_id} did not complete within {self.config.max_wait_seconds}s "
            f"(elapsed: {elapsed:.2f}s, attempts: {poll_attempts})"
        )
    
    def _check_job_status(self, status_url: str, correlation_id: str) -> JobStatus:
        """Check job status via API.
        
        Args:
            status_url: URL to check status
            correlation_id: Correlation ID for tracking
            
        Returns:
            Job status: "pending", "ready", or "failed"
            
        Raises:
            JobPollingError: If status check fails
        """
        try:
            response = self.session.get(status_url, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            
        except requests.RequestException as e:
            raise JobPollingError(f"Status check failed: {e}") from e
        except json.JSONDecodeError as e:
            raise JobPollingError(f"Invalid status response JSON: {e}") from e
            
        # Parse status from response
        status = result.get("status", "").lower()
        
        if status in ("ready", "finished"):
            return "ready"
        elif status in ("pending", "running", "started"):
            return "pending" 
        elif status in ("failed", "error"):
            return "failed"
        else:
            # Log unknown status but treat as pending
            self.logger.warning(
                "Unknown job status received",
                extra={
                    "correlation_id": correlation_id,
                    "status": status,
                    "full_response": result,
                    "event": "unknown_status"
                }
            )
            return "pending"
    
    def _get_download_url(self, status_url: str, correlation_id: str) -> str:
        """Get download URL from completed job.
        
        Args:
            status_url: Status URL to query
            correlation_id: Correlation ID for tracking
            
        Returns:
            Download URL for the result file
            
        Raises:
            JobPollingError: If download URL cannot be obtained
        """
        try:
            response = self.session.get(status_url, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            
        except requests.RequestException as e:
            raise JobPollingError(f"Failed to get download URL: {e}") from e
        except json.JSONDecodeError as e:
            raise JobPollingError(f"Invalid download URL response JSON: {e}") from e
            
        download_url = result.get("url") or result.get("download_url")
        
        if not download_url:
            raise JobPollingError(f"No download URL in response: {result}")
            
        self.logger.debug(
            "Download URL obtained",
            extra={
                "correlation_id": correlation_id,
                "download_url": download_url,
                "event": "download_url_obtained"
            }
        )
        
        return download_url
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.RequestException, requests.HTTPError)),
        after=after_log(logging.getLogger(__name__), logging.WARNING)
    )
    def _download_file(
        self, 
        download_url: str, 
        job_id: str, 
        correlation_id: str
    ) -> tuple[Path, int, str]:
        """Download file from URL with streaming and integrity checking.
        
        Args:
            download_url: URL to download from
            job_id: Job identifier for filename
            correlation_id: Correlation ID for tracking
            
        Returns:
            Tuple of (download_path, file_size_bytes, sha256_hash)
            
        Raises:
            DownloadError: If download fails
        """
        # Prepare download path
        download_dir = Path(self.config.download_dir)
        download_dir.mkdir(parents=True, exist_ok=True)
        
        # Use job_id as base filename, ensure .zip extension
        filename = job_id if job_id.endswith('.zip') else f"{job_id}.zip"
        download_path = download_dir / filename
        
        self.logger.info(
            "Starting file download",
            extra={
                "correlation_id": correlation_id,
                "job_id": job_id,
                "download_url": download_url,
                "download_path": str(download_path),
                "event": "download_start"
            }
        )
        
        hasher = hashlib.sha256()
        file_size = 0
        
        try:
            # Stream download with progress tracking
            with self.session.get(download_url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()
                
                # Check Content-Length if available
                content_length = response.headers.get('content-length')
                if content_length:
                    expected_size = int(content_length)
                    self.logger.debug(
                        "Download content length",
                        extra={
                            "correlation_id": correlation_id,
                            "expected_size_bytes": expected_size,
                            "event": "download_size"
                        }
                    )
                
                # Stream to file with hash computation
                with open(download_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                        if chunk:  # filter out keep-alive chunks
                            f.write(chunk)
                            hasher.update(chunk)
                            file_size += len(chunk)
        
        except requests.RequestException as e:
            # Clean up partial download
            if download_path.exists():
                download_path.unlink()
            raise DownloadError(f"Download failed: {e}") from e
        except OSError as e:
            # File system error
            if download_path.exists():
                download_path.unlink()
            raise DownloadError(f"File write error: {e}") from e
        
        # Compute final hash
        zip_sha256 = hasher.hexdigest()
        
        self.logger.info(
            "File download completed",
            extra={
                "correlation_id": correlation_id,
                "job_id": job_id,
                "download_path": str(download_path),
                "file_size_bytes": file_size,
                "sha256": zip_sha256,
                "event": "download_complete"
            }
        )
        
        return download_path, file_size, zip_sha256
    
    def _compute_request_hash(self, request: BulkJobRequest) -> str:
        """Compute hash of request payload for metadata."""
        payload = {
            "award_levels": [request.award_type + "s"],
            "filters": {
                "date_type": request.date_type,
                "date_range": {
                    "start_date": request.date_from,
                    "end_date": request.date_to
                }
            },
            "file_format": "csv"
        }
        
        # Serialize to JSON with consistent ordering
        payload_json = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        payload_bytes = payload_json.encode('utf-8')
        
        return hashlib.sha256(payload_bytes).hexdigest()
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience function for simple usage
def fetch_prime_awards_bulk(
    date_from: str,
    date_to: str, 
    date_type: DateType = "action_date",
    config: Optional[ETLConfig] = None,
    correlation_id: Optional[str] = None
) -> BulkJobResult:
    """Convenience function to fetch prime awards via bulk API.
    
    Args:
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        date_type: Date field to filter on
        config: ETL configuration (loads from env if None)
        correlation_id: Optional correlation ID
        
    Returns:
        BulkJobResult with download info
    """
    if config is None:
        from etl.config import get_config
        config = get_config()
    
    with USASpendingAPIClient(config) as client:
        return client.fetch_awards_bulk(
            date_from=date_from,
            date_to=date_to,
            date_type=date_type,
            award_type="prime",
            correlation_id=correlation_id
        )


def fetch_subawards_bulk(
    date_from: str,
    date_to: str,
    date_type: DateType = "action_date", 
    config: Optional[ETLConfig] = None,
    correlation_id: Optional[str] = None
) -> BulkJobResult:
    """Convenience function to fetch subawards via bulk API.
    
    Args:
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        date_type: Date field to filter on
        config: ETL configuration (loads from env if None)
        correlation_id: Optional correlation ID
        
    Returns:
        BulkJobResult with download info
    """
    if config is None:
        from etl.config import get_config
        config = get_config()
    
    with USASpendingAPIClient(config) as client:
        return client.fetch_awards_bulk(
            date_from=date_from,
            date_to=date_to,
            date_type=date_type,
            award_type="sub",
            correlation_id=correlation_id
        )