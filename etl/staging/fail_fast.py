"""
Fail-fast controller for ETL pipeline error handling.

This module implements fail-fast behavior for the ETL pipeline, providing centralized
exception handling, error classification, and pipeline halt logic for critical errors.
"""

import logging
import sys
from typing import Optional, Dict, Any, List, Type, Callable
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
import uuid
import traceback

from ..config import get_config
from ..utils.logging import get_logger
from .progress import ProgressRecorder, ChunkStatus

logger = get_logger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels for fail-fast decisions."""
    LOW = "low"           # Recoverable, continue processing
    MEDIUM = "medium"     # Recoverable with retry, log warning
    HIGH = "high"         # Chunk-level failure, skip chunk but continue batch
    CRITICAL = "critical" # Pipeline-level failure, halt immediately


class FailFastAction(Enum):
    """Actions to take when errors occur."""
    CONTINUE = "continue"     # Log and continue processing
    RETRY = "retry"           # Retry the operation
    SKIP_CHUNK = "skip_chunk" # Skip current chunk, continue batch
    HALT_BATCH = "halt_batch" # Stop current batch processing  
    HALT_PIPELINE = "halt_pipeline" # Stop entire pipeline


@dataclass
class ErrorInfo:
    """
    Information about an error occurrence.
    
    Attributes:
        exception: The original exception
        severity: Error severity level
        action: Recommended action
        context: Additional context information
        correlation_id: Chunk or job correlation ID
        timestamp: When the error occurred
        traceback_str: Full traceback string
        retry_count: Number of retries attempted
    """
    exception: Exception
    severity: ErrorSeverity
    action: FailFastAction
    context: Dict[str, Any]
    correlation_id: Optional[uuid.UUID] = None
    timestamp: Optional[datetime] = None
    traceback_str: Optional[str] = None
    retry_count: int = 0
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.traceback_str is None:
            self.traceback_str = traceback.format_exc()


class FailFastError(Exception):
    """Raised when fail-fast logic determines pipeline should halt."""
    
    def __init__(self, message: str, error_info: ErrorInfo):
        super().__init__(message)
        self.error_info = error_info


class FailFastController:
    """
    Controls fail-fast behavior and error handling throughout the ETL pipeline.
    
    Features:
    - Error classification by severity and type
    - Configurable fail-fast behavior
    - Progress tracking integration for failed chunks
    - Retry logic with backoff
    - Graceful shutdown coordination
    - Error reporting and metrics
    """
    
    # Error classification rules
    # Map exception types to (severity, action) pairs
    ERROR_RULES = {
        # Network and API errors - often transient
        'ConnectionError': (ErrorSeverity.MEDIUM, FailFastAction.RETRY),
        'TimeoutError': (ErrorSeverity.MEDIUM, FailFastAction.RETRY),
        'RequestException': (ErrorSeverity.MEDIUM, FailFastAction.RETRY),
        
        # File system errors - usually recoverable
        'FileNotFoundError': (ErrorSeverity.HIGH, FailFastAction.SKIP_CHUNK),
        'PermissionError': (ErrorSeverity.CRITICAL, FailFastAction.HALT_PIPELINE),
        'OSError': (ErrorSeverity.HIGH, FailFastAction.SKIP_CHUNK),
        
        # Database errors - severity depends on type
        'DatabaseConnectionError': (ErrorSeverity.CRITICAL, FailFastAction.HALT_PIPELINE),
        'IntegrityError': (ErrorSeverity.HIGH, FailFastAction.SKIP_CHUNK),
        'OperationalError': (ErrorSeverity.MEDIUM, FailFastAction.RETRY),
        
        # Data validation errors
        'HeaderValidationError': (ErrorSeverity.HIGH, FailFastAction.SKIP_CHUNK),
        'CSVProcessingError': (ErrorSeverity.HIGH, FailFastAction.SKIP_CHUNK),
        'ValidationError': (ErrorSeverity.HIGH, FailFastAction.SKIP_CHUNK),
        
        # Resource exhaustion - critical
        'DiskGuardError': (ErrorSeverity.CRITICAL, FailFastAction.HALT_PIPELINE),
        'MemoryError': (ErrorSeverity.CRITICAL, FailFastAction.HALT_PIPELINE),
        
        # Configuration and setup errors - critical
        'ConfigValidationError': (ErrorSeverity.CRITICAL, FailFastAction.HALT_PIPELINE),
        
        # Default for unknown exceptions
        'Exception': (ErrorSeverity.HIGH, FailFastAction.SKIP_CHUNK)
    }
    
    def __init__(self, progress_recorder: Optional[ProgressRecorder] = None, fail_fast: Optional[bool] = None):
        """
        Initialize fail-fast controller.
        
        Args:
            progress_recorder: Optional ProgressRecorder for updating chunk status
            fail_fast: Override fail_fast config (defaults to config.fail_fast)
        """
        config = get_config()
        self.fail_fast_enabled = fail_fast if fail_fast is not None else config.fail_fast
        self.progress_recorder = progress_recorder
        self.max_retries = config.max_retries
        
        # Error tracking
        self.error_count = 0
        self.critical_errors = []
        self.chunk_failures = {}
        
        logger.debug(f"Initialized FailFastController: fail_fast={self.fail_fast_enabled}, max_retries={self.max_retries}")
    
    def handle_error(
        self,
        exception: Exception,
        context: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[uuid.UUID] = None,
        operation: str = "unknown"
    ) -> ErrorInfo:
        """
        Handle an error according to fail-fast rules.
        
        Args:
            exception: The exception that occurred
            context: Additional context information
            correlation_id: Chunk or job correlation ID
            operation: Description of the operation that failed
            
        Returns:
            ErrorInfo with classification and recommended action
            
        Raises:
            FailFastError: If fail-fast is enabled and error is critical
        """
        # Classify error
        error_info = self._classify_error(exception, context or {}, correlation_id)
        
        # Update error tracking
        self.error_count += 1
        if error_info.severity == ErrorSeverity.CRITICAL:
            self.critical_errors.append(error_info)
        
        # Log error with appropriate level
        self._log_error(error_info, operation)
        
        # Update progress tracking if available
        if self.progress_recorder and correlation_id:
            self._update_progress_for_error(correlation_id, error_info)
        
        # Apply fail-fast logic
        if self.fail_fast_enabled:
            self._apply_fail_fast_action(error_info, operation)
        
        return error_info
    
    def handle_chunk_error(
        self,
        exception: Exception,
        correlation_id: uuid.UUID,
        context: Optional[Dict[str, Any]] = None
    ) -> ErrorInfo:
        """
        Handle errors specific to chunk processing.
        
        Args:
            exception: The exception that occurred
            correlation_id: Chunk correlation ID
            context: Additional context information
            
        Returns:
            ErrorInfo with classification and recommended action
        """
        # Track chunk-specific failures
        if correlation_id not in self.chunk_failures:
            self.chunk_failures[correlation_id] = []
        
        error_info = self.handle_error(exception, context, correlation_id, "chunk_processing")
        self.chunk_failures[correlation_id].append(error_info)
        
        # Check if chunk has too many failures
        if len(self.chunk_failures[correlation_id]) > self.max_retries:
            logger.error(f"Chunk {correlation_id} exceeded max retries ({self.max_retries})")
            if self.fail_fast_enabled:
                raise FailFastError(
                    f"Chunk {correlation_id} failed after {self.max_retries} retries",
                    error_info
                )
        
        return error_info
    
    def should_retry(self, error_info: ErrorInfo) -> bool:
        """
        Determine if an operation should be retried.
        
        Args:
            error_info: ErrorInfo from previous failure
            
        Returns:
            True if operation should be retried
        """
        if error_info.action != FailFastAction.RETRY:
            return False
        
        if error_info.retry_count >= self.max_retries:
            return False
        
        # Don't retry critical errors
        if error_info.severity == ErrorSeverity.CRITICAL:
            return False
        
        return True
    
    def increment_retry_count(self, error_info: ErrorInfo) -> ErrorInfo:
        """Increment retry count for an error."""
        error_info.retry_count += 1
        return error_info
    
    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get summary of errors encountered.
        
        Returns:
            Dictionary with error statistics and summaries
        """
        severity_counts = {}
        action_counts = {}
        exception_types = {}
        
        # Count errors by correlation_id
        all_errors = []
        for errors in self.chunk_failures.values():
            all_errors.extend(errors)
        all_errors.extend(self.critical_errors)
        
        for error in all_errors:
            # Count by severity
            severity = error.severity.value
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            # Count by action
            action = error.action.value
            action_counts[action] = action_counts.get(action, 0) + 1
            
            # Count by exception type
            exc_type = error.exception.__class__.__name__
            exception_types[exc_type] = exception_types.get(exc_type, 0) + 1
        
        return {
            'total_errors': self.error_count,
            'critical_errors': len(self.critical_errors),
            'failed_chunks': len(self.chunk_failures),
            'severity_counts': severity_counts,
            'action_counts': action_counts,
            'exception_types': exception_types,
            'fail_fast_enabled': self.fail_fast_enabled
        }
    
    def _classify_error(self, exception: Exception, context: Dict[str, Any], correlation_id: Optional[uuid.UUID]) -> ErrorInfo:
        """Classify error by type and determine appropriate action."""
        exc_type = exception.__class__.__name__
        
        # Look up error rules (fallback to base Exception rule)
        severity, action = self.ERROR_RULES.get(exc_type, self.ERROR_RULES['Exception'])
        
        # Override with fail-fast configuration
        if not self.fail_fast_enabled and action == FailFastAction.HALT_PIPELINE:
            action = FailFastAction.HALT_BATCH
        
        return ErrorInfo(
            exception=exception,
            severity=severity,
            action=action,
            context=context,
            correlation_id=correlation_id
        )
    
    def _log_error(self, error_info: ErrorInfo, operation: str) -> None:
        """Log error with appropriate level and structured data."""
        log_level = self._get_log_level(error_info.severity)
        
        log_data = {
            'event': 'error_handled',
            'operation': operation,
            'exception_type': error_info.exception.__class__.__name__,
            'exception_message': str(error_info.exception),
            'severity': error_info.severity.value,
            'action': error_info.action.value,
            'correlation_id': str(error_info.correlation_id) if error_info.correlation_id else None,
            'retry_count': error_info.retry_count,
            'context': error_info.context
        }
        
        logger.log(log_level, f"Error in {operation}: {error_info.exception}", extra=log_data)
        
        # Log traceback for critical errors
        if error_info.severity == ErrorSeverity.CRITICAL:
            logger.error(f"Traceback for critical error:\n{error_info.traceback_str}")
    
    def _update_progress_for_error(self, correlation_id: uuid.UUID, error_info: ErrorInfo) -> None:
        """Update progress tracking for failed chunk."""
        try:
            if error_info.action in [FailFastAction.SKIP_CHUNK, FailFastAction.HALT_BATCH, FailFastAction.HALT_PIPELINE]:
                self.progress_recorder.mark_chunk_failed(correlation_id, error_info.exception)
        except Exception as e:
            logger.warning(f"Failed to update progress for error: {e}")
    
    def _apply_fail_fast_action(self, error_info: ErrorInfo, operation: str) -> None:
        """Apply the fail-fast action for an error."""
        if error_info.action == FailFastAction.HALT_PIPELINE:
            logger.critical(f"HALTING PIPELINE due to critical error in {operation}")
            raise FailFastError(f"Pipeline halted due to critical error: {error_info.exception}", error_info)
        
        elif error_info.action == FailFastAction.HALT_BATCH:
            logger.error(f"HALTING BATCH due to error in {operation}")
            raise FailFastError(f"Batch halted due to error: {error_info.exception}", error_info)
    
    @staticmethod
    def _get_log_level(severity: ErrorSeverity) -> int:
        """Map error severity to logging level."""
        return {
            ErrorSeverity.LOW: logging.INFO,
            ErrorSeverity.MEDIUM: logging.WARNING,
            ErrorSeverity.HIGH: logging.ERROR,
            ErrorSeverity.CRITICAL: logging.CRITICAL
        }[severity]


# Decorator for automatic error handling
def handle_errors(
    controller: FailFastController,
    operation: str = "unknown",
    correlation_id: Optional[uuid.UUID] = None,
    context: Optional[Dict[str, Any]] = None
):
    """
    Decorator for automatic error handling with fail-fast logic.
    
    Args:
        controller: FailFastController instance
        operation: Description of the operation
        correlation_id: Optional correlation ID
        context: Additional context information
    """
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                controller.handle_error(e, context, correlation_id, operation)
                raise
        return wrapper
    return decorator


# Context manager for batch error handling
class FailFastBatch:
    """Context manager for batch operations with fail-fast error handling."""
    
    def __init__(self, controller: FailFastController, operation: str):
        self.controller = controller
        self.operation = operation
        self.batch_errors = []
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            error_info = self.controller.handle_error(exc_val, {}, None, self.operation)
            self.batch_errors.append(error_info)
        
        # Suppress exception if not in fail-fast mode or not critical
        if exc_type is not None and not self.controller.fail_fast_enabled:
            return True  # Suppress exception
        
        return False  # Let exception propagate