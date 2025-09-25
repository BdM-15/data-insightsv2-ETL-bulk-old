"""
Disk guard integration for acquisition workflow.

This module provides pre-flight disk space checks and guardrails for the ETL pipeline.
It integrates the disk utility functions with chunk planning and download operations.
"""

import logging
from typing import List, Dict, Optional
from pathlib import Path
from dataclasses import dataclass

from ..config import get_config
from ..utils.disk import get_disk_usage, check_disk_space, ensure_download_space, DiskGuardError
from ..utils.logging import get_logger
from .chunk_planner import DateChunk

logger = get_logger(__name__)


@dataclass
class DiskGuardResult:
    """Result of disk space pre-flight check."""
    passed: bool
    free_gb: float
    total_gb: float
    used_gb: float
    required_gb: float
    estimated_peak_gb: float
    warnings: List[str]
    errors: List[str]
    
    @property
    def safety_margin_gb(self) -> float:
        """Calculate safety margin in GB."""
        return self.free_gb - self.required_gb
    
    @property
    def utilization_percent(self) -> float:
        """Calculate disk utilization percentage."""
        return (self.used_gb / self.total_gb) * 100


class DiskGuardManager:
    """
    Manages disk space guardrails for ETL operations.
    
    Features:
    - Pre-flight checks before starting chunk processing
    - Download size estimation based on historical data
    - Progressive space monitoring during execution
    - Fail-fast disk space violations
    """
    
    # Rough size estimates per day of data (in GB)
    # These are conservative estimates based on USASpending data patterns
    PRIME_AWARDS_GB_PER_DAY = 0.15  # ~150MB per day for prime awards
    SUBAWARDS_GB_PER_DAY = 0.05     # ~50MB per day for subawards
    
    # Compression and archive overhead factors
    COMPRESSION_FACTOR = 0.3  # ZIP compression typically reduces size by ~70%
    ARCHIVE_OVERHEAD = 1.2    # 20% overhead for metadata, temp files, etc.
    
    def __init__(self, download_dir: Optional[str] = None):
        """
        Initialize disk guard manager.
        
        Args:
            download_dir: Override download directory (defaults to config)
        """
        config = get_config()
        self.download_dir = Path(download_dir or config.download_dir)
        self.config = config
        
        # Ensure download directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"Initialized DiskGuardManager: download_dir={self.download_dir}")
    
    def estimate_chunk_size_gb(self, chunks: List[DateChunk], include_subawards: bool = False) -> float:
        """
        Estimate total disk space required for processing chunks.
        
        Args:
            chunks: List of DateChunk objects to process
            include_subawards: Whether to include subaward data estimates
            
        Returns:
            Estimated disk space in GB (including overhead)
        """
        total_days = sum(chunk.days_span for chunk in chunks)
        
        # Base data size estimates
        prime_size_gb = total_days * self.PRIME_AWARDS_GB_PER_DAY
        subaward_size_gb = total_days * self.SUBAWARDS_GB_PER_DAY if include_subawards else 0
        raw_data_gb = prime_size_gb + subaward_size_gb
        
        # Account for compression and overhead
        compressed_size_gb = raw_data_gb * self.COMPRESSION_FACTOR
        total_size_gb = compressed_size_gb * self.ARCHIVE_OVERHEAD
        
        logger.debug(f"Size estimate for {len(chunks)} chunks ({total_days} days): "
                    f"raw={raw_data_gb:.2f}GB, compressed={compressed_size_gb:.2f}GB, "
                    f"total={total_size_gb:.2f}GB")
        
        return total_size_gb
    
    def pre_flight_check(
        self, 
        chunks: List[DateChunk], 
        include_subawards: bool = False,
        fail_fast: Optional[bool] = None
    ) -> DiskGuardResult:
        """
        Perform pre-flight disk space check before processing chunks.
        
        Args:
            chunks: List of DateChunk objects to process
            include_subawards: Whether to include subaward data estimates
            fail_fast: Override fail_fast config (defaults to config.fail_fast)
            
        Returns:
            DiskGuardResult with check results
            
        Raises:
            DiskGuardError: If fail_fast=True and checks fail
        """
        if fail_fast is None:
            fail_fast = self.config.fail_fast
        
        # Get current disk usage
        usage = get_disk_usage(str(self.download_dir))
        
        # Estimate required space
        estimated_size_gb = self.estimate_chunk_size_gb(chunks, include_subawards)
        required_gb = self.config.min_free_gb + estimated_size_gb
        
        # Calculate peak usage estimate
        estimated_peak_gb = usage.used_gb + estimated_size_gb
        
        # Initialize result
        warnings = []
        errors = []
        passed = True
        
        # Check minimum free space
        if usage.free_gb < required_gb:
            error_msg = (f"Insufficient disk space: {usage.free_gb:.1f}GB free, "
                        f"require {required_gb:.1f}GB (min {self.config.min_free_gb:.1f}GB + "
                        f"estimated {estimated_size_gb:.1f}GB)")
            errors.append(error_msg)
            passed = False
        
        # Check target peak usage
        if estimated_peak_gb > self.config.target_peak_gb:
            warning_msg = (f"Processing may exceed target peak usage: "
                          f"estimated {estimated_peak_gb:.1f}GB > "
                          f"target {self.config.target_peak_gb:.1f}GB")
            warnings.append(warning_msg)
        
        # Check for very low free space
        if usage.free_gb < self.config.min_free_gb * 1.5:
            warning_msg = (f"Low free space: {usage.free_gb:.1f}GB "
                          f"(< 1.5x minimum {self.config.min_free_gb:.1f}GB)")
            warnings.append(warning_msg)
        
        result = DiskGuardResult(
            passed=passed,
            free_gb=usage.free_gb,
            total_gb=usage.total_gb,
            used_gb=usage.used_gb,
            required_gb=required_gb,
            estimated_peak_gb=estimated_peak_gb,
            warnings=warnings,
            errors=errors
        )
        
        # Log results
        self._log_pre_flight_result(result, chunks)
        
        # Fail-fast handling
        if fail_fast and not passed:
            error_summary = "; ".join(errors)
            raise DiskGuardError(f"Pre-flight disk check failed: {error_summary}")
        
        return result
    
    def monitor_during_execution(self, operation: str = "processing") -> DiskGuardResult:
        """
        Monitor disk space during execution for progressive guardrails.
        
        Args:
            operation: Description of current operation for logging
            
        Returns:
            DiskGuardResult with current status
            
        Raises:
            DiskGuardError: If fail_fast=True and critical thresholds are breached
        """
        usage = get_disk_usage(str(self.download_dir))
        
        warnings = []
        errors = []
        passed = True
        
        # Critical check: below absolute minimum
        if usage.free_gb < self.config.min_free_gb:
            error_msg = f"Critical: free space below minimum during {operation}: {usage.free_gb:.1f}GB"
            errors.append(error_msg)
            passed = False
        
        # Warning: approaching minimum  
        elif usage.free_gb < self.config.min_free_gb * 1.2:
            warning_msg = f"Warning: approaching minimum free space during {operation}: {usage.free_gb:.1f}GB"
            warnings.append(warning_msg)
        
        result = DiskGuardResult(
            passed=passed,
            free_gb=usage.free_gb,
            total_gb=usage.total_gb,
            used_gb=usage.used_gb,
            required_gb=self.config.min_free_gb,
            estimated_peak_gb=usage.used_gb,  # Current usage as peak estimate
            warnings=warnings,
            errors=errors
        )
        
        # Log monitoring result
        if errors or warnings:
            log_level = logging.ERROR if errors else logging.WARNING
            logger.log(log_level, f"Disk space monitoring during {operation}", extra={
                'event': 'disk_space_monitor',
                'operation': operation,
                'free_gb': usage.free_gb,
                'used_gb': usage.used_gb,
                'warnings': warnings,
                'errors': errors
            })
        
        # Fail-fast handling
        if self.config.fail_fast and not passed:
            error_summary = "; ".join(errors)
            raise DiskGuardError(f"Disk space monitoring failed during {operation}: {error_summary}")
        
        return result
    
    def ensure_chunk_space(self, chunk: DateChunk) -> None:
        """
        Ensure sufficient space for processing a single chunk.
        
        Args:
            chunk: DateChunk to process
            
        Raises:
            DiskGuardError: If insufficient space
        """
        estimated_size_gb = self.estimate_chunk_size_gb([chunk])
        
        try:
            ensure_download_space(str(self.download_dir), estimated_size_gb)
            logger.debug(f"Disk space OK for chunk {chunk.chunk_id} (estimated {estimated_size_gb:.2f}GB)")
        except DiskGuardError as e:
            logger.error(f"Insufficient space for chunk {chunk.chunk_id}: {e}")
            raise
    
    def get_space_recommendations(self, result: DiskGuardResult) -> List[str]:
        """
        Generate space management recommendations based on disk check results.
        
        Args:
            result: DiskGuardResult from pre-flight check
            
        Returns:
            List of recommended actions
        """
        recommendations = []
        
        if not result.passed:
            if result.safety_margin_gb < 0:
                recommendations.append(f"Free up at least {abs(result.safety_margin_gb):.1f}GB of disk space")
            
            if result.utilization_percent > 85:
                recommendations.append("Consider moving to a system with more disk space")
            
            recommendations.append("Reduce chunk_days configuration to process smaller batches")
            recommendations.append("Enable archive cleanup (if not already enabled)")
        
        elif result.warnings:
            recommendations.append("Monitor disk space closely during processing")
            
            if result.estimated_peak_gb > self.config.target_peak_gb:
                recommendations.append("Consider processing in smaller batches")
            
            recommendations.append("Verify archive cleanup is working properly")
        
        return recommendations
    
    def _log_pre_flight_result(self, result: DiskGuardResult, chunks: List[DateChunk]) -> None:
        """Log pre-flight check results."""
        log_level = logging.ERROR if result.errors else (logging.WARNING if result.warnings else logging.INFO)
        
        logger.log(log_level, f"Disk pre-flight check {'PASSED' if result.passed else 'FAILED'}", extra={
            'event': 'disk_pre_flight',
            'passed': result.passed,
            'chunks_count': len(chunks),
            'total_days': sum(c.days_span for c in chunks),
            'free_gb': result.free_gb,
            'required_gb': result.required_gb,
            'safety_margin_gb': result.safety_margin_gb,
            'estimated_peak_gb': result.estimated_peak_gb,
            'utilization_percent': result.utilization_percent,
            'warnings': result.warnings,
            'errors': result.errors
        })