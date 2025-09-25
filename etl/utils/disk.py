"""Disk space utilities and guardrails."""
import shutil
from pathlib import Path
from typing import NamedTuple

from ..config import get_config


class DiskUsage(NamedTuple):
    """Disk usage information."""
    total_gb: float
    used_gb: float
    free_gb: float


class DiskGuardError(Exception):
    """Raised when disk space guardrails are violated."""
    pass


def get_disk_usage(path: str) -> DiskUsage:
    """Get disk usage for given path in GB."""
    usage = shutil.disk_usage(path)
    
    # Convert bytes to GB
    total_gb = usage.total / (1024 ** 3)
    free_gb = usage.free / (1024 ** 3)
    used_gb = total_gb - free_gb
    
    return DiskUsage(
        total_gb=round(total_gb, 2),
        used_gb=round(used_gb, 2),
        free_gb=round(free_gb, 2)
    )


def check_disk_space(path: str, required_gb: float = None) -> DiskUsage:
    """Check disk space and enforce guardrails.
    
    Args:
        path: Directory path to check
        required_gb: Optional override for minimum required space
        
    Returns:
        DiskUsage information
        
    Raises:
        DiskGuardError: If insufficient disk space
    """
    config = get_config()
    min_required = required_gb or config.min_free_gb
    
    # Ensure path exists
    Path(path).mkdir(parents=True, exist_ok=True)
    
    usage = get_disk_usage(path)
    
    if usage.free_gb < min_required:
        raise DiskGuardError(
            f"Insufficient disk space: {usage.free_gb:.1f}GB free, "
            f"require {min_required:.1f}GB minimum"
        )
    
    return usage


def log_disk_usage(path: str, logger) -> DiskUsage:
    """Log disk usage information."""
    usage = get_disk_usage(path)
    
    config = get_config()
    
    logger.info(
        "Disk usage check",
        extra={
            'event': 'disk_usage',
            'path': path,
            'free_gb': usage.free_gb,
            'used_gb': usage.used_gb,
            'total_gb': usage.total_gb,
            'min_free_gb_threshold': config.min_free_gb,
            'target_peak_gb_threshold': config.target_peak_gb,
            'above_min_threshold': usage.free_gb >= config.min_free_gb,
            'below_target_peak': usage.used_gb <= config.target_peak_gb
        }
    )
    
    return usage


def ensure_download_space(download_dir: str, estimated_size_gb: float = 0) -> DiskUsage:
    """Ensure sufficient space for download operations.
    
    Args:
        download_dir: Download directory path
        estimated_size_gb: Estimated size of download in GB
        
    Returns:
        DiskUsage information
        
    Raises:
        DiskGuardError: If insufficient space for operation
    """
    config = get_config()
    
    # Required space = min_free_gb + estimated download size + 10% buffer
    buffer_factor = 1.1
    required_space = config.min_free_gb + (estimated_size_gb * buffer_factor)
    
    usage = check_disk_space(download_dir, required_space)
    
    return usage