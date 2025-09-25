"""SHA256 hashing utilities for file integrity verification."""
import hashlib
from pathlib import Path
from typing import BinaryIO, Union


CHUNK_SIZE = 64 * 1024  # 64KB chunks for streaming


def compute_file_hash(file_path: Union[str, Path]) -> str:
    """Compute SHA256 hash of a file using streaming reads.
    
    Args:
        file_path: Path to file to hash
        
    Returns:
        SHA256 hash as hex string
        
    Raises:
        FileNotFoundError: If file doesn't exist
        IOError: If file cannot be read
    """
    sha256_hash = hashlib.sha256()
    
    with open(file_path, "rb") as f:
        # Read file in chunks to handle large files efficiently
        while chunk := f.read(CHUNK_SIZE):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def compute_stream_hash(stream: BinaryIO) -> str:
    """Compute SHA256 hash of a binary stream.
    
    Args:
        stream: Binary stream to hash
        
    Returns:
        SHA256 hash as hex string
    """
    sha256_hash = hashlib.sha256()
    
    # Read stream in chunks
    while chunk := stream.read(CHUNK_SIZE):
        sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def compute_bytes_hash(data: bytes) -> str:
    """Compute SHA256 hash of bytes data.
    
    Args:
        data: Bytes to hash
        
    Returns:
        SHA256 hash as hex string
    """
    return hashlib.sha256(data).hexdigest()


def verify_file_hash(file_path: Union[str, Path], expected_hash: str) -> bool:
    """Verify file hash matches expected value.
    
    Args:
        file_path: Path to file to verify
        expected_hash: Expected SHA256 hash as hex string
        
    Returns:
        True if hash matches, False otherwise
    """
    try:
        actual_hash = compute_file_hash(file_path)
        return actual_hash.lower() == expected_hash.lower()
    except (FileNotFoundError, IOError):
        return False


class HashingStream:
    """Wrapper that computes hash while writing to underlying stream."""
    
    def __init__(self, stream: BinaryIO):
        self.stream = stream
        self.hasher = hashlib.sha256()
    
    def write(self, data: bytes) -> int:
        """Write data to stream and update hash."""
        self.hasher.update(data)
        return self.stream.write(data)
    
    def get_hash(self) -> str:
        """Get current hash value."""
        return self.hasher.hexdigest()
    
    def __getattr__(self, name):
        """Delegate other methods to underlying stream."""
        return getattr(self.stream, name)