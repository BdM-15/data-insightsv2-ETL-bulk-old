"""Integration test for archive processing flow.

This test MUST fail initially until archive processing implementations exist.
"""
import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import json
from datetime import datetime

# These will fail until implementations exist  
from etl.staging.archive_processor import ArchiveProcessor  # Will fail until implemented
from etl.staging.raw_loader import RawLoader  # Will fail until implemented
from etl.utils.hash import sha256_file  # Already implemented


def test_archive_processing_success(fake_config, temp_download_dir):
    """Test successful archive processing extracts and loads data."""
    
    # Create fake downloaded archive
    archive_path = Path(temp_download_dir) / "awards_prime_20240115.zip"
    archive_path.touch()  # Create empty file for test
    
    # Create fake metadata
    metadata = {
        "job_id": "hist-job-123",
        "award_scope": "prime", 
        "date_type": "action_date",
        "window_start": "2024-01-01",
        "window_end": "2024-01-15",
        "chunk_number": 1,
        "total_chunks": 1,
        "downloaded_at": datetime.now().isoformat(),
        "file_url": "https://api.usaspending.gov/download/hist.zip",
        "size_bytes": 500000,
        "checksum": "a" * 64
    }
    
    metadata_path = Path(temp_download_dir) / "awards_prime_20240115_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    
    # This will fail until ArchiveProcessor is implemented
    processor = ArchiveProcessor(fake_config)
    
    # Mock extraction results
    with patch.object(processor, 'extract_archive') as mock_extract:
        extracted_files = [
            Path(temp_download_dir) / "awards_prime_20240115" / "awards.csv",
            Path(temp_download_dir) / "awards_prime_20240115" / "sub_awards.csv"
        ]
        
        # Create fake extracted files
        for file_path in extracted_files:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("id,title,amount\n1,Test Award,100000")
        
        mock_extract.return_value = {
            'extract_dir': Path(temp_download_dir) / "awards_prime_20240115",
            'csv_files': extracted_files,
            'total_files': 2,
            'total_size_bytes': 1000
        }
        
        # This will fail until RawLoader is implemented
        loader = RawLoader(fake_config)
        
        # Mock load results 
        with patch.object(loader, 'load_chunk') as mock_load:
            mock_load.return_value = {
                'success': True,
                'records_loaded': 1000,
                'tables_created': ['awards_raw_20240115', 'sub_awards_raw_20240115'],
                'load_duration_seconds': 30
            }
            
            # Process the archive
            result = processor.process_archive(
                archive_path=archive_path,
                metadata_path=metadata_path
            )
            
            assert result['success'] == True, "Archive processing should succeed"
            assert result['records_loaded'] == 1000, "Should load expected records"
            assert len(result['tables_created']) == 2, "Should create expected tables"
            
            # Verify extract was called correctly
            mock_extract.assert_called_once_with(
                archive_path=archive_path,
                extract_dir=Path(temp_download_dir) / "awards_prime_20240115"
            )
            
            # Verify load was called correctly
            mock_load.assert_called_once()
            load_args = mock_load.call_args[1]
            assert load_args['metadata'] == metadata
            assert load_args['csv_files'] == extracted_files


def test_archive_processing_checksum_validation(fake_config, temp_download_dir):
    """Test that archive processing validates checksums before extraction."""
    
    # Create fake archive with known content
    archive_path = Path(temp_download_dir) / "awards_prime_20240115.zip"
    test_content = b"fake zip file content for checksum test"
    archive_path.write_bytes(test_content)
    
    # Calculate actual checksum
    actual_checksum = sha256_file(archive_path)
    
    # Create metadata with correct checksum
    metadata = {
        "job_id": "hist-job-123",
        "checksum": actual_checksum,
        "size_bytes": len(test_content)
    }
    
    metadata_path = Path(temp_download_dir) / "awards_prime_20240115_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    
    processor = ArchiveProcessor(fake_config)
    
    # Should validate checksum successfully
    with patch.object(processor, 'extract_archive') as mock_extract:
        mock_extract.return_value = {
            'extract_dir': Path(temp_download_dir) / "awards_prime_20240115",
            'csv_files': [],
            'total_files': 0,
            'total_size_bytes': 0
        }
        
        with patch.object(processor, '_load_data') as mock_load:
            result = processor.process_archive(
                archive_path=archive_path,
                metadata_path=metadata_path  
            )
            
            assert result.get('checksum_valid') == True, "Checksum validation should pass"


def test_archive_processing_checksum_mismatch_fails(fake_config, temp_download_dir):
    """Test that archive processing fails on checksum mismatch."""
    
    # Create fake archive
    archive_path = Path(temp_download_dir) / "awards_prime_20240115.zip"
    archive_path.write_bytes(b"fake zip content")
    
    # Create metadata with wrong checksum
    metadata = {
        "job_id": "hist-job-123",
        "checksum": "wrong_checksum_value",  # Intentionally wrong
        "size_bytes": 100
    }
    
    metadata_path = Path(temp_download_dir) / "awards_prime_20240115_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    
    processor = ArchiveProcessor(fake_config)
    
    # Should fail on checksum mismatch
    with pytest.raises(Exception) as exc_info:
        processor.process_archive(
            archive_path=archive_path,
            metadata_path=metadata_path
        )
    
    assert "checksum" in str(exc_info.value).lower(), "Should fail with checksum error"


def test_archive_processing_extraction_error_fails(fake_config, temp_download_dir):
    """Test that archive processing fails fast on extraction errors."""
    
    # Create fake archive and metadata
    archive_path = Path(temp_download_dir) / "awards_prime_20240115.zip"
    archive_path.write_bytes(b"corrupted zip content")
    
    metadata = {
        "job_id": "hist-job-123",
        "checksum": sha256_file(archive_path),
        "size_bytes": archive_path.stat().st_size
    }
    
    metadata_path = Path(temp_download_dir) / "awards_prime_20240115_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    
    processor = ArchiveProcessor(fake_config)
    
    # Mock extraction failure
    with patch.object(processor, 'extract_archive') as mock_extract:
        mock_extract.side_effect = Exception("Zip extraction failed - corrupted file")
        
        with pytest.raises(Exception) as exc_info:
            processor.process_archive(
                archive_path=archive_path,
                metadata_path=metadata_path
            )
        
        assert "extraction" in str(exc_info.value).lower() or "zip" in str(exc_info.value).lower()


def test_archive_processing_load_error_fails(fake_config, temp_download_dir):
    """Test that archive processing fails fast on load errors."""
    
    # Create fake archive and metadata  
    archive_path = Path(temp_download_dir) / "awards_prime_20240115.zip"
    archive_path.write_bytes(b"fake zip content")
    
    metadata = {
        "job_id": "hist-job-123", 
        "checksum": sha256_file(archive_path),
        "size_bytes": archive_path.stat().st_size
    }
    
    metadata_path = Path(temp_download_dir) / "awards_prime_20240115_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    
    processor = ArchiveProcessor(fake_config)
    
    # Mock successful extraction but failed load
    with patch.object(processor, 'extract_archive') as mock_extract:
        extracted_files = [Path(temp_download_dir) / "test.csv"]
        extracted_files[0].parent.mkdir(parents=True, exist_ok=True)
        extracted_files[0].write_text("id,title\n1,Test")
        
        mock_extract.return_value = {
            'extract_dir': Path(temp_download_dir) / "extracted",
            'csv_files': extracted_files,
            'total_files': 1,
            'total_size_bytes': 100
        }
        
        # Mock load failure
        loader = RawLoader(fake_config)
        with patch.object(loader, 'load_chunk') as mock_load:
            mock_load.side_effect = Exception("Database connection failed")
            
            with pytest.raises(Exception) as exc_info:
                processor.process_archive(
                    archive_path=archive_path,
                    metadata_path=metadata_path
                )
            
            assert "database" in str(exc_info.value).lower() or "load" in str(exc_info.value).lower()


def test_archive_processing_cleanup_on_success(fake_config, temp_download_dir):
    """Test that archive processing cleans up temporary files on success."""
    
    # Create fake archive and metadata
    archive_path = Path(temp_download_dir) / "awards_prime_20240115.zip"
    archive_path.write_bytes(b"fake zip content")
    
    metadata_path = Path(temp_download_dir) / "awards_prime_20240115_metadata.json" 
    metadata_path.write_text('{"job_id": "test"}')
    
    processor = ArchiveProcessor(fake_config)
    
    # Mock successful processing
    extract_dir = Path(temp_download_dir) / "awards_prime_20240115"
    with patch.object(processor, 'extract_archive') as mock_extract:
        mock_extract.return_value = {
            'extract_dir': extract_dir,
            'csv_files': [],
            'total_files': 0,
            'total_size_bytes': 0
        }
        
        with patch.object(processor, '_load_data') as mock_load:
            mock_load.return_value = {
                'success': True,
                'records_loaded': 0,
                'tables_created': []
            }
            
            # Mock cleanup behavior
            with patch('shutil.rmtree') as mock_rmtree:
                result = processor.process_archive(
                    archive_path=archive_path,
                    metadata_path=metadata_path,
                    cleanup_after_success=True
                )
                
                assert result['success'] == True
                
                # Should clean up extraction directory
                mock_rmtree.assert_called_with(extract_dir, ignore_errors=True)


def test_archive_processing_preserve_files_on_failure(fake_config, temp_download_dir):
    """Test that archive processing preserves files on failure for debugging."""
    
    archive_path = Path(temp_download_dir) / "awards_prime_20240115.zip"
    archive_path.write_bytes(b"fake zip content")
    
    metadata_path = Path(temp_download_dir) / "awards_prime_20240115_metadata.json"
    metadata_path.write_text('{"job_id": "test"}')
    
    processor = ArchiveProcessor(fake_config)
    
    # Mock extraction success but load failure
    extract_dir = Path(temp_download_dir) / "awards_prime_20240115"
    with patch.object(processor, 'extract_archive') as mock_extract:
        mock_extract.return_value = {
            'extract_dir': extract_dir,
            'csv_files': [],
            'total_files': 0,
            'total_size_bytes': 0
        }
        
        with patch.object(processor, '_load_data') as mock_load:
            mock_load.side_effect = Exception("Load failed")
            
            # Mock cleanup behavior - should NOT be called on failure
            with patch('shutil.rmtree') as mock_rmtree:
                with pytest.raises(Exception):
                    processor.process_archive(
                        archive_path=archive_path,
                        metadata_path=metadata_path,
                        cleanup_after_success=True
                    )
                
                # Should NOT clean up on failure
                mock_rmtree.assert_not_called()