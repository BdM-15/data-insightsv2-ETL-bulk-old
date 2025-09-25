"""End-to-end integration test for complete ETL pipeline.

This test MUST fail initially until all pipeline components are implemented.
"""
import pytest
from unittest.mock import Mock, patch
from datetime import date, datetime, timedelta
from pathlib import Path
import json

# These will fail until implementations exist
from scripts.run_pipeline import run_full_pipeline  # Will fail until implemented
from etl.checks.data_validator import DataValidator  # Will fail until implemented


def test_end_to_end_historical_pipeline_success(fake_config, temp_download_dir, pg_connection):
    """Test complete historical pipeline from API to database."""
    
    # This will fail until run_full_pipeline script exists
    # Mock successful pipeline execution
    with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
        mock_pipeline.return_value = {
            'success': True,
            'pipeline_type': 'historical',
            'total_chunks': 3,
            'chunks_processed': 3,
            'chunks_failed': 0,
            'total_records': 15000,
            'processing_duration_seconds': 180,
            'tables_created': [
                'awards_raw_20240101_20240115',
                'sub_awards_raw_20240101_20240115',
                'awards_raw_20240116_20240131', 
                'sub_awards_raw_20240116_20240131',
                'awards_raw_20240201_20240215',
                'sub_awards_raw_20240201_20240215'
            ],
            'data_quality_checks': {
                'total_checks': 8,
                'passed': 8,
                'failed': 0
            }
        }
        
        # Run historical pipeline
        result = run_full_pipeline(
            pipeline_type='historical',
            award_scope='prime',
            date_range_start=date(2024, 1, 1),
            date_range_end=date(2024, 2, 15),
            config=fake_config
        )
        
        assert result['success'] == True, "Historical pipeline should succeed"
        assert result['total_chunks'] == 3, "Should process expected number of chunks"
        assert result['chunks_failed'] == 0, "No chunks should fail"
        assert result['total_records'] > 0, "Should process records"
        
        # Verify data quality checks passed
        assert result['data_quality_checks']['failed'] == 0, "All data quality checks should pass"
        
        # Verify pipeline was called with correct parameters
        mock_pipeline.assert_called_once_with(
            pipeline_type='historical',
            award_scope='prime',
            date_range_start=date(2024, 1, 1),
            date_range_end=date(2024, 2, 15),
            config=fake_config
        )


def test_end_to_end_incremental_pipeline_success(fake_config, temp_download_dir, pg_connection):
    """Test complete incremental pipeline with watermark management."""
    
    # Set up existing watermark
    with pg_connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO watermarks (pipeline_name, last_modified_to, overlap_days)
            VALUES (%s, %s, %s)
        """, (fake_config.pipeline_name, datetime(2024, 1, 15, 0, 0, 0), 7))
        pg_connection.commit()
    
    with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
        mock_pipeline.return_value = {
            'success': True,
            'pipeline_type': 'incremental',
            'total_chunks': 1,
            'chunks_processed': 1,
            'chunks_failed': 0,
            'total_records': 2500,
            'processing_duration_seconds': 45,
            'watermark_advanced': True,
            'new_watermark': datetime(2024, 1, 20, 0, 0, 0),
            'tables_created': [
                'awards_raw_20240108_20240118',  # With overlap
                'sub_awards_raw_20240108_20240118'
            ],
            'data_quality_checks': {
                'total_checks': 4,
                'passed': 4,
                'failed': 0
            }
        }
        
        result = run_full_pipeline(
            pipeline_type='incremental',
            award_scope='prime',
            config=fake_config
        )
        
        assert result['success'] == True, "Incremental pipeline should succeed"
        assert result['watermark_advanced'] == True, "Watermark should be advanced"
        assert result['new_watermark'] > datetime(2024, 1, 15, 0, 0, 0), "New watermark should be later"
        assert result['total_records'] > 0, "Should process new records"


def test_end_to_end_pipeline_with_data_quality_failures(fake_config, pg_connection):
    """Test pipeline behavior when data quality checks fail."""
    
    with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
        mock_pipeline.return_value = {
            'success': False,
            'pipeline_type': 'historical',
            'total_chunks': 2,
            'chunks_processed': 2,
            'chunks_failed': 0,
            'total_records': 8000,
            'processing_duration_seconds': 90,
            'data_quality_checks': {
                'total_checks': 6,
                'passed': 4,
                'failed': 2,
                'failure_details': [
                    {
                        'check_name': 'duplicate_award_ids',
                        'table': 'awards_raw_20240101_20240115',
                        'error': 'Found 15 duplicate award IDs'
                    },
                    {
                        'check_name': 'missing_required_fields',
                        'table': 'sub_awards_raw_20240101_20240115', 
                        'error': 'Missing recipient_name in 23 records'
                    }
                ]
            },
            'failure_reason': 'Data quality validation failed'
        }
        
        result = run_full_pipeline(
            pipeline_type='historical',
            award_scope='prime',
            date_range_start=date(2024, 1, 1),
            date_range_end=date(2024, 1, 15),
            config=fake_config
        )
        
        assert result['success'] == False, "Pipeline should fail when data quality fails"
        assert result['data_quality_checks']['failed'] > 0, "Should report failed checks"
        assert len(result['data_quality_checks']['failure_details']) > 0, "Should provide failure details"
        assert 'duplicate_award_ids' in str(result), "Should report specific quality issues"


def test_end_to_end_pipeline_with_api_failures(fake_config):
    """Test pipeline behavior when API calls fail."""
    
    with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
        mock_pipeline.side_effect = ConnectionError("USASpending API is down")
        
        with pytest.raises(ConnectionError) as exc_info:
            run_full_pipeline(
                pipeline_type='historical',
                award_scope='prime',
                date_range_start=date(2024, 1, 1),
                date_range_end=date(2024, 1, 15),
                config=fake_config
            )
        
        assert "api" in str(exc_info.value).lower() or "usaspending" in str(exc_info.value).lower()


def test_end_to_end_pipeline_with_disk_space_failure(fake_config, temp_download_dir):
    """Test pipeline behavior when disk space is insufficient."""
    
    # Mock disk space check to return insufficient space
    with patch('etl.utils.disk.check_available_space') as mock_disk_check:
        mock_disk_check.return_value = {
            'available_gb': 0.5,  # Less than required minimum
            'sufficient': False
        }
        
        with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
            mock_pipeline.side_effect = Exception("Insufficient disk space: 0.5 GB available, 2 GB required")
            
            with pytest.raises(Exception) as exc_info:
                run_full_pipeline(
                    pipeline_type='historical',
                    award_scope='prime',
                    date_range_start=date(2024, 1, 1),
                    date_range_end=date(2024, 2, 15),
                    config=fake_config
                )
            
            assert "disk space" in str(exc_info.value).lower()


def test_end_to_end_pipeline_progress_tracking(fake_config, pg_connection):
    """Test that pipeline progress is tracked correctly in progress_chunks table."""
    
    with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
        
        def mock_pipeline_with_progress(*args, **kwargs):
            # Simulate progress updates during pipeline execution
            with pg_connection.cursor() as cursor:
                # Insert initial chunks
                cursor.execute("""
                    INSERT INTO progress_chunks (pipeline_name, chunk_number, total_chunks, status)
                    VALUES (%s, 1, 2, 'in_progress'),
                           (%s, 2, 2, 'pending')
                """, (fake_config.pipeline_name, fake_config.pipeline_name))
                
                # Update first chunk to complete
                cursor.execute("""
                    UPDATE progress_chunks 
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                    WHERE pipeline_name = %s AND chunk_number = 1
                """, (fake_config.pipeline_name,))
                
                # Update second chunk to complete
                cursor.execute("""
                    UPDATE progress_chunks 
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP  
                    WHERE pipeline_name = %s AND chunk_number = 2
                """, (fake_config.pipeline_name,))
                
                pg_connection.commit()
            
            return {
                'success': True,
                'pipeline_type': 'historical',
                'total_chunks': 2,
                'chunks_processed': 2,
                'chunks_failed': 0,
                'total_records': 10000
            }
        
        mock_pipeline.side_effect = mock_pipeline_with_progress
        
        result = run_full_pipeline(
            pipeline_type='historical',
            award_scope='prime',
            date_range_start=date(2024, 1, 1),
            date_range_end=date(2024, 1, 15),
            config=fake_config
        )
        
        assert result['success'] == True
        
        # Verify progress tracking
        with pg_connection.cursor() as cursor:
            cursor.execute("""
                SELECT chunk_number, status, completed_at
                FROM progress_chunks 
                WHERE pipeline_name = %s
                ORDER BY chunk_number
            """, (fake_config.pipeline_name,))
            
            progress_rows = cursor.fetchall()
            assert len(progress_rows) == 2, "Should track 2 chunks"
            
            for chunk_num, status, completed_at in progress_rows:
                assert status == 'completed', f"Chunk {chunk_num} should be completed"
                assert completed_at is not None, f"Chunk {chunk_num} should have completion time"


def test_end_to_end_pipeline_cleanup_on_success(fake_config, temp_download_dir):
    """Test that pipeline cleans up temporary files on success."""
    
    # Create some fake temporary files
    temp_archive = Path(temp_download_dir) / "awards_prime_20240115.zip"
    temp_extract_dir = Path(temp_download_dir) / "awards_prime_20240115"
    temp_archive.touch()
    temp_extract_dir.mkdir()
    (temp_extract_dir / "awards.csv").touch()
    
    with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
        mock_pipeline.return_value = {
            'success': True,
            'pipeline_type': 'historical',
            'total_chunks': 1,
            'chunks_processed': 1,
            'chunks_failed': 0,
            'total_records': 5000,
            'cleanup_performed': True,
            'files_cleaned': 3
        }
        
        # Mock cleanup operations
        with patch('pathlib.Path.unlink') as mock_unlink, \
             patch('shutil.rmtree') as mock_rmtree:
            
            result = run_full_pipeline(
                pipeline_type='historical',
                award_scope='prime',
                date_range_start=date(2024, 1, 1),
                date_range_end=date(2024, 1, 15),
                config=fake_config
            )
            
            assert result['success'] == True
            assert result['cleanup_performed'] == True, "Should perform cleanup on success"
            assert result['files_cleaned'] > 0, "Should clean up files"


def test_end_to_end_data_validation_integration(fake_config, pg_connection):
    """Test integration with data validation checks."""
    
    # This will fail until DataValidator is implemented
    with patch('etl.checks.data_validator.DataValidator') as mock_validator_class:
        validator_instance = Mock()
        mock_validator_class.return_value = validator_instance
        
        # Mock validation results
        validator_instance.validate_chunk_data.return_value = {
            'valid': True,
            'checks_performed': [
                'duplicate_detection',
                'required_fields',
                'data_types',
                'referential_integrity'
            ],
            'issues_found': [],
            'record_count': 5000,
            'quality_score': 0.98
        }
        
        with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
            mock_pipeline.return_value = {
                'success': True,
                'pipeline_type': 'historical',
                'total_chunks': 1,
                'chunks_processed': 1,
                'total_records': 5000,
                'data_quality_checks': {
                    'total_checks': 4,
                    'passed': 4,
                    'failed': 0,
                    'quality_score': 0.98
                }
            }
            
            result = run_full_pipeline(
                pipeline_type='historical',
                award_scope='prime',
                date_range_start=date(2024, 1, 1),
                date_range_end=date(2024, 1, 15),
                config=fake_config
            )
            
            assert result['success'] == True
            assert result['data_quality_checks']['quality_score'] >= 0.95, "Quality score should be high"
            
            # Verify validator was called
            validator_instance.validate_chunk_data.assert_called()


def test_end_to_end_pipeline_resume_after_failure(fake_config, pg_connection):
    """Test that pipeline can resume from where it left off after failure."""
    
    # Set up partial progress (first chunk completed, second failed)
    with pg_connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO progress_chunks (pipeline_name, chunk_number, total_chunks, status)
            VALUES (%s, 1, 3, 'completed'),
                   (%s, 2, 3, 'failed'), 
                   (%s, 3, 3, 'pending')
        """, (fake_config.pipeline_name, fake_config.pipeline_name, fake_config.pipeline_name))
        pg_connection.commit()
    
    with patch('scripts.run_pipeline.run_full_pipeline') as mock_pipeline:
        mock_pipeline.return_value = {
            'success': True,
            'pipeline_type': 'historical',
            'total_chunks': 3,
            'chunks_processed': 2,  # Only processed chunks 2 and 3 (resumed)
            'chunks_skipped': 1,    # Skipped chunk 1 (already completed)
            'chunks_failed': 0,
            'total_records': 7000,
            'resume_mode': True,
            'resumed_from_chunk': 2
        }
        
        result = run_full_pipeline(
            pipeline_type='historical',
            award_scope='prime',
            date_range_start=date(2024, 1, 1),
            date_range_end=date(2024, 2, 15),
            config=fake_config,
            resume_mode=True
        )
        
        assert result['success'] == True
        assert result['resume_mode'] == True, "Should indicate resume mode"
        assert result['chunks_skipped'] == 1, "Should skip already completed chunks"
        assert result['resumed_from_chunk'] == 2, "Should resume from correct chunk"