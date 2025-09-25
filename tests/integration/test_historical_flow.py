"""Integration test for historical backfill flow.

This test MUST fail initially until the full backfill implementation exists.
Tests the end-to-end flow with mocked API responses.
"""
import pytest
from unittest.mock import Mock, patch
from datetime import date, datetime

# These will fail until implementations exist
from etl.acquisition.chunk_planner import ChunkPlanner  # Will fail until implemented
from etl.acquisition.api_client import USASpendingClient  # Will fail until implemented  
from etl.staging.progress import ProgressRecorder  # Will fail until implemented


def test_historical_backfill_flow_success(fake_config, temp_download_dir):
    """Test successful historical backfill creates progress records sequentially."""
    # This will fail until all components are implemented
    
    # Mock API responses
    with patch('etl.acquisition.api_client.USASpendingClient') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock successful job creation and completion
        mock_client.create_bulk_job.return_value = {
            'job_id': 'test-job-123',
            'status_url': 'https://api.usaspending.gov/status/test-job-123'
        }
        
        mock_client.poll_job_status.return_value = {
            'status': 'ready',
            'file_url': 'https://api.usaspending.gov/download/test-file.zip'
        }
        
        mock_client.download_file.return_value = {
            'local_path': f"{temp_download_dir}/test-file.zip",
            'size_bytes': 1000000,
            'checksum': 'a' * 64
        }
        
        # Test backfill for small date range
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 14)  # 2 weeks = 2 chunks of 7 days
        
        # This will fail until ChunkPlanner is implemented
        planner = ChunkPlanner(fake_config)
        chunks = planner.plan_historical_chunks(start_date, end_date, 'prime')
        
        assert len(chunks) == 2, "Should create 2 chunks for 14-day range"
        
        # This will fail until ProgressRecorder is implemented
        progress_recorder = ProgressRecorder(fake_config)
        
        # Process each chunk
        for chunk_index, chunk in enumerate(chunks):
            # Record chunk start
            progress_id = progress_recorder.start_chunk(
                pipeline_name=fake_config.pipeline_name,
                window_start=chunk['window_start'],
                window_end=chunk['window_end'], 
                chunk_index=chunk_index,
                job_id=f'test-job-{chunk_index}'
            )
            
            # Simulate successful processing
            progress_recorder.complete_chunk(
                progress_id=progress_id,
                rows_staged=10000,
                rows_deduped=9500,
                archive_path='test/path.zip',
                archive_sha256='b' * 64
            )
        
        # Verify all chunks completed successfully
        completed_chunks = progress_recorder.get_completed_chunks(fake_config.pipeline_name)
        assert len(completed_chunks) == 2, "All chunks should be completed"
        
        # Verify no fail-fast was triggered
        for chunk in completed_chunks:
            assert chunk['status'] == 'success'
            assert chunk['rows_staged'] > 0
            assert chunk['rows_deduped'] > 0


def test_historical_backfill_flow_with_api_failure(fake_config, temp_download_dir):
    """Test that API failure triggers fail-fast behavior."""
    
    # Mock API client that fails on second chunk
    with patch('etl.acquisition.api_client.USASpendingClient') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # First chunk succeeds
        call_count = 0
        def mock_create_job(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    'job_id': 'test-job-1',
                    'status_url': 'https://api.usaspending.gov/status/test-job-1'
                }
            else:
                # Second chunk fails
                raise ConnectionError("API unavailable")
        
        mock_client.create_bulk_job.side_effect = mock_create_job
        
        # This will fail until implementation exists
        from scripts.fetch_historical import run_historical_backfill  # Will fail until script exists
        
        start_date = date(2024, 1, 1) 
        end_date = date(2024, 1, 14)  # 2 chunks
        
        # Should raise exception due to fail-fast
        with pytest.raises(Exception) as exc_info:
            run_historical_backfill(
                start_date=start_date,
                end_date=end_date,
                award_scope='prime',
                config=fake_config
            )
        
        # Verify fail-fast was triggered
        assert "fail" in str(exc_info.value).lower() or "connection" in str(exc_info.value).lower()


def test_historical_backfill_chunk_progress_ordering(fake_config):
    """Test that chunks are processed in correct chronological order."""
    
    # This will fail until ChunkPlanner is implemented
    planner = ChunkPlanner(fake_config)
    
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 21)  # 3 weeks = 3 chunks
    
    chunks = planner.plan_historical_chunks(start_date, end_date, 'prime')
    
    assert len(chunks) == 3, "Should create 3 chunks for 21-day range"
    
    # Verify chunks are in chronological order
    prev_end = None
    for i, chunk in enumerate(chunks):
        assert chunk['chunk_index'] == i, f"Chunk {i} should have correct index"
        assert chunk['window_start'] < chunk['window_end'], "Chunk start should be before end"
        
        if prev_end is not None:
            assert chunk['window_start'] >= prev_end, "Chunks should not overlap"
        
        prev_end = chunk['window_end']


def test_historical_backfill_disk_space_check(fake_config):
    """Test that backfill checks disk space before starting chunks."""
    
    # Mock disk space check that fails
    with patch('etl.utils.disk.check_disk_space') as mock_disk_check:
        mock_disk_check.side_effect = Exception("Insufficient disk space")
        
        # This will fail until implementation exists
        from scripts.fetch_historical import run_historical_backfill
        
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)
        
        # Should fail with disk space error
        with pytest.raises(Exception) as exc_info:
            run_historical_backfill(
                start_date=start_date,
                end_date=end_date, 
                award_scope='prime',
                config=fake_config
            )
        
        assert "disk" in str(exc_info.value).lower()


def test_historical_backfill_resume_from_checkpoint(fake_config):
    """Test that backfill can resume from last successful chunk."""
    
    # This will fail until ProgressRecorder is implemented  
    progress_recorder = ProgressRecorder(fake_config)
    
    # Mock some completed chunks already exist
    with patch.object(progress_recorder, 'get_last_completed_chunk') as mock_last_chunk:
        mock_last_chunk.return_value = {
            'window_end': date(2024, 1, 7),
            'chunk_index': 0
        }
        
        # This will fail until ChunkPlanner supports resume
        planner = ChunkPlanner(fake_config)
        
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 21)
        
        # Should start from after last completed chunk
        chunks = planner.plan_historical_chunks_resume(
            start_date, end_date, 'prime', 
            last_completed_date=date(2024, 1, 7)
        )
        
        # Should only have remaining chunks
        assert len(chunks) == 2, "Should skip already completed chunk"
        assert chunks[0]['window_start'] >= date(2024, 1, 8), "Should start after last completed"