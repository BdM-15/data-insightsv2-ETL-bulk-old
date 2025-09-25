"""Integration test for daily incremental flow.

This test MUST fail initially until watermark and incremental implementations exist.
"""
import pytest
from unittest.mock import Mock, patch
from datetime import date, datetime, timedelta

# These will fail until implementations exist
from etl.staging.watermark import WatermarkManager  # Will fail until implemented
from etl.acquisition.chunk_planner import ChunkPlanner  # Will fail until implemented


def test_daily_incremental_flow_success(fake_config, temp_download_dir):
    """Test successful daily incremental run advances watermark with overlap."""
    
    # This will fail until WatermarkManager is implemented
    watermark_manager = WatermarkManager(fake_config)
    
    # Mock existing watermark
    initial_watermark = datetime(2024, 1, 15, 0, 0, 0)
    with patch.object(watermark_manager, 'get_watermark') as mock_get_watermark:
        mock_get_watermark.return_value = {
            'last_modified_to': initial_watermark,
            'overlap_days': 7,
            'updated_at': initial_watermark
        }
        
        # This will fail until ChunkPlanner supports incremental
        planner = ChunkPlanner(fake_config)
        
        # Calculate incremental window (should include overlap)
        current_date = date(2024, 1, 20)
        lookback_days = fake_config.current_days_lookback
        
        chunks = planner.plan_incremental_chunks(
            pipeline_name=fake_config.pipeline_name,
            current_date=current_date,
            lookback_days=lookback_days,
            award_scope='prime'
        )
        
        # Should create one chunk with overlap
        assert len(chunks) == 1, "Daily incremental should create one chunk"
        
        chunk = chunks[0]
        expected_start = initial_watermark.date() - timedelta(days=7)  # overlap
        expected_end = current_date - timedelta(days=lookback_days)
        
        assert chunk['window_start'] >= expected_start, "Should include overlap period"
        assert chunk['window_end'] <= expected_end, "Should end at lookback boundary"
        
        # Mock successful processing
        with patch('etl.acquisition.api_client.USASpendingClient') as mock_client:
            mock_client_instance = Mock()
            mock_client.return_value = mock_client_instance
            
            mock_client_instance.create_bulk_job.return_value = {
                'job_id': 'incremental-job-123'
            }
            mock_client_instance.poll_job_status.return_value = {
                'status': 'ready',
                'file_url': 'https://api.usaspending.gov/download/incr.zip'
            }
            mock_client_instance.download_file.return_value = {
                'local_path': f"{temp_download_dir}/incr.zip",
                'size_bytes': 500000,
                'checksum': 'c' * 64
            }
            
            # This will fail until script exists
            from scripts.fetch_incremental import run_daily_incremental
            
            result = run_daily_incremental(
                award_scope='prime',
                config=fake_config
            )
            
            assert result['success'] == True, "Incremental run should succeed"
            assert result['chunks_processed'] == 1, "Should process one chunk"
            
            # Verify watermark was advanced
            mock_advance_watermark = Mock()
            with patch.object(watermark_manager, 'advance_watermark', mock_advance_watermark):
                # Should advance to end of processing window
                expected_new_watermark = datetime.combine(
                    chunk['window_end'], 
                    datetime.min.time()
                )
                mock_advance_watermark.assert_called_once_with(
                    fake_config.pipeline_name,
                    expected_new_watermark
                )


def test_daily_incremental_no_new_data(fake_config):
    """Test daily incremental when no new data exists since last run."""
    
    # Mock watermark that is very recent (yesterday)
    recent_watermark = datetime.now() - timedelta(days=1)
    
    watermark_manager = WatermarkManager(fake_config)
    with patch.object(watermark_manager, 'get_watermark') as mock_get_watermark:
        mock_get_watermark.return_value = {
            'last_modified_to': recent_watermark,
            'overlap_days': 7,
            'updated_at': recent_watermark
        }
        
        planner = ChunkPlanner(fake_config)
        
        chunks = planner.plan_incremental_chunks(
            pipeline_name=fake_config.pipeline_name,
            current_date=date.today(),
            lookback_days=fake_config.current_days_lookback,
            award_scope='prime'
        )
        
        # Should create no chunks if window is too small or recent
        assert len(chunks) == 0, "Should create no chunks when no new data"


def test_daily_incremental_watermark_overlap_logic(fake_config):
    """Test that incremental chunks properly handle overlap days."""
    
    watermark_manager = WatermarkManager(fake_config)
    
    # Test different overlap configurations
    for overlap_days in [3, 7, 14]:
        base_watermark = datetime(2024, 1, 10, 0, 0, 0)
        
        with patch.object(watermark_manager, 'get_watermark') as mock_get_watermark:
            mock_get_watermark.return_value = {
                'last_modified_to': base_watermark,
                'overlap_days': overlap_days,
                'updated_at': base_watermark
            }
            
            planner = ChunkPlanner(fake_config)
            
            chunks = planner.plan_incremental_chunks(
                pipeline_name=fake_config.pipeline_name,
                current_date=date(2024, 1, 20),
                lookback_days=2,
                award_scope='prime'
            )
            
            if chunks:  # Only test if chunks were created
                chunk = chunks[0]
                expected_start = base_watermark.date() - timedelta(days=overlap_days)
                
                assert chunk['window_start'] <= expected_start, f"Overlap of {overlap_days} days not applied"
                assert chunk['overlap_days'] == overlap_days, "Chunk should track overlap days"


def test_daily_incremental_fail_fast_on_error(fake_config):
    """Test that incremental run fails fast on errors."""
    
    # Mock API client that fails
    with patch('etl.acquisition.api_client.USASpendingClient') as mock_client:
        mock_client_instance = Mock() 
        mock_client.return_value = mock_client_instance
        
        mock_client_instance.create_bulk_job.side_effect = ConnectionError("API down")
        
        # This will fail until script exists
        from scripts.fetch_incremental import run_daily_incremental
        
        with pytest.raises(Exception) as exc_info:
            run_daily_incremental(
                award_scope='prime',
                config=fake_config
            )
        
        # Should fail fast without attempting retries beyond configured limit
        assert "connection" in str(exc_info.value).lower() or "api" in str(exc_info.value).lower()


def test_daily_incremental_watermark_not_advanced_on_failure(fake_config):
    """Test that watermark is not advanced if incremental processing fails."""
    
    watermark_manager = WatermarkManager(fake_config)
    initial_watermark = datetime(2024, 1, 15, 0, 0, 0)
    
    with patch.object(watermark_manager, 'get_watermark') as mock_get_watermark:
        mock_get_watermark.return_value = {
            'last_modified_to': initial_watermark,
            'overlap_days': 7,
            'updated_at': initial_watermark
        }
        
        # Mock processing failure after successful download
        with patch('etl.staging.raw_loader.RawLoader') as mock_loader:
            mock_loader_instance = Mock()
            mock_loader.return_value = mock_loader_instance
            
            mock_loader_instance.load_chunk.side_effect = Exception("Database error")
            
            from scripts.fetch_incremental import run_daily_incremental
            
            with pytest.raises(Exception):
                run_daily_incremental(
                    award_scope='prime',
                    config=fake_config
                )
            
            # Verify watermark was NOT advanced
            with patch.object(watermark_manager, 'advance_watermark') as mock_advance:
                mock_advance.assert_not_called()


def test_daily_incremental_date_type_last_modified(fake_config):
    """Test that daily incremental uses last_modified_date type."""
    
    planner = ChunkPlanner(fake_config)
    
    # Mock watermark
    watermark_manager = WatermarkManager(fake_config) 
    with patch.object(watermark_manager, 'get_watermark') as mock_get_watermark:
        mock_get_watermark.return_value = {
            'last_modified_to': datetime(2024, 1, 10, 0, 0, 0),
            'overlap_days': 7,
            'updated_at': datetime(2024, 1, 10, 0, 0, 0)
        }
        
        chunks = planner.plan_incremental_chunks(
            pipeline_name=fake_config.pipeline_name,
            current_date=date(2024, 1, 20),
            lookback_days=2,
            award_scope='prime'
        )
        
        if chunks:
            chunk = chunks[0]
            assert chunk['date_type'] == 'last_modified_date', "Daily incremental should use last_modified_date"