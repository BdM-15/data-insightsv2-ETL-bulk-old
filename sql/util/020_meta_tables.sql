-- SQL Migration: Create meta tables for ETL pipeline tracking
-- File: sql/util/020_meta_tables.sql  
-- Purpose: Create progress tracking and watermark tables
-- Dependencies: None
-- Target: PostgreSQL 14+

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS capture_insights;

-- Drop tables if exists (for re-running migration)
DROP TABLE IF EXISTS capture_insights.meta_chunk_progress CASCADE;
DROP TABLE IF EXISTS capture_insights.meta_refresh_watermarks CASCADE;

-- Create progress chunk tracking table
-- Tracks individual chunk processing status within pipeline runs
CREATE TABLE capture_insights.meta_chunk_progress (
    id bigserial PRIMARY KEY,
    pipeline_name text NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    chunk_index integer NOT NULL,
    job_id uuid NOT NULL,
    correlation_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('pending','in_progress','success','failed')),
    rows_staged bigint,
    rows_deduped bigint,
    archive_path_rel text,
    archive_sha256 text,
    error_class text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(pipeline_name, window_start, window_end, chunk_index)
);

-- Create watermark tracking table
-- Tracks incremental processing watermarks with overlap configuration
CREATE TABLE capture_insights.meta_refresh_watermarks (
    pipeline_name text PRIMARY KEY,
    last_modified_to timestamptz NOT NULL,
    overlap_days integer NOT NULL DEFAULT 7,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Create performance indexes for progress chunks
CREATE INDEX idx_meta_chunk_progress_pipeline_status 
ON capture_insights.meta_chunk_progress(pipeline_name, status);

CREATE INDEX idx_meta_chunk_progress_created_at 
ON capture_insights.meta_chunk_progress(created_at);

CREATE INDEX idx_meta_chunk_progress_window 
ON capture_insights.meta_chunk_progress(window_start, window_end);

CREATE INDEX idx_meta_chunk_progress_correlation_id 
ON capture_insights.meta_chunk_progress(correlation_id);

-- Create performance indexes for watermarks
CREATE INDEX idx_meta_refresh_watermarks_updated_at 
ON capture_insights.meta_refresh_watermarks(updated_at);

-- Add table comments
COMMENT ON TABLE capture_insights.meta_chunk_progress IS 
'Tracks processing status of individual chunks within ETL pipeline runs. 
Each chunk represents a date window being processed from the USASpending API.';

COMMENT ON TABLE capture_insights.meta_refresh_watermarks IS 
'Tracks incremental refresh watermarks for each pipeline. 
Controls where incremental processing should resume with overlap configuration.';

-- Add column comments for progress chunks
COMMENT ON COLUMN capture_insights.meta_chunk_progress.pipeline_name IS 'Identifies the specific ETL pipeline (e.g., usaspending-prime-awards)';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.window_start IS 'Start date of the data window being processed in this chunk';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.window_end IS 'End date of the data window being processed in this chunk';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.chunk_index IS 'Sequential index of this chunk within the larger pipeline run';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.job_id IS 'USASpending API job identifier for this chunk';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.correlation_id IS 'Links related chunks within the same pipeline execution';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.status IS 'Current processing status: pending, in_progress, success, or failed';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.rows_staged IS 'Number of raw records loaded into staging tables';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.rows_deduped IS 'Number of records after deduplication processing';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.archive_path_rel IS 'Relative path to downloaded archive file';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.archive_sha256 IS 'SHA256 checksum of downloaded archive for integrity verification';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.error_class IS 'Python exception class name if chunk processing failed';
COMMENT ON COLUMN capture_insights.meta_chunk_progress.error_message IS 'Error details if chunk processing failed';

-- Add column comments for watermarks  
COMMENT ON COLUMN capture_insights.meta_refresh_watermarks.pipeline_name IS 'Identifies the specific ETL pipeline (e.g., usaspending-prime-awards)';
COMMENT ON COLUMN capture_insights.meta_refresh_watermarks.last_modified_to IS 'Latest last_modified_date successfully processed by incremental runs';
COMMENT ON COLUMN capture_insights.meta_refresh_watermarks.overlap_days IS 'Number of days to overlap with previous runs to catch late-arriving updates';
COMMENT ON COLUMN capture_insights.meta_refresh_watermarks.updated_at IS 'Timestamp when this watermark was last advanced';

-- Grant permissions to expected roles
-- Note: Adjust these based on your actual database role setup
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA capture_insights TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA capture_insights TO PUBLIC;

-- Trigger to update updated_at timestamp on watermarks
CREATE OR REPLACE FUNCTION capture_insights.update_watermark_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_meta_refresh_watermarks_updated_at
    BEFORE UPDATE ON capture_insights.meta_refresh_watermarks
    FOR EACH ROW
    EXECUTE FUNCTION capture_insights.update_watermark_timestamp();

-- Trigger to update updated_at timestamp on progress chunks
CREATE OR REPLACE FUNCTION capture_insights.update_progress_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_meta_chunk_progress_updated_at
    BEFORE UPDATE ON capture_insights.meta_chunk_progress
    FOR EACH ROW
    EXECUTE FUNCTION capture_insights.update_progress_timestamp();

-- Example usage and validation queries
DO $$
BEGIN
    -- Verify tables were created successfully
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'capture_insights' 
        AND table_name = 'meta_chunk_progress'
    ) THEN
        RAISE EXCEPTION 'meta_chunk_progress table was not created';
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'capture_insights' 
        AND table_name = 'meta_refresh_watermarks'
    ) THEN
        RAISE EXCEPTION 'meta_refresh_watermarks table was not created';
    END IF;
    
    -- Verify indexes were created
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE schemaname = 'capture_insights'
        AND tablename = 'meta_chunk_progress'
        AND indexname = 'idx_meta_chunk_progress_pipeline_status'
    ) THEN
        RAISE EXCEPTION 'Progress chunks pipeline_status index was not created';
    END IF;
    
    RAISE NOTICE 'Meta tables migration completed successfully!';
    RAISE NOTICE 'Created tables: meta_chunk_progress, meta_refresh_watermarks';
    RAISE NOTICE 'Created indexes: 4 for progress chunks, 1 for watermarks';
    RAISE NOTICE 'Created triggers: automatic updated_at timestamp updates';
END;
$$;