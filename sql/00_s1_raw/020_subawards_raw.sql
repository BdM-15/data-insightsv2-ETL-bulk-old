-- SQL DDL: Raw subawards table (s1_raw layer)  
-- File: sql/00_s1_raw/020_subawards_raw.sql
-- Purpose: Create raw staging table for USASpending subawards (columns TBD)
-- Dependencies: capture_insights schema
-- Target: PostgreSQL 14+
-- Note: Column enumeration pending discovery script - this is a placeholder structure

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS capture_insights;

-- Drop table if exists (for re-running migration)
DROP TABLE IF EXISTS capture_insights.s1_raw_usaspending_subawards_v2 CASCADE;

-- Create raw subawards table with placeholder structure
-- TODO: Update with actual column enumeration from discovery script
CREATE TABLE capture_insights.s1_raw_usaspending_subawards_v2 (
    -- Placeholder business keys (text preserved)
    prime_award_unique_key text,
    subaward_number text,
    
    -- Common fields expected to be present (based on USASpending API patterns)
    -- These are educated guesses - will be replaced with actual enumeration
    subaward_type text,
    subaward_description text,
    action_date text,
    amount text,
    recipient_name text,
    recipient_uei text,
    recipient_address_line_1 text,
    recipient_city_name text,
    recipient_state_code text,
    recipient_zip_code text,
    primary_place_of_performance_city_name text,
    primary_place_of_performance_state_code text,
    
    -- Additional placeholder columns
    -- TODO: Replace with actual API schema discovery
    placeholder_column_01 text,
    placeholder_column_02 text,
    placeholder_column_03 text,
    placeholder_column_04 text,
    placeholder_column_05 text,
    placeholder_column_06 text,
    placeholder_column_07 text,
    placeholder_column_08 text,
    placeholder_column_09 text,
    placeholder_column_10 text,
    placeholder_column_11 text,
    placeholder_column_12 text,
    placeholder_column_13 text,
    placeholder_column_14 text,
    placeholder_column_15 text,
    placeholder_column_16 text,
    placeholder_column_17 text,
    placeholder_column_18 text,
    placeholder_column_19 text,
    placeholder_column_20 text,
    -- Add more placeholders as needed based on discovery
    
    -- ETL metadata (properly typed since added by our pipeline)
    fetch_date date NOT NULL DEFAULT CURRENT_DATE,
    ingestion_ts timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    
    -- Chunk tracking metadata
    chunk_window_start date,
    chunk_window_end date,
    chunk_correlation_id uuid,
    archive_sha256 text
);

-- Create performance indexes for expected business keys
CREATE INDEX idx_s1_raw_subawards_prime_award_key 
ON capture_insights.s1_raw_usaspending_subawards_v2(prime_award_unique_key);

CREATE INDEX idx_s1_raw_subawards_composite_key
ON capture_insights.s1_raw_usaspending_subawards_v2(prime_award_unique_key, subaward_number);

-- ETL operational indexes
CREATE INDEX idx_s1_raw_subawards_ingestion_ts
ON capture_insights.s1_raw_usaspending_subawards_v2(ingestion_ts);

CREATE INDEX idx_s1_raw_subawards_chunk_correlation
ON capture_insights.s1_raw_usaspending_subawards_v2(chunk_correlation_id);

CREATE INDEX idx_s1_raw_subawards_chunk_window
ON capture_insights.s1_raw_usaspending_subawards_v2(chunk_window_start, chunk_window_end);

-- Date processing indexes
CREATE INDEX idx_s1_raw_subawards_action_date_text
ON capture_insights.s1_raw_usaspending_subawards_v2(action_date);

-- Add table and column comments
COMMENT ON TABLE capture_insights.s1_raw_usaspending_subawards_v2 IS 
'Raw staging table for USASpending subawards data (PLACEHOLDER STRUCTURE).
Column enumeration pending discovery script execution.
All CSV fields preserved as text to avoid type coercion errors during ingestion.
Type casting and validation occurs in s2_interim layer.';

-- Key column comments
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_subawards_v2.prime_award_unique_key IS 'Links subaward to parent prime award';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_subawards_v2.subaward_number IS 'Unique identifier for subaward within prime award';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_subawards_v2.fetch_date IS 'Date when this record was downloaded from USASpending API';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_subawards_v2.ingestion_ts IS 'Timestamp when this record was loaded into database';

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE capture_insights.s1_raw_usaspending_subawards_v2 TO PUBLIC;

-- Discovery reminder and validation
DO $$
BEGIN
    RAISE WARNING 'PLACEHOLDER TABLE CREATED - REQUIRES COLUMN ENUMERATION';
    RAISE WARNING 'TODO: Execute discovery script to enumerate actual subawards API schema';
    RAISE WARNING 'TODO: Replace placeholder columns with actual field names and types';
    RAISE WARNING 'TODO: Update indexes based on actual business keys discovered';
    RAISE WARNING 'Current structure is for testing/scaffolding only';
    
    RAISE NOTICE 'Subawards raw table placeholder created successfully!';
    RAISE NOTICE 'Business key indexes: 2 (prime_award_unique_key, composite)';
    RAISE NOTICE 'ETL operational indexes: 3 (ingestion_ts, chunk tracking)';
    RAISE NOTICE 'Date processing indexes: 1 (action_date)';
    RAISE NOTICE 'Total placeholder data columns: 33 (requires discovery update)';
END;
$$;

-- Add a reminder function that can be called during discovery
CREATE OR REPLACE FUNCTION capture_insights.update_subawards_schema_reminder()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN 'REMINDER: Update sql/00_s1_raw/020_subawards_raw.sql with actual column enumeration from discovery script';
END;
$$;

COMMENT ON FUNCTION capture_insights.update_subawards_schema_reminder() IS 
'Reminder function - call during discovery phase to update subawards table schema';