-- SQL Transform: Interim subawards table (s2_interim layer)  
-- File: sql/10_s2_interim/020_subawards_interim.sql
-- Purpose: Transform raw subawards with type casting, cleansing, and enrichment
-- Dependencies: s1_raw.usaspending_subawards_v2, util.clean_description function  
-- Target: PostgreSQL 14+
-- Note: Column transformations pending discovery - this is a placeholder structure

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS capture_insights;

-- Drop table if exists (for re-running)
DROP TABLE IF EXISTS capture_insights.s2_interim_usaspending_subawards CASCADE;

-- Create interim subawards table with placeholder transformations
-- TODO: Update with actual column enumeration and transformations from discovery script
CREATE TABLE capture_insights.s2_interim_usaspending_subawards AS
SELECT 
    -- Expected business keys (preserved and trimmed)
    trim(prime_award_unique_key) as prime_award_unique_key,
    trim(subaward_number) as subaward_number,
    
    -- Expected common fields with safe transformations
    trim(subaward_type) as subaward_type,
    
    -- Description field with semantic cleaning (if available)
    CASE 
        WHEN subaward_description IS NOT NULL AND trim(subaward_description) != ''
        THEN util.clean_description(trim(subaward_description))
        ELSE NULL
    END as semantic_description,
    
    trim(subaward_description) as subaward_description,
    
    -- Date casting (safe with NULL on invalid)
    CASE 
        WHEN action_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN action_date::date 
        ELSE NULL 
    END as action_date,
    
    -- Derived fiscal calculations (if action_date is valid)
    CASE 
        WHEN action_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN EXTRACT(YEAR FROM (action_date::date + INTERVAL '3 months'))::integer
        ELSE NULL
    END as fiscal_year,
    
    CASE 
        WHEN action_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN EXTRACT(QUARTER FROM (action_date::date + INTERVAL '3 months'))::integer
        ELSE NULL
    END as fiscal_quarter,
    
    -- Amount casting (safe numeric conversion)
    CASE 
        WHEN amount ~ '^-?\d+\.?\d*$' 
        THEN amount::numeric 
        ELSE NULL 
    END as amount,
    
    -- Recipient information (trimmed)
    trim(recipient_name) as recipient_name,
    trim(recipient_uei) as recipient_uei,
    trim(recipient_address_line_1) as recipient_address_line_1,
    trim(recipient_city_name) as recipient_city_name,
    trim(recipient_state_code) as recipient_state_code,
    trim(recipient_zip_code) as recipient_zip_code,
    
    -- Place of performance (trimmed)
    trim(primary_place_of_performance_city_name) as primary_place_of_performance_city_name,
    trim(primary_place_of_performance_state_code) as primary_place_of_performance_state_code,
    
    -- Placeholder transformations for discovered columns
    -- TODO: Replace with actual field transformations from discovery
    trim(placeholder_column_01) as placeholder_column_01_cleaned,
    trim(placeholder_column_02) as placeholder_column_02_cleaned,
    trim(placeholder_column_03) as placeholder_column_03_cleaned,
    trim(placeholder_column_04) as placeholder_column_04_cleaned,
    trim(placeholder_column_05) as placeholder_column_05_cleaned,
    trim(placeholder_column_06) as placeholder_column_06_cleaned,
    trim(placeholder_column_07) as placeholder_column_07_cleaned,
    trim(placeholder_column_08) as placeholder_column_08_cleaned,
    trim(placeholder_column_09) as placeholder_column_09_cleaned,
    trim(placeholder_column_10) as placeholder_column_10_cleaned,
    trim(placeholder_column_11) as placeholder_column_11_cleaned,
    trim(placeholder_column_12) as placeholder_column_12_cleaned,
    trim(placeholder_column_13) as placeholder_column_13_cleaned,
    trim(placeholder_column_14) as placeholder_column_14_cleaned,
    trim(placeholder_column_15) as placeholder_column_15_cleaned,
    trim(placeholder_column_16) as placeholder_column_16_cleaned,
    trim(placeholder_column_17) as placeholder_column_17_cleaned,
    trim(placeholder_column_18) as placeholder_column_18_cleaned,
    trim(placeholder_column_19) as placeholder_column_19_cleaned,
    trim(placeholder_column_20) as placeholder_column_20_cleaned,
    
    -- Derived last_modified_date (use ingestion_ts as substitute)
    -- TODO: Update this when actual last_modified_date field is discovered in API
    ingestion_ts as last_modified_date,
    
    -- ETL metadata (preserved)
    fetch_date,
    ingestion_ts,
    chunk_window_start,
    chunk_window_end,
    chunk_correlation_id,
    archive_sha256,
    
    -- Audit timestamps
    now() as created_at,
    now() as updated_at

FROM capture_insights.s1_raw_usaspending_subawards_v2;

-- Create performance indexes on interim table
-- Primary business key indexes
CREATE INDEX idx_s2_interim_subawards_prime_award_key 
ON capture_insights.s2_interim_usaspending_subawards(prime_award_unique_key);

CREATE UNIQUE INDEX idx_s2_interim_subawards_composite_key
ON capture_insights.s2_interim_usaspending_subawards(prime_award_unique_key, subaward_number);

-- Date and fiscal indexes (now properly typed)
CREATE INDEX idx_s2_interim_subawards_action_date
ON capture_insights.s2_interim_usaspending_subawards(action_date);

CREATE INDEX idx_s2_interim_subawards_fiscal_year
ON capture_insights.s2_interim_usaspending_subawards(fiscal_year);

CREATE INDEX idx_s2_interim_subawards_last_modified_date
ON capture_insights.s2_interim_usaspending_subawards(last_modified_date);

-- Financial analysis indexes
CREATE INDEX idx_s2_interim_subawards_amount
ON capture_insights.s2_interim_usaspending_subawards(amount) 
WHERE amount IS NOT NULL;

-- Recipient analysis indexes  
CREATE INDEX idx_s2_interim_subawards_recipient_uei
ON capture_insights.s2_interim_usaspending_subawards(recipient_uei);

-- ETL operational indexes
CREATE INDEX idx_s2_interim_subawards_chunk_correlation
ON capture_insights.s2_interim_usaspending_subawards(chunk_correlation_id);

-- Geographic analysis indexes
CREATE INDEX idx_s2_interim_subawards_performance_state
ON capture_insights.s2_interim_usaspending_subawards(primary_place_of_performance_state_code);

CREATE INDEX idx_s2_interim_subawards_recipient_state
ON capture_insights.s2_interim_usaspending_subawards(recipient_state_code);

-- Add table and column comments
COMMENT ON TABLE capture_insights.s2_interim_usaspending_subawards IS 
'Interim layer for subawards with type casting, cleansing, and semantic enrichment (PLACEHOLDER STRUCTURE).
Column transformations pending discovery script execution.
All numeric/date fields safely cast with NULL on invalid values.
Description fields cleaned of IGF patterns using util.clean_description function.';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_subawards.semantic_description IS 
'Cleaned subaward description with IGF patterns removed (if description field exists in API)';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_subawards.fiscal_year IS 
'Fiscal year derived from action_date (October start)';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_subawards.fiscal_quarter IS 
'Fiscal quarter derived from action_date (Q1=Oct-Dec, Q2=Jan-Mar, etc.)';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_subawards.last_modified_date IS 
'Last modification timestamp - currently using ingestion_ts as substitute (TODO: use actual API field when available)';

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE capture_insights.s2_interim_usaspending_subawards TO PUBLIC;

-- Discovery reminder and validation
DO $$
DECLARE
    raw_count bigint;
    interim_count bigint;
    null_action_dates bigint;
    null_amounts bigint;
    cleaned_descriptions bigint;
BEGIN
    RAISE WARNING 'PLACEHOLDER TABLE CREATED - REQUIRES COLUMN ENUMERATION';
    RAISE WARNING 'TODO: Execute discovery script to enumerate actual subawards API schema';
    RAISE WARNING 'TODO: Replace placeholder column transformations with actual field logic';
    RAISE WARNING 'TODO: Update indexes based on actual business keys and analysis needs';
    RAISE WARNING 'Current structure is for testing/scaffolding only';
    
    -- Get record counts if raw table has data
    BEGIN
        SELECT COUNT(*) INTO raw_count FROM capture_insights.s1_raw_usaspending_subawards_v2;
        SELECT COUNT(*) INTO interim_count FROM capture_insights.s2_interim_usaspending_subawards;
        
        -- Get quality metrics for expected fields
        SELECT COUNT(*) INTO null_action_dates 
        FROM capture_insights.s2_interim_usaspending_subawards 
        WHERE action_date IS NULL;
        
        SELECT COUNT(*) INTO null_amounts 
        FROM capture_insights.s2_interim_usaspending_subawards 
        WHERE amount IS NULL;
        
        SELECT COUNT(*) INTO cleaned_descriptions 
        FROM capture_insights.s2_interim_usaspending_subawards 
        WHERE semantic_description IS NOT NULL;
        
        RAISE NOTICE 'Subawards interim table placeholder created successfully!';
        RAISE NOTICE 'Records: % raw -> % interim', raw_count, interim_count;
        RAISE NOTICE 'Expected data quality: % null action dates, % null amounts', null_action_dates, null_amounts;
        RAISE NOTICE 'Semantic cleaning: % descriptions available for cleaning', cleaned_descriptions;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Subawards interim table placeholder created (no source data yet)';
    END;
    
    RAISE NOTICE 'Indexes created: 10 (business keys, dates, financial, recipient, ETL, geographic)';
END;
$$;

-- Add a reminder function that can be called during discovery
CREATE OR REPLACE FUNCTION capture_insights.update_subawards_interim_reminder()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN 'REMINDER: Update sql/10_s2_interim/020_subawards_interim.sql with actual column transformations from discovery script';
END;
$$;

COMMENT ON FUNCTION capture_insights.update_subawards_interim_reminder() IS 
'Reminder function - call during discovery phase to update subawards interim transformations';