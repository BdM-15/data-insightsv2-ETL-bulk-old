-- SQL DDL: Raw prime awards table (s1_raw layer)
-- File: sql/00_s1_raw/010_prime_awards_raw.sql
-- Purpose: Create raw staging table for USASpending prime awards (54 columns)
-- Dependencies: capture_insights schema
-- Target: PostgreSQL 14+

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS capture_insights;

-- Drop table if exists (for re-running migration)
DROP TABLE IF EXISTS capture_insights.s1_raw_usaspending_prime_awards_slimv2 CASCADE;

-- Create raw prime awards table with all 54 authoritative columns
-- Raw layer preserves all text as-is from CSV, no type coercion
CREATE TABLE capture_insights.s1_raw_usaspending_prime_awards_slimv2 (
    -- Business keys (text preserved)
    contract_transaction_unique_key text,
    contract_award_unique_key text,
    
    -- Date and fiscal fields (text preserved - will cast in interim layer)
    action_date_fiscal_year text,
    action_date text,
    
    -- Award identification
    parent_award_id_piid text,
    award_id_piid text,
    modification_number text,
    
    -- Financial fields (text preserved - will cast to numeric in interim layer) 
    federal_action_obligation text,
    total_dollars_obligated text,
    potential_total_value_of_award text,
    total_outlayed_amount_for_overall_award text,
    
    -- Performance period dates (text preserved)
    period_of_performance_start_date text,
    period_of_performance_current_end_date text,
    period_of_performance_potential_end_date text,
    ordering_period_end_date text,
    
    -- Place of performance
    primary_place_of_performance_city_name text,
    primary_place_of_performance_state_code text,
    
    -- Descriptions
    prime_award_base_transaction_description text,
    transaction_description text,
    
    -- Industry classification
    naics_code text,
    naics_description text,
    product_or_service_code text,
    product_or_service_code_description text,
    dod_acquisition_program_description text,
    
    -- Agency information
    parent_award_agency_name text,
    awarding_sub_agency_name text,
    awarding_office_name text,
    funding_agency_name text,
    funding_sub_agency_name text,
    funding_office_name text,
    
    -- Recipient information
    recipient_name text,
    recipient_uei text,
    recipient_parent_name text,
    recipient_parent_uei text,
    
    -- Solicitation details
    solicitation_date text,
    solicitation_identifier text,
    solicitation_procedures text,
    
    -- Competition details
    extent_competed text,
    type_of_set_aside text,
    fair_opportunity_limited_sources text,
    other_than_full_and_open_competition text,
    number_of_offers_received text,  -- text preserved (will cast to integer in interim)
    
    -- Contract details
    subcontracting_plan text,
    government_furnished_property text,
    type_of_contract_pricing text,
    action_type text,
    award_type text,
    type_of_idc text,
    idv_type text,
    undefinitized_action text,
    program_acronym text,
    multi_year_contract text,
    multiple_or_single_award_idv text,
    
    -- USASpending metadata
    usaspending_permalink text,
    
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

-- Create performance indexes
-- Primary business key indexes
CREATE INDEX idx_s1_raw_prime_awards_contract_transaction_key 
ON capture_insights.s1_raw_usaspending_prime_awards_slimv2(contract_transaction_unique_key);

CREATE INDEX idx_s1_raw_prime_awards_contract_award_key
ON capture_insights.s1_raw_usaspending_prime_awards_slimv2(contract_award_unique_key);

-- ETL operational indexes
CREATE INDEX idx_s1_raw_prime_awards_ingestion_ts
ON capture_insights.s1_raw_usaspending_prime_awards_slimv2(ingestion_ts);

CREATE INDEX idx_s1_raw_prime_awards_chunk_correlation
ON capture_insights.s1_raw_usaspending_prime_awards_slimv2(chunk_correlation_id);

CREATE INDEX idx_s1_raw_prime_awards_chunk_window
ON capture_insights.s1_raw_usaspending_prime_awards_slimv2(chunk_window_start, chunk_window_end);

-- Date processing indexes (on text fields for raw layer scanning)
CREATE INDEX idx_s1_raw_prime_awards_action_date_text
ON capture_insights.s1_raw_usaspending_prime_awards_slimv2(action_date);

-- Recipient analysis indexes
CREATE INDEX idx_s1_raw_prime_awards_recipient_uei
ON capture_insights.s1_raw_usaspending_prime_awards_slimv2(recipient_uei);

-- Add table and column comments
COMMENT ON TABLE capture_insights.s1_raw_usaspending_prime_awards_slimv2 IS 
'Raw staging table for USASpending prime awards data. 
All CSV fields preserved as text to avoid type coercion errors during ingestion.
Type casting and validation occurs in s2_interim layer.';

-- Key column comments
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.contract_transaction_unique_key IS 'Primary business key identifying unique prime award transaction';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.contract_award_unique_key IS 'Groups related transactions under same prime award';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.action_date IS 'Date of award action (text format from CSV)';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.prime_award_base_transaction_description IS 'Primary description field for award (may contain IGF patterns)';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.transaction_description IS 'Transaction-specific description (may contain IGF patterns)';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.fetch_date IS 'Date when this record was downloaded from USASpending API';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.ingestion_ts IS 'Timestamp when this record was loaded into database';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.chunk_window_start IS 'Start date of chunk window this record belongs to';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.chunk_window_end IS 'End date of chunk window this record belongs to';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.chunk_correlation_id IS 'Links this record to specific pipeline run';
COMMENT ON COLUMN capture_insights.s1_raw_usaspending_prime_awards_slimv2.archive_sha256 IS 'SHA256 checksum of source archive file';

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE capture_insights.s1_raw_usaspending_prime_awards_slimv2 TO PUBLIC;

-- Validation check
DO $$
DECLARE
    column_count integer;
BEGIN
    -- Count actual columns in created table (excluding ETL metadata)
    SELECT COUNT(*) INTO column_count
    FROM information_schema.columns
    WHERE table_schema = 'capture_insights'
    AND table_name = 's1_raw_usaspending_prime_awards_slimv2'
    AND column_name NOT IN ('fetch_date', 'ingestion_ts', 'created_at', 'updated_at', 
                           'chunk_window_start', 'chunk_window_end', 'chunk_correlation_id', 'archive_sha256');
    
    -- Should have exactly 54 data columns from USASpending API
    IF column_count != 54 THEN
        RAISE EXCEPTION 'Expected 54 USASpending data columns, found %', column_count;
    END IF;
    
    RAISE NOTICE 'Prime awards raw table created successfully!';
    RAISE NOTICE 'Data columns: % (54 expected from USASpending)', column_count;
    RAISE NOTICE 'ETL metadata columns: 8 (fetch_date, ingestion_ts, created_at, updated_at, chunk fields)';
    RAISE NOTICE 'Total columns: %', column_count + 8;
    RAISE NOTICE 'Indexes created: 7 (business keys, ETL operations, performance)';
END;
$$;