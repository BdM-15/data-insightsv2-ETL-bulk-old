-- SQL Transform: Processed subawards table (s3_processed layer)
-- File: sql/20_s3_processed/020_subawards_processed.sql
-- Purpose: Create deduplicated subawards with UPSERT logic and audit fields
-- Dependencies: s2_interim.usaspending_subawards
-- Target: PostgreSQL 14+

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS capture_insights;

-- Drop table if exists (for re-running)
DROP TABLE IF EXISTS capture_insights.s3_processed_usaspending_subawards CASCADE;

-- Create processed subawards table structure
CREATE TABLE capture_insights.s3_processed_usaspending_subawards (
    -- Business keys (preserved from interim)
    subaward_report_key text PRIMARY KEY,
    prime_award_report_key text,
    
    -- Parent award linkage
    prime_award_unique_key text,
    prime_award_piid text,
    
    -- Date and fiscal fields (typed from interim)
    action_date_fiscal_year integer,
    action_date date,
    fiscal_year integer,
    
    -- Award identification
    subaward_number text,
    subaward_amount numeric,
    
    -- Performance period dates
    subaward_date date,
    
    -- Place of performance
    place_of_performance_city_name text,
    place_of_performance_state_code text,
    place_of_performance_zip_code text,
    place_of_performance_country_code text,
    
    -- Descriptions (cleaned from interim)
    subaward_description text,
    semantic_description text NOT NULL,
    
    -- Industry classification
    naics_code text,
    naics_description text,
    
    -- Prime recipient information (parent award)
    prime_recipient_name text,
    prime_recipient_uei text,
    
    -- Subaward recipient information
    sub_recipient_name text,
    sub_recipient_uei text,
    sub_recipient_parent_name text,
    sub_recipient_parent_uei text,
    sub_recipient_legal_organization_name text,
    sub_recipient_doing_business_as_name text,
    
    -- Address information for subaward recipient
    sub_recipient_address_line_1 text,
    sub_recipient_address_line_2 text,
    sub_recipient_city_name text,
    sub_recipient_state_code text,
    sub_recipient_zip_code text,
    sub_recipient_country_code text,
    
    -- Business type information
    sub_recipient_business_type_description text,
    
    -- USASpending metadata
    usaspending_permalink text,
    
    -- Deduplication control fields
    last_modified_date timestamptz NOT NULL,
    source_ingestion_ts timestamptz NOT NULL,
    source_chunk_correlation_id uuid,
    source_archive_sha256 text,
    
    -- Deduplication audit fields
    dedupe_rank integer NOT NULL DEFAULT 1,
    dedupe_total_duplicates integer NOT NULL DEFAULT 1,
    dedupe_precedence_reason text NOT NULL,
    is_canonical boolean NOT NULL DEFAULT true,
    
    -- Processed layer timestamps
    processed_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

-- Initial population with deduplication logic
-- UPSERT key: subaward_report_key
-- Keep latest by (last_modified_date DESC, action_date DESC, ingestion_ts DESC)
INSERT INTO capture_insights.s3_processed_usaspending_subawards
SELECT DISTINCT ON (subaward_report_key)
    -- Business keys
    subaward_report_key,
    prime_award_report_key,
    
    -- Parent award linkage
    prime_award_unique_key,
    prime_award_piid,
    
    -- Date and fiscal fields
    action_date_fiscal_year,
    action_date,
    fiscal_year,
    
    -- Award identification
    subaward_number,
    subaward_amount,
    
    -- Performance period dates
    subaward_date,
    
    -- Place of performance
    place_of_performance_city_name,
    place_of_performance_state_code,
    place_of_performance_zip_code,
    place_of_performance_country_code,
    
    -- Descriptions
    subaward_description,
    COALESCE(semantic_description, 'No description available') as semantic_description,
    
    -- Industry classification
    naics_code,
    naics_description,
    
    -- Prime recipient information
    prime_recipient_name,
    prime_recipient_uei,
    
    -- Subaward recipient information
    sub_recipient_name,
    sub_recipient_uei,
    sub_recipient_parent_name,
    sub_recipient_parent_uei,
    sub_recipient_legal_organization_name,
    sub_recipient_doing_business_as_name,
    
    -- Address information for subaward recipient
    sub_recipient_address_line_1,
    sub_recipient_address_line_2,
    sub_recipient_city_name,
    sub_recipient_state_code,
    sub_recipient_zip_code,
    sub_recipient_country_code,
    
    -- Business type information
    sub_recipient_business_type_description,
    
    -- USASpending metadata
    usaspending_permalink,
    
    -- Deduplication control fields
    last_modified_date,
    ingestion_ts as source_ingestion_ts,
    chunk_correlation_id as source_chunk_correlation_id,
    archive_sha256 as source_archive_sha256,
    
    -- Deduplication audit fields (calculated)
    ROW_NUMBER() OVER (
        PARTITION BY subaward_report_key 
        ORDER BY last_modified_date DESC NULLS LAST, 
                 action_date DESC NULLS LAST, 
                 ingestion_ts DESC
    ) as dedupe_rank,
    
    COUNT(*) OVER (PARTITION BY subaward_report_key) as dedupe_total_duplicates,
    
    CASE 
        WHEN ROW_NUMBER() OVER (
            PARTITION BY subaward_report_key 
            ORDER BY last_modified_date DESC NULLS LAST, 
                     action_date DESC NULLS LAST, 
                     ingestion_ts DESC
        ) = 1 THEN 'latest_by_precedence_rule'
        ELSE 'superseded_by_newer_record'
    END as dedupe_precedence_reason,
    
    -- Only the first ranked record is canonical
    ROW_NUMBER() OVER (
        PARTITION BY subaward_report_key 
        ORDER BY last_modified_date DESC NULLS LAST, 
                 action_date DESC NULLS LAST, 
                 ingestion_ts DESC
    ) = 1 as is_canonical,
    
    -- Processing timestamp
    now() as processed_at,
    now() as created_at,
    now() as updated_at

FROM capture_insights.s2_interim_usaspending_subawards
ORDER BY subaward_report_key,
         last_modified_date DESC NULLS LAST,
         action_date DESC NULLS LAST,
         ingestion_ts DESC;

-- Create performance indexes on processed table
-- Primary business key (already have PRIMARY KEY constraint)
CREATE INDEX idx_s3_processed_subawards_prime_award_key
ON capture_insights.s3_processed_usaspending_subawards(prime_award_unique_key);

CREATE INDEX idx_s3_processed_subawards_prime_report_key
ON capture_insights.s3_processed_usaspending_subawards(prime_award_report_key);

-- Deduplication analysis indexes
CREATE INDEX idx_s3_processed_subawards_canonical
ON capture_insights.s3_processed_usaspending_subawards(is_canonical, subaward_report_key);

CREATE INDEX idx_s3_processed_subawards_duplicates
ON capture_insights.s3_processed_usaspending_subawards(dedupe_total_duplicates DESC)
WHERE dedupe_total_duplicates > 1;

-- Date and precedence indexes
CREATE INDEX idx_s3_processed_subawards_last_modified
ON capture_insights.s3_processed_usaspending_subawards(last_modified_date DESC);

CREATE INDEX idx_s3_processed_subawards_action_date
ON capture_insights.s3_processed_usaspending_subawards(action_date DESC);

CREATE INDEX idx_s3_processed_subawards_processed_at
ON capture_insights.s3_processed_usaspending_subawards(processed_at DESC);

-- Financial analysis indexes (canonical records only for performance)
CREATE INDEX idx_s3_processed_subawards_amount_canonical
ON capture_insights.s3_processed_usaspending_subawards(subaward_amount DESC)
WHERE is_canonical = true AND subaward_amount IS NOT NULL;

-- Recipient analysis indexes (canonical records only)
CREATE INDEX idx_s3_processed_subawards_sub_recipient_uei_canonical
ON capture_insights.s3_processed_usaspending_subawards(sub_recipient_uei)
WHERE is_canonical = true;

CREATE INDEX idx_s3_processed_subawards_prime_recipient_uei_canonical
ON capture_insights.s3_processed_usaspending_subawards(prime_recipient_uei)
WHERE is_canonical = true;

-- Geographic analysis indexes (canonical records only)
CREATE INDEX idx_s3_processed_subawards_performance_state_canonical
ON capture_insights.s3_processed_usaspending_subawards(place_of_performance_state_code)
WHERE is_canonical = true;

CREATE INDEX idx_s3_processed_subawards_recipient_state_canonical
ON capture_insights.s3_processed_usaspending_subawards(sub_recipient_state_code)
WHERE is_canonical = true;

-- Industry analysis indexes (canonical records only)
CREATE INDEX idx_s3_processed_subawards_naics_canonical
ON capture_insights.s3_processed_usaspending_subawards(naics_code)
WHERE is_canonical = true;

-- Source tracking indexes
CREATE INDEX idx_s3_processed_subawards_source_correlation
ON capture_insights.s3_processed_usaspending_subawards(source_chunk_correlation_id);

-- Add table and column comments
COMMENT ON TABLE capture_insights.s3_processed_usaspending_subawards IS 
'Processed layer for subawards with deduplication and canonical record identification.
UPSERT key: subaward_report_key
Precedence: last_modified_date DESC, action_date DESC, ingestion_ts DESC
Maintains original subaward grain with deduplication audit fields.';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.subaward_report_key IS 
'Primary deduplication key - unique subaward identifier';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.semantic_description IS 
'Cleaned description from interim layer with IGF patterns removed';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.last_modified_date IS 
'Primary precedence field for deduplication - latest wins';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.dedupe_rank IS 
'Rank within duplicate group (1 = most recent, canonical)';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.dedupe_total_duplicates IS 
'Total number of records sharing the same subaward_report_key';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.dedupe_precedence_reason IS 
'Explanation of why this record was ranked as it was in deduplication';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.is_canonical IS 
'True for the single authoritative record per subaward_report_key (dedupe_rank = 1)';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_subawards.source_ingestion_ts IS 
'Original ingestion timestamp from interim layer - used for tie-breaking';

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE capture_insights.s3_processed_usaspending_subawards TO PUBLIC;

-- Create UPSERT function for incremental processing
CREATE OR REPLACE FUNCTION capture_insights.upsert_subawards_processed()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Handle INSERT/UPDATE with deduplication logic
    INSERT INTO capture_insights.s3_processed_usaspending_subawards (
        subaward_report_key,
        prime_award_report_key,
        prime_award_unique_key,
        prime_award_piid,
        action_date_fiscal_year,
        action_date,
        fiscal_year,
        subaward_number,
        subaward_amount,
        subaward_date,
        place_of_performance_city_name,
        place_of_performance_state_code,
        place_of_performance_zip_code,
        place_of_performance_country_code,
        subaward_description,
        semantic_description,
        naics_code,
        naics_description,
        prime_recipient_name,
        prime_recipient_uei,
        sub_recipient_name,
        sub_recipient_uei,
        sub_recipient_parent_name,
        sub_recipient_parent_uei,
        sub_recipient_legal_organization_name,
        sub_recipient_doing_business_as_name,
        sub_recipient_address_line_1,
        sub_recipient_address_line_2,
        sub_recipient_city_name,
        sub_recipient_state_code,
        sub_recipient_zip_code,
        sub_recipient_country_code,
        sub_recipient_business_type_description,
        usaspending_permalink,
        last_modified_date,
        source_ingestion_ts,
        source_chunk_correlation_id,
        source_archive_sha256,
        dedupe_rank,
        dedupe_total_duplicates,
        dedupe_precedence_reason,
        is_canonical
    )
    VALUES (
        NEW.subaward_report_key,
        NEW.prime_award_report_key,
        NEW.prime_award_unique_key,
        NEW.prime_award_piid,
        NEW.action_date_fiscal_year,
        NEW.action_date,
        NEW.fiscal_year,
        NEW.subaward_number,
        NEW.subaward_amount,
        NEW.subaward_date,
        NEW.place_of_performance_city_name,
        NEW.place_of_performance_state_code,
        NEW.place_of_performance_zip_code,
        NEW.place_of_performance_country_code,
        NEW.subaward_description,
        COALESCE(NEW.semantic_description, 'No description available'),
        NEW.naics_code,
        NEW.naics_description,
        NEW.prime_recipient_name,
        NEW.prime_recipient_uei,
        NEW.sub_recipient_name,
        NEW.sub_recipient_uei,
        NEW.sub_recipient_parent_name,
        NEW.sub_recipient_parent_uei,
        NEW.sub_recipient_legal_organization_name,
        NEW.sub_recipient_doing_business_as_name,
        NEW.sub_recipient_address_line_1,
        NEW.sub_recipient_address_line_2,
        NEW.sub_recipient_city_name,
        NEW.sub_recipient_state_code,
        NEW.sub_recipient_zip_code,
        NEW.sub_recipient_country_code,
        NEW.sub_recipient_business_type_description,
        NEW.usaspending_permalink,
        NEW.last_modified_date,
        NEW.ingestion_ts,
        NEW.chunk_correlation_id,
        NEW.archive_sha256,
        1,  -- dedupe_rank (will be recalculated)
        1,  -- dedupe_total_duplicates (will be recalculated)
        'latest_by_precedence_rule',  -- will be recalculated
        true  -- is_canonical (will be recalculated)
    )
    ON CONFLICT (subaward_report_key) 
    DO UPDATE SET
        prime_award_report_key = EXCLUDED.prime_award_report_key,
        prime_award_unique_key = EXCLUDED.prime_award_unique_key,
        prime_award_piid = EXCLUDED.prime_award_piid,
        action_date_fiscal_year = EXCLUDED.action_date_fiscal_year,
        action_date = EXCLUDED.action_date,
        fiscal_year = EXCLUDED.fiscal_year,
        subaward_number = EXCLUDED.subaward_number,
        subaward_amount = EXCLUDED.subaward_amount,
        subaward_date = EXCLUDED.subaward_date,
        place_of_performance_city_name = EXCLUDED.place_of_performance_city_name,
        place_of_performance_state_code = EXCLUDED.place_of_performance_state_code,
        place_of_performance_zip_code = EXCLUDED.place_of_performance_zip_code,
        place_of_performance_country_code = EXCLUDED.place_of_performance_country_code,
        subaward_description = EXCLUDED.subaward_description,
        semantic_description = EXCLUDED.semantic_description,
        naics_code = EXCLUDED.naics_code,
        naics_description = EXCLUDED.naics_description,
        prime_recipient_name = EXCLUDED.prime_recipient_name,
        prime_recipient_uei = EXCLUDED.prime_recipient_uei,
        sub_recipient_name = EXCLUDED.sub_recipient_name,
        sub_recipient_uei = EXCLUDED.sub_recipient_uei,
        sub_recipient_parent_name = EXCLUDED.sub_recipient_parent_name,
        sub_recipient_parent_uei = EXCLUDED.sub_recipient_parent_uei,
        sub_recipient_legal_organization_name = EXCLUDED.sub_recipient_legal_organization_name,
        sub_recipient_doing_business_as_name = EXCLUDED.sub_recipient_doing_business_as_name,
        sub_recipient_address_line_1 = EXCLUDED.sub_recipient_address_line_1,
        sub_recipient_address_line_2 = EXCLUDED.sub_recipient_address_line_2,
        sub_recipient_city_name = EXCLUDED.sub_recipient_city_name,
        sub_recipient_state_code = EXCLUDED.sub_recipient_state_code,
        sub_recipient_zip_code = EXCLUDED.sub_recipient_zip_code,
        sub_recipient_country_code = EXCLUDED.sub_recipient_country_code,
        sub_recipient_business_type_description = EXCLUDED.sub_recipient_business_type_description,
        usaspending_permalink = EXCLUDED.usaspending_permalink,
        last_modified_date = GREATEST(
            capture_insights.s3_processed_usaspending_subawards.last_modified_date,
            EXCLUDED.last_modified_date
        ),
        source_ingestion_ts = EXCLUDED.source_ingestion_ts,
        source_chunk_correlation_id = EXCLUDED.source_chunk_correlation_id,
        source_archive_sha256 = EXCLUDED.source_archive_sha256,
        updated_at = now()
        WHERE EXCLUDED.last_modified_date >= capture_insights.s3_processed_usaspending_subawards.last_modified_date
           OR EXCLUDED.action_date >= capture_insights.s3_processed_usaspending_subawards.action_date
           OR EXCLUDED.source_ingestion_ts > capture_insights.s3_processed_usaspending_subawards.source_ingestion_ts;
    
    RETURN NULL;  -- For AFTER trigger
END;
$$;

COMMENT ON FUNCTION capture_insights.upsert_subawards_processed() IS 
'UPSERT function for incremental processing of subawards with precedence-based conflict resolution';

-- Validation and statistics
DO $$
DECLARE
    interim_count bigint;
    processed_count bigint;
    canonical_count bigint;
    duplicate_groups bigint;
    max_duplicates integer;
BEGIN
    -- Get record counts
    SELECT COUNT(*) INTO interim_count FROM capture_insights.s2_interim_usaspending_subawards;
    SELECT COUNT(*) INTO processed_count FROM capture_insights.s3_processed_usaspending_subawards;
    SELECT COUNT(*) INTO canonical_count 
    FROM capture_insights.s3_processed_usaspending_subawards 
    WHERE is_canonical = true;
    
    -- Get deduplication statistics
    SELECT COUNT(*) INTO duplicate_groups 
    FROM capture_insights.s3_processed_usaspending_subawards 
    WHERE dedupe_total_duplicates > 1;
    
    SELECT COALESCE(MAX(dedupe_total_duplicates), 0) INTO max_duplicates 
    FROM capture_insights.s3_processed_usaspending_subawards;
    
    RAISE NOTICE 'Subawards processed table created successfully!';
    RAISE NOTICE 'Records: % interim -> % processed (% canonical)', interim_count, processed_count, canonical_count;
    RAISE NOTICE 'Deduplication: % records in duplicate groups, max % duplicates per key', duplicate_groups, max_duplicates;
    RAISE NOTICE 'Indexes created: 12 (primary key, parent linkage, dedup analysis, precedence, financial, recipient, geographic, industry, source tracking)';
    RAISE NOTICE 'UPSERT function created: upsert_subawards_processed() for incremental processing';
END;
$$;