-- SQL Transform: Processed prime awards table (s3_processed layer)
-- File: sql/20_s3_processed/010_prime_awards_processed.sql
-- Purpose: Create deduplicated prime awards with UPSERT logic and audit fields
-- Dependencies: s2_interim.usaspending_prime_awards
-- Target: PostgreSQL 14+

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS capture_insights;

-- Drop table if exists (for re-running)
DROP TABLE IF EXISTS capture_insights.s3_processed_usaspending_prime_awards CASCADE;

-- Create processed prime awards table structure
CREATE TABLE capture_insights.s3_processed_usaspending_prime_awards (
    -- Business keys (preserved from interim)
    contract_transaction_unique_key text PRIMARY KEY,
    contract_award_unique_key text NOT NULL,
    
    -- Date and fiscal fields (typed from interim)
    action_date_fiscal_year integer,
    action_date date,
    fiscal_year integer,
    fiscal_quarter integer,
    
    -- Award identification
    parent_award_id_piid text,
    award_id_piid text,
    modification_number text,
    
    -- Financial fields (numeric from interim)
    federal_action_obligation numeric,
    total_dollars_obligated numeric,
    potential_total_value_of_award numeric,
    total_outlayed_amount_for_overall_award numeric,
    
    -- Performance period dates
    period_of_performance_start_date date,
    period_of_performance_current_end_date date,
    period_of_performance_potential_end_date date,
    ordering_period_end_date date,
    
    -- Place of performance
    primary_place_of_performance_city_name text,
    primary_place_of_performance_state_code text,
    
    -- Descriptions (cleaned from interim)
    prime_award_base_transaction_description text,
    transaction_description text,
    semantic_description text NOT NULL,
    
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
    solicitation_date date,
    solicitation_identifier text,
    solicitation_procedures text,
    
    -- Competition details
    extent_competed text,
    type_of_set_aside text,
    fair_opportunity_limited_sources text,
    other_than_full_and_open_competition text,
    number_of_offers_received integer,
    
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
-- UPSERT key: contract_transaction_unique_key
-- Keep latest by (last_modified_date DESC, action_date DESC, ingestion_ts DESC)
INSERT INTO capture_insights.s3_processed_usaspending_prime_awards
SELECT DISTINCT ON (contract_transaction_unique_key)
    -- Business keys
    contract_transaction_unique_key,
    contract_award_unique_key,
    
    -- Date and fiscal fields
    action_date_fiscal_year,
    action_date,
    fiscal_year,
    fiscal_quarter,
    
    -- Award identification
    parent_award_id_piid,
    award_id_piid,
    modification_number,
    
    -- Financial fields
    federal_action_obligation,
    total_dollars_obligated,
    potential_total_value_of_award,
    total_outlayed_amount_for_overall_award,
    
    -- Performance period dates
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    period_of_performance_potential_end_date,
    ordering_period_end_date,
    
    -- Place of performance
    primary_place_of_performance_city_name,
    primary_place_of_performance_state_code,
    
    -- Descriptions
    prime_award_base_transaction_description,
    transaction_description,
    COALESCE(semantic_description, 'No description available') as semantic_description,
    
    -- Industry classification
    naics_code,
    naics_description,
    product_or_service_code,
    product_or_service_code_description,
    dod_acquisition_program_description,
    
    -- Agency information
    parent_award_agency_name,
    awarding_sub_agency_name,
    awarding_office_name,
    funding_agency_name,
    funding_sub_agency_name,
    funding_office_name,
    
    -- Recipient information
    recipient_name,
    recipient_uei,
    recipient_parent_name,
    recipient_parent_uei,
    
    -- Solicitation details
    solicitation_date,
    solicitation_identifier,
    solicitation_procedures,
    
    -- Competition details
    extent_competed,
    type_of_set_aside,
    fair_opportunity_limited_sources,
    other_than_full_and_open_competition,
    number_of_offers_received,
    
    -- Contract details
    subcontracting_plan,
    government_furnished_property,
    type_of_contract_pricing,
    action_type,
    award_type,
    type_of_idc,
    idv_type,
    undefinitized_action,
    program_acronym,
    multi_year_contract,
    multiple_or_single_award_idv,
    
    -- USASpending metadata
    usaspending_permalink,
    
    -- Deduplication control fields
    last_modified_date,
    ingestion_ts as source_ingestion_ts,
    chunk_correlation_id as source_chunk_correlation_id,
    archive_sha256 as source_archive_sha256,
    
    -- Deduplication audit fields (calculated)
    ROW_NUMBER() OVER (
        PARTITION BY contract_transaction_unique_key 
        ORDER BY last_modified_date DESC NULLS LAST, 
                 action_date DESC NULLS LAST, 
                 ingestion_ts DESC
    ) as dedupe_rank,
    
    COUNT(*) OVER (PARTITION BY contract_transaction_unique_key) as dedupe_total_duplicates,
    
    CASE 
        WHEN ROW_NUMBER() OVER (
            PARTITION BY contract_transaction_unique_key 
            ORDER BY last_modified_date DESC NULLS LAST, 
                     action_date DESC NULLS LAST, 
                     ingestion_ts DESC
        ) = 1 THEN 'latest_by_precedence_rule'
        ELSE 'superseded_by_newer_record'
    END as dedupe_precedence_reason,
    
    -- Only the first ranked record is canonical
    ROW_NUMBER() OVER (
        PARTITION BY contract_transaction_unique_key 
        ORDER BY last_modified_date DESC NULLS LAST, 
                 action_date DESC NULLS LAST, 
                 ingestion_ts DESC
    ) = 1 as is_canonical,
    
    -- Processing timestamp
    now() as processed_at,
    now() as created_at,
    now() as updated_at

FROM capture_insights.s2_interim_usaspending_prime_awards
ORDER BY contract_transaction_unique_key,
         last_modified_date DESC NULLS LAST,
         action_date DESC NULLS LAST,
         ingestion_ts DESC;

-- Create performance indexes on processed table
-- Primary business key (already have PRIMARY KEY constraint)
CREATE INDEX idx_s3_processed_prime_awards_contract_award_key
ON capture_insights.s3_processed_usaspending_prime_awards(contract_award_unique_key);

-- Deduplication analysis indexes
CREATE INDEX idx_s3_processed_prime_awards_canonical
ON capture_insights.s3_processed_usaspending_prime_awards(is_canonical, contract_transaction_unique_key);

CREATE INDEX idx_s3_processed_prime_awards_duplicates
ON capture_insights.s3_processed_usaspending_prime_awards(dedupe_total_duplicates DESC)
WHERE dedupe_total_duplicates > 1;

-- Date and precedence indexes
CREATE INDEX idx_s3_processed_prime_awards_last_modified
ON capture_insights.s3_processed_usaspending_prime_awards(last_modified_date DESC);

CREATE INDEX idx_s3_processed_prime_awards_action_date
ON capture_insights.s3_processed_usaspending_prime_awards(action_date DESC);

CREATE INDEX idx_s3_processed_prime_awards_processed_at
ON capture_insights.s3_processed_usaspending_prime_awards(processed_at DESC);

-- Financial analysis indexes (canonical records only for performance)
CREATE INDEX idx_s3_processed_prime_awards_federal_obligation_canonical
ON capture_insights.s3_processed_usaspending_prime_awards(federal_action_obligation DESC)
WHERE is_canonical = true AND federal_action_obligation IS NOT NULL;

-- Recipient analysis indexes (canonical records only)
CREATE INDEX idx_s3_processed_prime_awards_recipient_uei_canonical
ON capture_insights.s3_processed_usaspending_prime_awards(recipient_uei)
WHERE is_canonical = true;

-- Agency analysis indexes (canonical records only)
CREATE INDEX idx_s3_processed_prime_awards_awarding_agency_canonical
ON capture_insights.s3_processed_usaspending_prime_awards(parent_award_agency_name)
WHERE is_canonical = true;

-- Geographic analysis indexes (canonical records only)
CREATE INDEX idx_s3_processed_prime_awards_performance_state_canonical
ON capture_insights.s3_processed_usaspending_prime_awards(primary_place_of_performance_state_code)
WHERE is_canonical = true;

-- Industry analysis indexes (canonical records only)
CREATE INDEX idx_s3_processed_prime_awards_naics_canonical
ON capture_insights.s3_processed_usaspending_prime_awards(naics_code)
WHERE is_canonical = true;

-- Source tracking indexes
CREATE INDEX idx_s3_processed_prime_awards_source_correlation
ON capture_insights.s3_processed_usaspending_prime_awards(source_chunk_correlation_id);

-- Add table and column comments
COMMENT ON TABLE capture_insights.s3_processed_usaspending_prime_awards IS 
'Processed layer for prime awards with deduplication and canonical record identification.
UPSERT key: contract_transaction_unique_key
Precedence: last_modified_date DESC, action_date DESC, ingestion_ts DESC
Maintains original transaction grain with deduplication audit fields.';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.contract_transaction_unique_key IS 
'Primary deduplication key - unique prime award transaction identifier';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.semantic_description IS 
'Cleaned description from interim layer with IGF patterns removed';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.last_modified_date IS 
'Primary precedence field for deduplication - latest wins';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.dedupe_rank IS 
'Rank within duplicate group (1 = most recent, canonical)';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.dedupe_total_duplicates IS 
'Total number of records sharing the same contract_transaction_unique_key';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.dedupe_precedence_reason IS 
'Explanation of why this record was ranked as it was in deduplication';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.is_canonical IS 
'True for the single authoritative record per contract_transaction_unique_key (dedupe_rank = 1)';

COMMENT ON COLUMN capture_insights.s3_processed_usaspending_prime_awards.source_ingestion_ts IS 
'Original ingestion timestamp from interim layer - used for tie-breaking';

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE capture_insights.s3_processed_usaspending_prime_awards TO PUBLIC;

-- Create UPSERT function for incremental processing
CREATE OR REPLACE FUNCTION capture_insights.upsert_prime_awards_processed()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Handle INSERT/UPDATE with deduplication logic
    INSERT INTO capture_insights.s3_processed_usaspending_prime_awards (
        contract_transaction_unique_key,
        contract_award_unique_key,
        action_date_fiscal_year,
        action_date,
        fiscal_year,
        fiscal_quarter,
        parent_award_id_piid,
        award_id_piid,
        modification_number,
        federal_action_obligation,
        total_dollars_obligated,
        potential_total_value_of_award,
        total_outlayed_amount_for_overall_award,
        period_of_performance_start_date,
        period_of_performance_current_end_date,
        period_of_performance_potential_end_date,
        ordering_period_end_date,
        primary_place_of_performance_city_name,
        primary_place_of_performance_state_code,
        prime_award_base_transaction_description,
        transaction_description,
        semantic_description,
        naics_code,
        naics_description,
        product_or_service_code,
        product_or_service_code_description,
        dod_acquisition_program_description,
        parent_award_agency_name,
        awarding_sub_agency_name,
        awarding_office_name,
        funding_agency_name,
        funding_sub_agency_name,
        funding_office_name,
        recipient_name,
        recipient_uei,
        recipient_parent_name,
        recipient_parent_uei,
        solicitation_date,
        solicitation_identifier,
        solicitation_procedures,
        extent_competed,
        type_of_set_aside,
        fair_opportunity_limited_sources,
        other_than_full_and_open_competition,
        number_of_offers_received,
        subcontracting_plan,
        government_furnished_property,
        type_of_contract_pricing,
        action_type,
        award_type,
        type_of_idc,
        idv_type,
        undefinitized_action,
        program_acronym,
        multi_year_contract,
        multiple_or_single_award_idv,
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
        NEW.contract_transaction_unique_key,
        NEW.contract_award_unique_key,
        NEW.action_date_fiscal_year,
        NEW.action_date,
        NEW.fiscal_year,
        NEW.fiscal_quarter,
        NEW.parent_award_id_piid,
        NEW.award_id_piid,
        NEW.modification_number,
        NEW.federal_action_obligation,
        NEW.total_dollars_obligated,
        NEW.potential_total_value_of_award,
        NEW.total_outlayed_amount_for_overall_award,
        NEW.period_of_performance_start_date,
        NEW.period_of_performance_current_end_date,
        NEW.period_of_performance_potential_end_date,
        NEW.ordering_period_end_date,
        NEW.primary_place_of_performance_city_name,
        NEW.primary_place_of_performance_state_code,
        NEW.prime_award_base_transaction_description,
        NEW.transaction_description,
        COALESCE(NEW.semantic_description, 'No description available'),
        NEW.naics_code,
        NEW.naics_description,
        NEW.product_or_service_code,
        NEW.product_or_service_code_description,
        NEW.dod_acquisition_program_description,
        NEW.parent_award_agency_name,
        NEW.awarding_sub_agency_name,
        NEW.awarding_office_name,
        NEW.funding_agency_name,
        NEW.funding_sub_agency_name,
        NEW.funding_office_name,
        NEW.recipient_name,
        NEW.recipient_uei,
        NEW.recipient_parent_name,
        NEW.recipient_parent_uei,
        NEW.solicitation_date,
        NEW.solicitation_identifier,
        NEW.solicitation_procedures,
        NEW.extent_competed,
        NEW.type_of_set_aside,
        NEW.fair_opportunity_limited_sources,
        NEW.other_than_full_and_open_competition,
        NEW.number_of_offers_received,
        NEW.subcontracting_plan,
        NEW.government_furnished_property,
        NEW.type_of_contract_pricing,
        NEW.action_type,
        NEW.award_type,
        NEW.type_of_idc,
        NEW.idv_type,
        NEW.undefinitized_action,
        NEW.program_acronym,
        NEW.multi_year_contract,
        NEW.multiple_or_single_award_idv,
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
    ON CONFLICT (contract_transaction_unique_key) 
    DO UPDATE SET
        contract_award_unique_key = EXCLUDED.contract_award_unique_key,
        action_date_fiscal_year = EXCLUDED.action_date_fiscal_year,
        action_date = EXCLUDED.action_date,
        fiscal_year = EXCLUDED.fiscal_year,
        fiscal_quarter = EXCLUDED.fiscal_quarter,
        parent_award_id_piid = EXCLUDED.parent_award_id_piid,
        award_id_piid = EXCLUDED.award_id_piid,
        modification_number = EXCLUDED.modification_number,
        federal_action_obligation = EXCLUDED.federal_action_obligation,
        total_dollars_obligated = EXCLUDED.total_dollars_obligated,
        potential_total_value_of_award = EXCLUDED.potential_total_value_of_award,
        total_outlayed_amount_for_overall_award = EXCLUDED.total_outlayed_amount_for_overall_award,
        period_of_performance_start_date = EXCLUDED.period_of_performance_start_date,
        period_of_performance_current_end_date = EXCLUDED.period_of_performance_current_end_date,
        period_of_performance_potential_end_date = EXCLUDED.period_of_performance_potential_end_date,
        ordering_period_end_date = EXCLUDED.ordering_period_end_date,
        primary_place_of_performance_city_name = EXCLUDED.primary_place_of_performance_city_name,
        primary_place_of_performance_state_code = EXCLUDED.primary_place_of_performance_state_code,
        prime_award_base_transaction_description = EXCLUDED.prime_award_base_transaction_description,
        transaction_description = EXCLUDED.transaction_description,
        semantic_description = EXCLUDED.semantic_description,
        naics_code = EXCLUDED.naics_code,
        naics_description = EXCLUDED.naics_description,
        product_or_service_code = EXCLUDED.product_or_service_code,
        product_or_service_code_description = EXCLUDED.product_or_service_code_description,
        dod_acquisition_program_description = EXCLUDED.dod_acquisition_program_description,
        parent_award_agency_name = EXCLUDED.parent_award_agency_name,
        awarding_sub_agency_name = EXCLUDED.awarding_sub_agency_name,
        awarding_office_name = EXCLUDED.awarding_office_name,
        funding_agency_name = EXCLUDED.funding_agency_name,
        funding_sub_agency_name = EXCLUDED.funding_sub_agency_name,
        funding_office_name = EXCLUDED.funding_office_name,
        recipient_name = EXCLUDED.recipient_name,
        recipient_uei = EXCLUDED.recipient_uei,
        recipient_parent_name = EXCLUDED.recipient_parent_name,
        recipient_parent_uei = EXCLUDED.recipient_parent_uei,
        solicitation_date = EXCLUDED.solicitation_date,
        solicitation_identifier = EXCLUDED.solicitation_identifier,
        solicitation_procedures = EXCLUDED.solicitation_procedures,
        extent_competed = EXCLUDED.extent_competed,
        type_of_set_aside = EXCLUDED.type_of_set_aside,
        fair_opportunity_limited_sources = EXCLUDED.fair_opportunity_limited_sources,
        other_than_full_and_open_competition = EXCLUDED.other_than_full_and_open_competition,
        number_of_offers_received = EXCLUDED.number_of_offers_received,
        subcontracting_plan = EXCLUDED.subcontracting_plan,
        government_furnished_property = EXCLUDED.government_furnished_property,
        type_of_contract_pricing = EXCLUDED.type_of_contract_pricing,
        action_type = EXCLUDED.action_type,
        award_type = EXCLUDED.award_type,
        type_of_idc = EXCLUDED.type_of_idc,
        idv_type = EXCLUDED.idv_type,
        undefinitized_action = EXCLUDED.undefinitized_action,
        program_acronym = EXCLUDED.program_acronym,
        multi_year_contract = EXCLUDED.multi_year_contract,
        multiple_or_single_award_idv = EXCLUDED.multiple_or_single_award_idv,
        usaspending_permalink = EXCLUDED.usaspending_permalink,
        last_modified_date = GREATEST(
            capture_insights.s3_processed_usaspending_prime_awards.last_modified_date,
            EXCLUDED.last_modified_date
        ),
        source_ingestion_ts = EXCLUDED.source_ingestion_ts,
        source_chunk_correlation_id = EXCLUDED.source_chunk_correlation_id,
        source_archive_sha256 = EXCLUDED.source_archive_sha256,
        updated_at = now()
        WHERE EXCLUDED.last_modified_date >= capture_insights.s3_processed_usaspending_prime_awards.last_modified_date
           OR EXCLUDED.action_date >= capture_insights.s3_processed_usaspending_prime_awards.action_date
           OR EXCLUDED.source_ingestion_ts > capture_insights.s3_processed_usaspending_prime_awards.source_ingestion_ts;
    
    RETURN NULL;  -- For AFTER trigger
END;
$$;

COMMENT ON FUNCTION capture_insights.upsert_prime_awards_processed() IS 
'UPSERT function for incremental processing of prime awards with precedence-based conflict resolution';

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
    SELECT COUNT(*) INTO interim_count FROM capture_insights.s2_interim_usaspending_prime_awards;
    SELECT COUNT(*) INTO processed_count FROM capture_insights.s3_processed_usaspending_prime_awards;
    SELECT COUNT(*) INTO canonical_count 
    FROM capture_insights.s3_processed_usaspending_prime_awards 
    WHERE is_canonical = true;
    
    -- Get deduplication statistics
    SELECT COUNT(*) INTO duplicate_groups 
    FROM capture_insights.s3_processed_usaspending_prime_awards 
    WHERE dedupe_total_duplicates > 1;
    
    SELECT COALESCE(MAX(dedupe_total_duplicates), 0) INTO max_duplicates 
    FROM capture_insights.s3_processed_usaspending_prime_awards;
    
    RAISE NOTICE 'Prime awards processed table created successfully!';
    RAISE NOTICE 'Records: % interim -> % processed (% canonical)', interim_count, processed_count, canonical_count;
    RAISE NOTICE 'Deduplication: % records in duplicate groups, max % duplicates per key', duplicate_groups, max_duplicates;
    RAISE NOTICE 'Indexes created: 11 (primary key, dedup analysis, precedence, financial, recipient, agency, geographic, industry, source tracking)';
    RAISE NOTICE 'UPSERT function created: upsert_prime_awards_processed() for incremental processing';
END;
$$;