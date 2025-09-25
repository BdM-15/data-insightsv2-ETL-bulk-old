-- SQL Transform: Interim prime awards table (s2_interim layer)
-- File: sql/10_s2_interim/010_prime_awards_interim.sql  
-- Purpose: Transform raw prime awards with type casting, cleansing, and enrichment
-- Dependencies: s1_raw.usaspending_prime_awards_slimv2, util.clean_description function
-- Target: PostgreSQL 14+

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS capture_insights;

-- Drop table if exists (for re-running)
DROP TABLE IF EXISTS capture_insights.s2_interim_usaspending_prime_awards CASCADE;

-- Create interim prime awards table with proper types and transformations
CREATE TABLE capture_insights.s2_interim_usaspending_prime_awards AS
SELECT 
    -- Business keys (preserved)
    contract_transaction_unique_key,
    contract_award_unique_key,
    
    -- Date and fiscal fields (cast with NULL on invalid)
    CASE 
        WHEN action_date_fiscal_year ~ '^\d{4}$' 
        THEN action_date_fiscal_year::integer 
        ELSE NULL 
    END as action_date_fiscal_year,
    
    CASE 
        WHEN action_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN action_date::date 
        ELSE NULL 
    END as action_date,
    
    -- Derived fiscal calculations
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
    
    -- Award identification (trimmed)
    trim(parent_award_id_piid) as parent_award_id_piid,
    trim(award_id_piid) as award_id_piid,
    trim(modification_number) as modification_number,
    
    -- Financial fields (safe numeric casting)
    CASE 
        WHEN federal_action_obligation ~ '^-?\d+\.?\d*$' 
        THEN federal_action_obligation::numeric 
        ELSE NULL 
    END as federal_action_obligation,
    
    CASE 
        WHEN total_dollars_obligated ~ '^-?\d+\.?\d*$' 
        THEN total_dollars_obligated::numeric 
        ELSE NULL 
    END as total_dollars_obligated,
    
    CASE 
        WHEN potential_total_value_of_award ~ '^-?\d+\.?\d*$' 
        THEN potential_total_value_of_award::numeric 
        ELSE NULL 
    END as potential_total_value_of_award,
    
    CASE 
        WHEN total_outlayed_amount_for_overall_award ~ '^-?\d+\.?\d*$' 
        THEN total_outlayed_amount_for_overall_award::numeric 
        ELSE NULL 
    END as total_outlayed_amount_for_overall_award,
    
    -- Performance period dates (safe date casting)
    CASE 
        WHEN period_of_performance_start_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN period_of_performance_start_date::date 
        ELSE NULL 
    END as period_of_performance_start_date,
    
    CASE 
        WHEN period_of_performance_current_end_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN period_of_performance_current_end_date::date 
        ELSE NULL 
    END as period_of_performance_current_end_date,
    
    CASE 
        WHEN period_of_performance_potential_end_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN period_of_performance_potential_end_date::date 
        ELSE NULL 
    END as period_of_performance_potential_end_date,
    
    CASE 
        WHEN ordering_period_end_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN ordering_period_end_date::date 
        ELSE NULL 
    END as ordering_period_end_date,
    
    -- Place of performance (trimmed)
    trim(primary_place_of_performance_city_name) as primary_place_of_performance_city_name,
    trim(primary_place_of_performance_state_code) as primary_place_of_performance_state_code,
    
    -- Descriptions (trimmed and cleaned)
    trim(prime_award_base_transaction_description) as prime_award_base_transaction_description,
    trim(transaction_description) as transaction_description,
    
    -- Semantic description (cleaned using util function)
    util.clean_description(
        COALESCE(
            nullif(trim(prime_award_base_transaction_description), ''),
            nullif(trim(transaction_description), ''),
            'No description available'
        )
    ) as semantic_description,
    
    -- Industry classification (trimmed)
    trim(naics_code) as naics_code,
    trim(naics_description) as naics_description,
    trim(product_or_service_code) as product_or_service_code,
    trim(product_or_service_code_description) as product_or_service_code_description,
    trim(dod_acquisition_program_description) as dod_acquisition_program_description,
    
    -- Agency information (trimmed)
    trim(parent_award_agency_name) as parent_award_agency_name,
    trim(awarding_sub_agency_name) as awarding_sub_agency_name,
    trim(awarding_office_name) as awarding_office_name,
    trim(funding_agency_name) as funding_agency_name,
    trim(funding_sub_agency_name) as funding_sub_agency_name,
    trim(funding_office_name) as funding_office_name,
    
    -- Recipient information (trimmed)
    trim(recipient_name) as recipient_name,
    trim(recipient_uei) as recipient_uei,
    trim(recipient_parent_name) as recipient_parent_name,
    trim(recipient_parent_uei) as recipient_parent_uei,
    
    -- Solicitation details (safe casting and trimming)
    CASE 
        WHEN solicitation_date ~ '^\d{4}-\d{2}-\d{2}$' 
        THEN solicitation_date::date 
        ELSE NULL 
    END as solicitation_date,
    
    trim(solicitation_identifier) as solicitation_identifier,
    trim(solicitation_procedures) as solicitation_procedures,
    
    -- Competition details (trimmed)
    trim(extent_competed) as extent_competed,
    trim(type_of_set_aside) as type_of_set_aside,
    trim(fair_opportunity_limited_sources) as fair_opportunity_limited_sources,
    trim(other_than_full_and_open_competition) as other_than_full_and_open_competition,
    
    CASE 
        WHEN number_of_offers_received ~ '^\d+$' 
        THEN number_of_offers_received::integer 
        ELSE NULL 
    END as number_of_offers_received,
    
    -- Contract details (trimmed)
    trim(subcontracting_plan) as subcontracting_plan,
    trim(government_furnished_property) as government_furnished_property,
    trim(type_of_contract_pricing) as type_of_contract_pricing,
    trim(action_type) as action_type,
    trim(award_type) as award_type,
    trim(type_of_idc) as type_of_idc,
    trim(idv_type) as idv_type,
    trim(undefinitized_action) as undefinitized_action,
    trim(program_acronym) as program_acronym,
    trim(multi_year_contract) as multi_year_contract,
    trim(multiple_or_single_award_idv) as multiple_or_single_award_idv,
    
    -- USASpending metadata
    trim(usaspending_permalink) as usaspending_permalink,
    
    -- Derived last_modified_date (use ingestion_ts as substitute if not available in API)
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

FROM capture_insights.s1_raw_usaspending_prime_awards_slimv2;

-- Create performance indexes on interim table
-- Primary business key indexes
CREATE UNIQUE INDEX idx_s2_interim_prime_awards_contract_transaction_key 
ON capture_insights.s2_interim_usaspending_prime_awards(contract_transaction_unique_key);

CREATE INDEX idx_s2_interim_prime_awards_contract_award_key
ON capture_insights.s2_interim_usaspending_prime_awards(contract_award_unique_key);

-- Date and fiscal indexes (now properly typed)
CREATE INDEX idx_s2_interim_prime_awards_action_date
ON capture_insights.s2_interim_usaspending_prime_awards(action_date);

CREATE INDEX idx_s2_interim_prime_awards_fiscal_year
ON capture_insights.s2_interim_usaspending_prime_awards(fiscal_year);

CREATE INDEX idx_s2_interim_prime_awards_last_modified_date
ON capture_insights.s2_interim_usaspending_prime_awards(last_modified_date);

-- Financial analysis indexes
CREATE INDEX idx_s2_interim_prime_awards_federal_obligation
ON capture_insights.s2_interim_usaspending_prime_awards(federal_action_obligation) 
WHERE federal_action_obligation IS NOT NULL;

-- Recipient analysis indexes  
CREATE INDEX idx_s2_interim_prime_awards_recipient_uei
ON capture_insights.s2_interim_usaspending_prime_awards(recipient_uei);

CREATE INDEX idx_s2_interim_prime_awards_recipient_parent_uei
ON capture_insights.s2_interim_usaspending_prime_awards(recipient_parent_uei);

-- Agency analysis indexes
CREATE INDEX idx_s2_interim_prime_awards_awarding_agency
ON capture_insights.s2_interim_usaspending_prime_awards(parent_award_agency_name);

-- ETL operational indexes
CREATE INDEX idx_s2_interim_prime_awards_chunk_correlation
ON capture_insights.s2_interim_usaspending_prime_awards(chunk_correlation_id);

-- Geographic analysis indexes
CREATE INDEX idx_s2_interim_prime_awards_performance_state
ON capture_insights.s2_interim_usaspending_prime_awards(primary_place_of_performance_state_code);

-- Industry analysis indexes
CREATE INDEX idx_s2_interim_prime_awards_naics_code
ON capture_insights.s2_interim_usaspending_prime_awards(naics_code);

-- Add table and column comments
COMMENT ON TABLE capture_insights.s2_interim_usaspending_prime_awards IS 
'Interim layer for prime awards with type casting, cleansing, and semantic enrichment.
All numeric/date fields safely cast with NULL on invalid values.
Description fields cleaned of IGF patterns using util.clean_description function.';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_prime_awards.semantic_description IS 
'Cleaned description with IGF patterns removed, prioritizing prime_award_base_transaction_description over transaction_description';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_prime_awards.fiscal_year IS 
'Fiscal year derived from action_date (October start)';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_prime_awards.fiscal_quarter IS 
'Fiscal quarter derived from action_date (Q1=Oct-Dec, Q2=Jan-Mar, etc.)';

COMMENT ON COLUMN capture_insights.s2_interim_usaspending_prime_awards.last_modified_date IS 
'Last modification timestamp - currently using ingestion_ts as substitute (TODO: use actual API field when available)';

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE capture_insights.s2_interim_usaspending_prime_awards TO PUBLIC;

-- Validation and statistics
DO $$
DECLARE
    raw_count bigint;
    interim_count bigint;
    null_action_dates bigint;
    null_fiscal_years bigint;
    null_obligations bigint;
    cleaned_descriptions bigint;
BEGIN
    -- Get record counts
    SELECT COUNT(*) INTO raw_count FROM capture_insights.s1_raw_usaspending_prime_awards_slimv2;
    SELECT COUNT(*) INTO interim_count FROM capture_insights.s2_interim_usaspending_prime_awards;
    
    -- Get quality metrics
    SELECT COUNT(*) INTO null_action_dates 
    FROM capture_insights.s2_interim_usaspending_prime_awards 
    WHERE action_date IS NULL;
    
    SELECT COUNT(*) INTO null_fiscal_years 
    FROM capture_insights.s2_interim_usaspending_prime_awards 
    WHERE fiscal_year IS NULL;
    
    SELECT COUNT(*) INTO null_obligations 
    FROM capture_insights.s2_interim_usaspending_prime_awards 
    WHERE federal_action_obligation IS NULL;
    
    SELECT COUNT(*) INTO cleaned_descriptions 
    FROM capture_insights.s2_interim_usaspending_prime_awards 
    WHERE semantic_description != COALESCE(prime_award_base_transaction_description, transaction_description, 'No description available');
    
    RAISE NOTICE 'Prime awards interim table created successfully!';
    RAISE NOTICE 'Records: % raw -> % interim', raw_count, interim_count;
    RAISE NOTICE 'Data quality: % null action dates, % null fiscal years, % null obligations', 
                 null_action_dates, null_fiscal_years, null_obligations;
    RAISE NOTICE 'Semantic cleaning: % descriptions cleaned of IGF patterns', cleaned_descriptions;
    RAISE NOTICE 'Indexes created: 12 (business keys, dates, financial, recipient, agency, ETL, geographic, industry)';
END;
$$;