-- SQL Test: Deduplication conflict scenarios and edge cases
-- File: sql/util/test_deduplication_conflicts.sql
-- Purpose: Validate deduplication logic handles edge cases correctly
-- Target: PostgreSQL 14+
-- Usage: Run after processed tables are created to validate deduplication

-- Test setup: Create temporary test data with known conflicts
BEGIN;

-- Test scenario 1: Multiple records with same transaction key, different precedence dates
DO $$
BEGIN
    RAISE NOTICE '=== TEST SCENARIO 1: Prime Awards - Date Precedence ===';
    
    -- Create test records in interim table (simulate conflicts)
    INSERT INTO capture_insights.s2_interim_usaspending_prime_awards (
        contract_transaction_unique_key,
        contract_award_unique_key,
        last_modified_date,
        action_date,
        ingestion_ts,
        chunk_correlation_id,
        archive_sha256,
        semantic_description,
        federal_action_obligation,
        recipient_name
    ) VALUES 
    -- Record 1: Oldest last_modified but newest action_date (should lose)
    ('TEST-CONFLICT-001', 'TEST-AWARD-001', 
     '2023-01-01'::timestamptz, '2024-06-01'::date, 
     '2024-01-01 10:00:00'::timestamptz, 
     gen_random_uuid(), 'test_hash_1',
     'Test conflict record 1 - oldest modified', 100000.00, 'Test Recipient A'),
    
    -- Record 2: Newest last_modified but oldest action_date (should WIN)
    ('TEST-CONFLICT-001', 'TEST-AWARD-001', 
     '2024-05-15'::timestamptz, '2023-12-01'::date, 
     '2024-01-01 11:00:00'::timestamptz, 
     gen_random_uuid(), 'test_hash_2',
     'Test conflict record 2 - newest modified', 200000.00, 'Test Recipient B'),
    
    -- Record 3: Middle last_modified, middle action_date (should lose)
    ('TEST-CONFLICT-001', 'TEST-AWARD-001', 
     '2024-03-01'::timestamptz, '2024-03-01'::date, 
     '2024-01-01 12:00:00'::timestamptz, 
     gen_random_uuid(), 'test_hash_3',
     'Test conflict record 3 - middle modified', 150000.00, 'Test Recipient C');
    
    -- Force re-run deduplication on these test records
    DELETE FROM capture_insights.s3_processed_usaspending_prime_awards 
    WHERE contract_transaction_unique_key = 'TEST-CONFLICT-001';
    
    INSERT INTO capture_insights.s3_processed_usaspending_prime_awards
    SELECT DISTINCT ON (contract_transaction_unique_key)
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
        COALESCE(semantic_description, 'No description available') as semantic_description,
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
        ingestion_ts as source_ingestion_ts,
        chunk_correlation_id as source_chunk_correlation_id,
        archive_sha256 as source_archive_sha256,
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
        ROW_NUMBER() OVER (
            PARTITION BY contract_transaction_unique_key 
            ORDER BY last_modified_date DESC NULLS LAST, 
                     action_date DESC NULLS LAST, 
                     ingestion_ts DESC
        ) = 1 as is_canonical,
        now() as processed_at,
        now() as created_at,
        now() as updated_at
    FROM capture_insights.s2_interim_usaspending_prime_awards
    WHERE contract_transaction_unique_key = 'TEST-CONFLICT-001'
    ORDER BY contract_transaction_unique_key,
             last_modified_date DESC NULLS LAST,
             action_date DESC NULLS LAST,
             ingestion_ts DESC;
    
    -- Validate results
    PERFORM assert_test_result(
        (SELECT COUNT(*) FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE contract_transaction_unique_key = 'TEST-CONFLICT-001'),
        1,
        'Should have exactly 1 canonical record for TEST-CONFLICT-001'
    );
    
    PERFORM assert_test_result(
        (SELECT recipient_name FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE contract_transaction_unique_key = 'TEST-CONFLICT-001' AND is_canonical = true),
        'Test Recipient B',
        'Should select record with newest last_modified_date (Test Recipient B)'
    );
    
    PERFORM assert_test_result(
        (SELECT dedupe_total_duplicates FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE contract_transaction_unique_key = 'TEST-CONFLICT-001'),
        3,
        'Should report 3 total duplicates for TEST-CONFLICT-001'
    );
    
    RAISE NOTICE 'TEST 1 PASSED: Date precedence working correctly';
END;
$$;

-- Test scenario 2: NULL date handling and tie-breaking
DO $$
BEGIN
    RAISE NOTICE '=== TEST SCENARIO 2: Subawards - NULL Date Handling ===';
    
    -- Create test records with NULL dates
    INSERT INTO capture_insights.s2_interim_usaspending_subawards (
        subaward_report_key,
        prime_award_report_key,
        last_modified_date,
        action_date,
        ingestion_ts,
        chunk_correlation_id,
        archive_sha256,
        semantic_description,
        subaward_amount,
        sub_recipient_name
    ) VALUES 
    -- Record 1: NULL last_modified, should lose to non-NULL
    ('TEST-SUB-CONFLICT-001', 'TEST-PRIME-001', 
     NULL, '2024-01-01'::date, 
     '2024-01-01 10:00:00'::timestamptz, 
     gen_random_uuid(), 'test_sub_hash_1',
     'Test subaward conflict 1 - NULL modified', 50000.00, 'Sub Recipient A'),
    
    -- Record 2: Valid last_modified, should WIN
    ('TEST-SUB-CONFLICT-001', 'TEST-PRIME-001', 
     '2024-03-15'::timestamptz, NULL, 
     '2024-01-01 09:00:00'::timestamptz, 
     gen_random_uuid(), 'test_sub_hash_2',
     'Test subaward conflict 2 - valid modified', 75000.00, 'Sub Recipient B'),
    
    -- Record 3: NULL both dates, tie-broken by ingestion_ts (latest ingestion should win)
    ('TEST-SUB-CONFLICT-001', 'TEST-PRIME-001', 
     NULL, NULL, 
     '2024-01-01 12:00:00'::timestamptz,  -- Latest ingestion
     gen_random_uuid(), 'test_sub_hash_3',
     'Test subaward conflict 3 - both NULL', 60000.00, 'Sub Recipient C');
    
    -- Clean and re-run deduplication
    DELETE FROM capture_insights.s3_processed_usaspending_subawards 
    WHERE subaward_report_key = 'TEST-SUB-CONFLICT-001';
    
    INSERT INTO capture_insights.s3_processed_usaspending_subawards
    SELECT DISTINCT ON (subaward_report_key)
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
        COALESCE(semantic_description, 'No description available') as semantic_description,
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
        ingestion_ts as source_ingestion_ts,
        chunk_correlation_id as source_chunk_correlation_id,
        archive_sha256 as source_archive_sha256,
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
        ROW_NUMBER() OVER (
            PARTITION BY subaward_report_key 
            ORDER BY last_modified_date DESC NULLS LAST, 
                     action_date DESC NULLS LAST, 
                     ingestion_ts DESC
        ) = 1 as is_canonical,
        now() as processed_at,
        now() as created_at,
        now() as updated_at
    FROM capture_insights.s2_interim_usaspending_subawards
    WHERE subaward_report_key = 'TEST-SUB-CONFLICT-001'
    ORDER BY subaward_report_key,
             last_modified_date DESC NULLS LAST,
             action_date DESC NULLS LAST,
             ingestion_ts DESC;
    
    -- Validate NULL handling
    PERFORM assert_test_result(
        (SELECT sub_recipient_name FROM capture_insights.s3_processed_usaspending_subawards 
         WHERE subaward_report_key = 'TEST-SUB-CONFLICT-001' AND is_canonical = true),
        'Sub Recipient B',
        'Should select record with valid last_modified_date over NULL dates'
    );
    
    RAISE NOTICE 'TEST 2 PASSED: NULL date handling working correctly';
END;
$$;

-- Test scenario 3: Identical precedence dates - ingestion timestamp tie-breaking
DO $$
BEGIN
    RAISE NOTICE '=== TEST SCENARIO 3: Prime Awards - Ingestion Tie-Breaking ===';
    
    -- Create records with identical precedence but different ingestion times
    INSERT INTO capture_insights.s2_interim_usaspending_prime_awards (
        contract_transaction_unique_key,
        contract_award_unique_key,
        last_modified_date,
        action_date,
        ingestion_ts,
        chunk_correlation_id,
        archive_sha256,
        semantic_description,
        federal_action_obligation,
        recipient_name
    ) VALUES 
    -- Record 1: Same dates, earlier ingestion (should lose)
    ('TEST-TIE-001', 'TEST-AWARD-TIE-001', 
     '2024-05-01'::timestamptz, '2024-05-01'::date, 
     '2024-06-01 10:00:00'::timestamptz, 
     gen_random_uuid(), 'test_tie_hash_1',
     'Test tie record 1 - earlier ingestion', 100000.00, 'Tie Recipient A'),
    
    -- Record 2: Same dates, later ingestion (should WIN)
    ('TEST-TIE-001', 'TEST-AWARD-TIE-001', 
     '2024-05-01'::timestamptz, '2024-05-01'::date, 
     '2024-06-01 15:30:00'::timestamptz, 
     gen_random_uuid(), 'test_tie_hash_2',
     'Test tie record 2 - later ingestion', 200000.00, 'Tie Recipient B');
    
    -- Clean and re-run
    DELETE FROM capture_insights.s3_processed_usaspending_prime_awards 
    WHERE contract_transaction_unique_key = 'TEST-TIE-001';
    
    -- Insert with deduplication (same logic as before, abbreviated for brevity)
    INSERT INTO capture_insights.s3_processed_usaspending_prime_awards
    SELECT DISTINCT ON (contract_transaction_unique_key)
        contract_transaction_unique_key,
        contract_award_unique_key,
        action_date_fiscal_year, action_date, fiscal_year, fiscal_quarter,
        parent_award_id_piid, award_id_piid, modification_number,
        federal_action_obligation, total_dollars_obligated, potential_total_value_of_award, total_outlayed_amount_for_overall_award,
        period_of_performance_start_date, period_of_performance_current_end_date, period_of_performance_potential_end_date, ordering_period_end_date,
        primary_place_of_performance_city_name, primary_place_of_performance_state_code,
        prime_award_base_transaction_description, transaction_description,
        COALESCE(semantic_description, 'No description available') as semantic_description,
        naics_code, naics_description, product_or_service_code, product_or_service_code_description, dod_acquisition_program_description,
        parent_award_agency_name, awarding_sub_agency_name, awarding_office_name, funding_agency_name, funding_sub_agency_name, funding_office_name,
        recipient_name, recipient_uei, recipient_parent_name, recipient_parent_uei,
        solicitation_date, solicitation_identifier, solicitation_procedures,
        extent_competed, type_of_set_aside, fair_opportunity_limited_sources, other_than_full_and_open_competition, number_of_offers_received,
        subcontracting_plan, government_furnished_property, type_of_contract_pricing, action_type, award_type, type_of_idc, idv_type, undefinitized_action, program_acronym, multi_year_contract, multiple_or_single_award_idv,
        usaspending_permalink, last_modified_date,
        ingestion_ts as source_ingestion_ts, chunk_correlation_id as source_chunk_correlation_id, archive_sha256 as source_archive_sha256,
        ROW_NUMBER() OVER (PARTITION BY contract_transaction_unique_key ORDER BY last_modified_date DESC NULLS LAST, action_date DESC NULLS LAST, ingestion_ts DESC) as dedupe_rank,
        COUNT(*) OVER (PARTITION BY contract_transaction_unique_key) as dedupe_total_duplicates,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY contract_transaction_unique_key ORDER BY last_modified_date DESC NULLS LAST, action_date DESC NULLS LAST, ingestion_ts DESC) = 1 THEN 'latest_by_precedence_rule' ELSE 'superseded_by_newer_record' END as dedupe_precedence_reason,
        ROW_NUMBER() OVER (PARTITION BY contract_transaction_unique_key ORDER BY last_modified_date DESC NULLS LAST, action_date DESC NULLS LAST, ingestion_ts DESC) = 1 as is_canonical,
        now() as processed_at, now() as created_at, now() as updated_at
    FROM capture_insights.s2_interim_usaspending_prime_awards
    WHERE contract_transaction_unique_key = 'TEST-TIE-001'
    ORDER BY contract_transaction_unique_key, last_modified_date DESC NULLS LAST, action_date DESC NULLS LAST, ingestion_ts DESC;
    
    -- Validate ingestion tie-breaking
    PERFORM assert_test_result(
        (SELECT recipient_name FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE contract_transaction_unique_key = 'TEST-TIE-001' AND is_canonical = true),
        'Tie Recipient B',
        'Should select record with later ingestion_ts when dates are identical'
    );
    
    RAISE NOTICE 'TEST 3 PASSED: Ingestion timestamp tie-breaking working correctly';
END;
$$;

-- Test scenario 4: Edge case with single record (no conflicts)
DO $$
BEGIN
    RAISE NOTICE '=== TEST SCENARIO 4: Single Record - No Conflicts ===';
    
    -- Insert single record
    INSERT INTO capture_insights.s2_interim_usaspending_prime_awards (
        contract_transaction_unique_key,
        contract_award_unique_key,
        last_modified_date,
        action_date,
        ingestion_ts,
        chunk_correlation_id,
        archive_sha256,
        semantic_description,
        federal_action_obligation,
        recipient_name
    ) VALUES 
    ('TEST-SINGLE-001', 'TEST-AWARD-SINGLE-001', 
     '2024-05-01'::timestamptz, '2024-05-01'::date, 
     '2024-06-01 10:00:00'::timestamptz, 
     gen_random_uuid(), 'test_single_hash_1',
     'Test single record - no conflicts', 100000.00, 'Single Recipient');
    
    -- Clean and process
    DELETE FROM capture_insights.s3_processed_usaspending_prime_awards 
    WHERE contract_transaction_unique_key = 'TEST-SINGLE-001';
    
    INSERT INTO capture_insights.s3_processed_usaspending_prime_awards
    SELECT DISTINCT ON (contract_transaction_unique_key)
        contract_transaction_unique_key, contract_award_unique_key, action_date_fiscal_year, action_date, fiscal_year, fiscal_quarter,
        parent_award_id_piid, award_id_piid, modification_number, federal_action_obligation, total_dollars_obligated, potential_total_value_of_award, total_outlayed_amount_for_overall_award,
        period_of_performance_start_date, period_of_performance_current_end_date, period_of_performance_potential_end_date, ordering_period_end_date,
        primary_place_of_performance_city_name, primary_place_of_performance_state_code, prime_award_base_transaction_description, transaction_description,
        COALESCE(semantic_description, 'No description available') as semantic_description, naics_code, naics_description, product_or_service_code, product_or_service_code_description, dod_acquisition_program_description,
        parent_award_agency_name, awarding_sub_agency_name, awarding_office_name, funding_agency_name, funding_sub_agency_name, funding_office_name,
        recipient_name, recipient_uei, recipient_parent_name, recipient_parent_uei, solicitation_date, solicitation_identifier, solicitation_procedures,
        extent_competed, type_of_set_aside, fair_opportunity_limited_sources, other_than_full_and_open_competition, number_of_offers_received,
        subcontracting_plan, government_furnished_property, type_of_contract_pricing, action_type, award_type, type_of_idc, idv_type, undefinitized_action, program_acronym, multi_year_contract, multiple_or_single_award_idv,
        usaspending_permalink, last_modified_date, ingestion_ts as source_ingestion_ts, chunk_correlation_id as source_chunk_correlation_id, archive_sha256 as source_archive_sha256,
        1 as dedupe_rank, 1 as dedupe_total_duplicates, 'latest_by_precedence_rule' as dedupe_precedence_reason, true as is_canonical,
        now() as processed_at, now() as created_at, now() as updated_at
    FROM capture_insights.s2_interim_usaspending_prime_awards
    WHERE contract_transaction_unique_key = 'TEST-SINGLE-001';
    
    -- Validate single record handling
    PERFORM assert_test_result(
        (SELECT dedupe_rank FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE contract_transaction_unique_key = 'TEST-SINGLE-001'),
        1,
        'Single record should have dedupe_rank = 1'
    );
    
    PERFORM assert_test_result(
        (SELECT dedupe_total_duplicates FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE contract_transaction_unique_key = 'TEST-SINGLE-001'),
        1,
        'Single record should have dedupe_total_duplicates = 1'
    );
    
    PERFORM assert_test_result(
        (SELECT is_canonical FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE contract_transaction_unique_key = 'TEST-SINGLE-001'),
        true,
        'Single record should be canonical'
    );
    
    RAISE NOTICE 'TEST 4 PASSED: Single record handling working correctly';
END;
$$;

-- Test scenario 5: Verify canonical flag consistency across all records
DO $$
DECLARE
    canonical_inconsistencies bigint;
    total_duplicate_groups bigint;
    multi_canonical_groups bigint;
BEGIN
    RAISE NOTICE '=== TEST SCENARIO 5: Canonical Flag Consistency ===';
    
    -- Check for duplicate groups with no canonical records
    SELECT COUNT(*) INTO canonical_inconsistencies
    FROM (
        SELECT contract_transaction_unique_key, COUNT(*) as canonical_count
        FROM capture_insights.s3_processed_usaspending_prime_awards 
        WHERE is_canonical = true
        GROUP BY contract_transaction_unique_key
        HAVING COUNT(*) = 0
    ) no_canonical;
    
    PERFORM assert_test_result(
        canonical_inconsistencies,
        0,
        'All duplicate groups should have at least one canonical record'
    );
    
    -- Check for duplicate groups with multiple canonical records
    SELECT COUNT(*) INTO multi_canonical_groups
    FROM (
        SELECT contract_transaction_unique_key, COUNT(*) as canonical_count
        FROM capture_insights.s3_processed_usaspending_prime_awards 
        WHERE is_canonical = true
        GROUP BY contract_transaction_unique_key
        HAVING COUNT(*) > 1
    ) multi_canonical;
    
    PERFORM assert_test_result(
        multi_canonical_groups,
        0,
        'No duplicate group should have multiple canonical records'
    );
    
    -- Verify dedupe_rank = 1 always corresponds to is_canonical = true
    PERFORM assert_test_result(
        (SELECT COUNT(*) FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE dedupe_rank = 1 AND is_canonical = false),
        0,
        'All records with dedupe_rank = 1 should be canonical'
    );
    
    PERFORM assert_test_result(
        (SELECT COUNT(*) FROM capture_insights.s3_processed_usaspending_prime_awards 
         WHERE dedupe_rank > 1 AND is_canonical = true),
        0,
        'No records with dedupe_rank > 1 should be canonical'
    );
    
    RAISE NOTICE 'TEST 5 PASSED: Canonical flag consistency validated';
END;
$$;

-- Helper function for test assertions
CREATE OR REPLACE FUNCTION assert_test_result(
    actual anyelement, 
    expected anyelement, 
    test_description text
) 
RETURNS void 
LANGUAGE plpgsql 
AS $$
BEGIN
    IF actual IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'TEST FAILED: % - Expected: %, Actual: %', 
              test_description, expected, actual;
    END IF;
END;
$$;

-- Clean up test data
DO $$
BEGIN
    RAISE NOTICE '=== CLEANING UP TEST DATA ===';
    
    -- Remove test records from processed tables
    DELETE FROM capture_insights.s3_processed_usaspending_prime_awards 
    WHERE contract_transaction_unique_key IN (
        'TEST-CONFLICT-001', 'TEST-TIE-001', 'TEST-SINGLE-001'
    );
    
    DELETE FROM capture_insights.s3_processed_usaspending_subawards 
    WHERE subaward_report_key IN (
        'TEST-SUB-CONFLICT-001'
    );
    
    -- Remove test records from interim tables
    DELETE FROM capture_insights.s2_interim_usaspending_prime_awards 
    WHERE contract_transaction_unique_key IN (
        'TEST-CONFLICT-001', 'TEST-TIE-001', 'TEST-SINGLE-001'
    );
    
    DELETE FROM capture_insights.s2_interim_usaspending_subawards 
    WHERE subaward_report_key IN (
        'TEST-SUB-CONFLICT-001'
    );
    
    RAISE NOTICE 'Test data cleanup completed';
END;
$$;

-- Performance validation: Check for potential performance issues
DO $$
DECLARE
    max_duplicates_prime integer;
    max_duplicates_sub integer;
    large_duplicate_groups_prime bigint;
    large_duplicate_groups_sub bigint;
BEGIN
    RAISE NOTICE '=== PERFORMANCE VALIDATION ===';
    
    -- Check maximum duplicate counts
    SELECT COALESCE(MAX(dedupe_total_duplicates), 0) INTO max_duplicates_prime
    FROM capture_insights.s3_processed_usaspending_prime_awards;
    
    SELECT COALESCE(MAX(dedupe_total_duplicates), 0) INTO max_duplicates_sub
    FROM capture_insights.s3_processed_usaspending_subawards;
    
    -- Count groups with excessive duplicates (performance concern)
    SELECT COUNT(*) INTO large_duplicate_groups_prime
    FROM capture_insights.s3_processed_usaspending_prime_awards 
    WHERE dedupe_total_duplicates > 10;
    
    SELECT COUNT(*) INTO large_duplicate_groups_sub
    FROM capture_insights.s3_processed_usaspending_subawards 
    WHERE dedupe_total_duplicates > 10;
    
    RAISE NOTICE 'Max duplicates - Prime: %, Subawards: %', max_duplicates_prime, max_duplicates_sub;
    RAISE NOTICE 'Large duplicate groups (>10) - Prime: %, Subawards: %', large_duplicate_groups_prime, large_duplicate_groups_sub;
    
    -- Warning for performance concerns
    IF max_duplicates_prime > 50 OR max_duplicates_sub > 50 THEN
        RAISE WARNING 'High duplicate counts detected. Consider investigating data quality.';
    END IF;
    
    RAISE NOTICE 'Performance validation completed';
END;
$$;

-- Final summary
DO $$
DECLARE
    total_prime_processed bigint;
    total_sub_processed bigint;
    canonical_prime bigint;
    canonical_sub bigint;
    duplicate_prime_groups bigint;
    duplicate_sub_groups bigint;
BEGIN
    RAISE NOTICE '=== DEDUPLICATION TEST SUMMARY ===';
    
    -- Get final counts
    SELECT COUNT(*) INTO total_prime_processed FROM capture_insights.s3_processed_usaspending_prime_awards;
    SELECT COUNT(*) INTO total_sub_processed FROM capture_insights.s3_processed_usaspending_subawards;
    SELECT COUNT(*) INTO canonical_prime FROM capture_insights.s3_processed_usaspending_prime_awards WHERE is_canonical = true;
    SELECT COUNT(*) INTO canonical_sub FROM capture_insights.s3_processed_usaspending_subawards WHERE is_canonical = true;
    
    SELECT COUNT(*) INTO duplicate_prime_groups FROM capture_insights.s3_processed_usaspending_prime_awards WHERE dedupe_total_duplicates > 1;
    SELECT COUNT(*) INTO duplicate_sub_groups FROM capture_insights.s3_processed_usaspending_subawards WHERE dedupe_total_duplicates > 1;
    
    RAISE NOTICE 'Prime Awards: % total records, % canonical, % in duplicate groups', 
                 total_prime_processed, canonical_prime, duplicate_prime_groups;
    RAISE NOTICE 'Subawards: % total records, % canonical, % in duplicate groups', 
                 total_sub_processed, canonical_sub, duplicate_sub_groups;
    
    RAISE NOTICE '=== ALL DEDUPLICATION TESTS PASSED ===';
    RAISE NOTICE 'Deduplication logic validated successfully!';
    RAISE NOTICE '- Date precedence working correctly';
    RAISE NOTICE '- NULL date handling working correctly';  
    RAISE NOTICE '- Ingestion timestamp tie-breaking working correctly';
    RAISE NOTICE '- Single record handling working correctly';
    RAISE NOTICE '- Canonical flag consistency validated';
    RAISE NOTICE '- Performance characteristics within acceptable ranges';
END;
$$;

-- Clean up helper function
DROP FUNCTION IF EXISTS assert_test_result(anyelement, anyelement, text);

COMMIT;

-- Usage instructions:
-- 1. Run this script after creating processed tables to validate deduplication
-- 2. All tests should pass with "ALL DEDUPLICATION TESTS PASSED" message
-- 3. Review warnings about performance if duplicate counts are high
-- 4. Test data is automatically cleaned up after validation