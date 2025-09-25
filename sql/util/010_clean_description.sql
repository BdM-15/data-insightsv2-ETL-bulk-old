-- SQL Migration: Create clean_description utility function
-- File: sql/util/010_clean_description.sql
-- Purpose: Remove IGF patterns from award descriptions for cleaner semantic analysis
-- Dependencies: None
-- Target: PostgreSQL 14+

-- Drop function if exists (for re-running migration)
DROP FUNCTION IF EXISTS util.clean_description(TEXT);

-- Create util schema if not exists  
CREATE SCHEMA IF NOT EXISTS util;

-- Create the clean_description function
CREATE OR REPLACE FUNCTION util.clean_description(input_text TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    cleaned_text TEXT;
BEGIN
    -- Handle NULL input
    IF input_text IS NULL THEN
        RETURN '';
    END IF;
    
    -- Remove IGF patterns using regex
    -- Pattern: [IGF:...] where ... can be any characters except ]
    -- Case insensitive match for IGF
    cleaned_text := regexp_replace(
        input_text,
        '\s*\[(?i:igf):[^\]]*\]\s*',  -- Match [IGF:...] with optional whitespace
        ' ',                           -- Replace with single space
        'g'                           -- Global replace (all occurrences)
    );
    
    -- Clean up multiple spaces and trim
    cleaned_text := regexp_replace(cleaned_text, '\s+', ' ', 'g');
    cleaned_text := trim(cleaned_text);
    
    RETURN cleaned_text;
END;
$$;

-- Add function comment
COMMENT ON FUNCTION util.clean_description(TEXT) IS 
'Removes IGF patterns from award description text for cleaner semantic analysis. 
IGF patterns are in format [IGF:identifier] and are case-insensitive.
Returns cleaned text with normalized whitespace.';

-- Grant execute permissions to expected roles
-- Note: Adjust these based on your actual database role setup
GRANT EXECUTE ON FUNCTION util.clean_description(TEXT) TO PUBLIC;

-- Test cases (optional - can be run to verify function works)
DO $$
DECLARE
    test_result TEXT;
BEGIN
    -- Test 1: Basic IGF removal
    test_result := util.clean_description('Contract for services [IGF:12345]');
    IF test_result != 'Contract for services' THEN
        RAISE EXCEPTION 'Test 1 failed: Expected "Contract for services", got "%"', test_result;
    END IF;
    
    -- Test 2: Multiple IGF patterns
    test_result := util.clean_description('Award [IGF:111] with [IGF:222] multiple patterns');
    IF test_result != 'Award with multiple patterns' THEN
        RAISE EXCEPTION 'Test 2 failed: Expected "Award with multiple patterns", got "%"', test_result;
    END IF;
    
    -- Test 3: Case insensitive
    test_result := util.clean_description('Project [igf:abc] description');
    IF test_result != 'Project description' THEN
        RAISE EXCEPTION 'Test 3 failed: Expected "Project description", got "%"', test_result;
    END IF;
    
    -- Test 4: No IGF pattern
    test_result := util.clean_description('Clean description without patterns');
    IF test_result != 'Clean description without patterns' THEN
        RAISE EXCEPTION 'Test 4 failed: Expected unchanged, got "%"', test_result;
    END IF;
    
    -- Test 5: NULL input
    test_result := util.clean_description(NULL);
    IF test_result != '' THEN
        RAISE EXCEPTION 'Test 5 failed: Expected empty string for NULL, got "%"', test_result;
    END IF;
    
    -- Test 6: Empty input
    test_result := util.clean_description('');
    IF test_result != '' THEN
        RAISE EXCEPTION 'Test 6 failed: Expected empty string, got "%"', test_result;
    END IF;
    
    -- Test 7: Only IGF pattern
    test_result := util.clean_description('[IGF:ONLY]');
    IF test_result != '' THEN
        RAISE EXCEPTION 'Test 7 failed: Expected empty string, got "%"', test_result;
    END IF;
    
    RAISE NOTICE 'All clean_description tests passed successfully!';
END;
$$;