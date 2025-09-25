"""Unit tests for description cleaning functionality.

This test MUST fail initially until clean_description function is implemented.
"""
import pytest
from unittest.mock import Mock, patch

# This will fail until implementation exists
from etl.utils.cleansing import clean_description  # Will fail until implemented


def test_clean_description_removes_igf_pattern():
    """Test that clean_description removes IGF pattern from award descriptions."""
    
    # Test cases with IGF pattern
    test_cases = [
        {
            'input': 'Contract for consulting services [IGF:12345678]',
            'expected': 'Contract for consulting services',
            'description': 'IGF pattern at end'
        },
        {
            'input': '[IGF:87654321] Software development project',
            'expected': 'Software development project', 
            'description': 'IGF pattern at start'
        },
        {
            'input': 'Research project [IGF:ABCD1234] with multiple phases',
            'expected': 'Research project with multiple phases',
            'description': 'IGF pattern in middle'
        },
        {
            'input': 'Multiple [IGF:111] awards [IGF:222] in description',
            'expected': 'Multiple awards in description',
            'description': 'Multiple IGF patterns'
        },
        {
            'input': 'Award with [IGF:999] and [IGF:888] and [IGF:777]',
            'expected': 'Award with and and',
            'description': 'Multiple consecutive IGF patterns'
        },
        {
            'input': 'No IGF pattern in this description',
            'expected': 'No IGF pattern in this description',
            'description': 'No pattern to remove'
        },
        {
            'input': '',
            'expected': '',
            'description': 'Empty string'
        },
        {
            'input': '[IGF:ONLY]',
            'expected': '',
            'description': 'Only IGF pattern'
        }
    ]
    
    # This will fail until clean_description is implemented
    for case in test_cases:
        result = clean_description(case['input'])
        assert result.strip() == case['expected'], f"Failed {case['description']}: '{case['input']}' -> '{result}' (expected '{case['expected']}')"


def test_clean_description_preserves_non_igf_brackets():
    """Test that clean_description preserves brackets that are not IGF patterns."""
    
    test_cases = [
        {
            'input': 'Contract [Revised] for services',
            'expected': 'Contract [Revised] for services',
            'description': 'Regular brackets should be preserved'
        },
        {
            'input': 'Award [Phase 1] and [Phase 2] implementation',
            'expected': 'Award [Phase 1] and [Phase 2] implementation',
            'description': 'Multiple non-IGF brackets'
        },
        {
            'input': 'Project [ref:123] with [IGF:456] and [note:important]',
            'expected': 'Project [ref:123] with and [note:important]',
            'description': 'Mixed brackets - only IGF should be removed'
        },
        {
            'input': '[Status: Active] project [IGF:999] details',
            'expected': '[Status: Active] project details',
            'description': 'IGF mixed with other bracket types'
        }
    ]
    
    for case in test_cases:
        result = clean_description(case['input'])
        assert result.strip() == case['expected'], f"Failed {case['description']}: '{case['input']}' -> '{result}' (expected '{case['expected']}')"


def test_clean_description_handles_case_variations():
    """Test that clean_description handles different case variations of IGF pattern."""
    
    test_cases = [
        {
            'input': 'Award [igf:12345] description',
            'expected': 'Award description',
            'description': 'Lowercase igf'
        },
        {
            'input': 'Contract [IGF:67890] details',
            'expected': 'Contract details', 
            'description': 'Uppercase IGF'
        },
        {
            'input': 'Project [Igf:ABCD] information',
            'expected': 'Project information',
            'description': 'Mixed case Igf'
        },
        {
            'input': 'Service [iGf:999] contract',
            'expected': 'Service contract',
            'description': 'Mixed case iGf'
        }
    ]
    
    for case in test_cases:
        result = clean_description(case['input'])
        assert result.strip() == case['expected'], f"Failed {case['description']}: '{case['input']}' -> '{result}' (expected '{case['expected']}')"


def test_clean_description_handles_malformed_igf_patterns():
    """Test that clean_description handles malformed IGF patterns correctly."""
    
    test_cases = [
        {
            'input': 'Award [IGF] without colon should be preserved',
            'expected': 'Award [IGF] without colon should be preserved',
            'description': 'IGF without colon should not be removed'
        },
        {
            'input': 'Contract [IGF:] empty value should be removed',
            'expected': 'Contract empty value should be removed',
            'description': 'IGF with empty value should be removed'
        },
        {
            'input': 'Project [IGF:123 unclosed bracket',
            'expected': 'Project [IGF:123 unclosed bracket',
            'description': 'Unclosed bracket should be preserved'
        },
        {
            'input': 'Service IGF:456] missing opening bracket',
            'expected': 'Service IGF:456] missing opening bracket',
            'description': 'Missing opening bracket should be preserved'
        }
    ]
    
    for case in test_cases:
        result = clean_description(case['input'])
        assert result.strip() == case['expected'], f"Failed {case['description']}: '{case['input']}' -> '{result}' (expected '{case['expected']}')"


def test_clean_description_handles_whitespace():
    """Test that clean_description handles whitespace correctly after removal."""
    
    test_cases = [
        {
            'input': 'Award    [IGF:123]    description',
            'expected': 'Award description',
            'description': 'Multiple spaces around IGF pattern'
        },
        {
            'input': 'Contract\t[IGF:456]\tdescription',
            'expected': 'Contract description',
            'description': 'Tabs around IGF pattern'
        },
        {
            'input': 'Project\n[IGF:789]\ndescription',
            'expected': 'Project description', 
            'description': 'Newlines around IGF pattern'
        },
        {
            'input': '  [IGF:111]  Leading and trailing spaces  [IGF:222]  ',
            'expected': 'Leading and trailing spaces',
            'description': 'Leading and trailing spaces with IGF'
        }
    ]
    
    for case in test_cases:
        result = clean_description(case['input'])
        assert result.strip() == case['expected'], f"Failed {case['description']}: '{case['input']}' -> '{result}' (expected '{case['expected']}')"


def test_clean_description_with_none_input():
    """Test that clean_description handles None input gracefully."""
    
    # This will fail until clean_description is implemented
    result = clean_description(None)
    assert result == '' or result is None, "clean_description should handle None input gracefully"


def test_clean_description_with_non_string_input():
    """Test that clean_description handles non-string input appropriately."""
    
    # Test with integer
    with pytest.raises(TypeError):
        clean_description(12345)
    
    # Test with list
    with pytest.raises(TypeError):
        clean_description(['IGF', '123'])
    
    # Test with dict
    with pytest.raises(TypeError):
        clean_description({'igf': '123'})


def test_clean_description_performance_with_large_input():
    """Test that clean_description performs reasonably with large input strings."""
    
    # Create a large string with multiple IGF patterns
    large_description = "Award description " * 1000
    igf_patterns = [" [IGF:{}] ".format(i) for i in range(100)]
    large_input = large_description + "".join(igf_patterns) + large_description
    
    # This will fail until clean_description is implemented
    result = clean_description(large_input)
    
    # Should remove all IGF patterns but preserve the rest
    assert '[IGF:' not in result, "All IGF patterns should be removed"
    assert 'Award description' in result, "Original text should be preserved"
    assert len(result) < len(large_input), "Result should be shorter than input"


def test_clean_description_regex_pattern_validation():
    """Test that the IGF regex pattern is correctly implemented."""
    
    # Test edge cases for the regex pattern
    test_cases = [
        {
            'input': '[IGF:123-456]',  # Hyphen in ID
            'expected': '',
            'description': 'IGF with hyphen in ID'
        },
        {
            'input': '[IGF:ABC_DEF]',  # Underscore in ID  
            'expected': '',
            'description': 'IGF with underscore in ID'
        },
        {
            'input': '[IGF:123.456]',  # Dot in ID
            'expected': '',
            'description': 'IGF with dot in ID'
        },
        {
            'input': '[IGF: 123]',     # Space after colon
            'expected': '',
            'description': 'IGF with space after colon'
        },
        {
            'input': '[IGF:123 ]',     # Space before closing bracket
            'expected': '',
            'description': 'IGF with space before closing bracket'
        }
    ]
    
    for case in test_cases:
        result = clean_description(case['input'])
        assert result.strip() == case['expected'], f"Failed {case['description']}: '{case['input']}' -> '{result}' (expected '{case['expected']}')"


def test_clean_description_sql_function_integration():
    """Test that clean_description behavior matches expected SQL function behavior."""
    
    # These test cases should match the SQL function implementation
    # when it's created in sql/util/010_clean_description.sql
    test_cases = [
        'Simple award [IGF:12345] for testing',
        '[IGF:ABCD] Leading IGF pattern',
        'Trailing IGF pattern [IGF:9999]',
        'Multiple [IGF:111] IGF [IGF:222] patterns [IGF:333]',
        'No IGF patterns in this description',
        ''
    ]
    
    # This will fail until clean_description is implemented
    for test_input in test_cases:
        python_result = clean_description(test_input)
        
        # Future: when SQL function is implemented, we can test consistency
        # For now, just ensure the function runs and returns a string
        assert isinstance(python_result, str), f"clean_description should return string for input: {test_input}"
        
        # Ensure no IGF patterns remain
        if '[IGF:' in test_input:
            assert '[IGF:' not in python_result, f"IGF pattern should be removed from: {test_input}"