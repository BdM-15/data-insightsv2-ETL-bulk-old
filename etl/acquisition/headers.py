"""
Header validation for CSV downloads from USASpending API.

This module validates CSV header columns against the expected schema for the raw layer table.
The authoritative field list is sourced from sql/00_s1_raw/010_prime_awards_raw.sql DDL.
"""

import logging
from typing import List, Set, Tuple, Optional
from enum import Enum

from ..utils.logging import get_logger

logger = get_logger(__name__)


class ValidationResult(Enum):
    """Result of header validation."""
    VALID = "valid"
    MISSING_REQUIRED = "missing_required"
    EXTRA_COLUMNS = "extra_columns"
    MISMATCH = "mismatch"


class HeaderValidationError(Exception):
    """Raised when CSV headers fail validation."""
    
    def __init__(self, result: ValidationResult, message: str, missing: Optional[List[str]] = None, extra: Optional[List[str]] = None):
        super().__init__(message)
        self.result = result
        self.missing = missing or []
        self.extra = extra or []


class USASpendingHeaderValidator:
    """
    Validates CSV header columns against the expected schema.
    
    The expected column list is sourced from the raw table DDL
    (sql/00_s1_raw/010_prime_awards_raw.sql).
    
    Attributes:
        EXPECTED_COLUMNS: The authoritative list of 54 columns expected in CSV files.
                         Order matters for positional validation.
    """
    
    # Authoritative column list from sql/00_s1_raw/010_prime_awards_raw.sql
    # Only includes CSV data columns (excludes ETL metadata columns)
    EXPECTED_COLUMNS = [
        # Business keys
        "contract_transaction_unique_key",
        "contract_award_unique_key",
        
        # Date and fiscal fields
        "action_date_fiscal_year",
        "action_date",
        
        # Award identification
        "parent_award_id_piid",
        "award_id_piid", 
        "modification_number",
        
        # Financial fields
        "federal_action_obligation",
        "total_dollars_obligated",
        "potential_total_value_of_award",
        "total_outlayed_amount_for_overall_award",
        
        # Performance period dates
        "period_of_performance_start_date",
        "period_of_performance_current_end_date",
        "period_of_performance_potential_end_date",
        "ordering_period_end_date",
        
        # Place of performance
        "primary_place_of_performance_city_name",
        "primary_place_of_performance_state_code",
        
        # Descriptions
        "prime_award_base_transaction_description",
        "transaction_description",
        
        # Industry classification
        "naics_code",
        "naics_description",
        "product_or_service_code",
        "product_or_service_code_description",
        "dod_acquisition_program_description",
        
        # Agency information
        "parent_award_agency_name",
        "awarding_sub_agency_name",
        "awarding_office_name",
        "funding_agency_name",
        "funding_sub_agency_name",
        "funding_office_name",
        
        # Recipient information
        "recipient_name",
        "recipient_uei",
        "recipient_parent_name",
        "recipient_parent_uei",
        
        # Solicitation details
        "solicitation_date",
        "solicitation_identifier",
        "solicitation_procedures",
        
        # Competition details
        "extent_competed",
        "type_of_set_aside",
        "fair_opportunity_limited_sources",
        "other_than_full_and_open_competition",
        "number_of_offers_received",
        
        # Contract details
        "subcontracting_plan",
        "government_furnished_property",
        "type_of_contract_pricing",
        "action_type",
        "award_type",
        "type_of_idc",
        "idv_type",
        "undefinitized_action",
        "program_acronym",
        "multi_year_contract",
        "multiple_or_single_award_idv",
        
        # USASpending metadata
        "usaspending_permalink"
    ]
    
    @classmethod
    def validate_headers(cls, csv_headers: List[str], strict: bool = True) -> Tuple[ValidationResult, str]:
        """
        Validate CSV headers against expected schema.
        
        Args:
            csv_headers: List of column headers from CSV file
            strict: If True, requires exact match. If False, allows extra columns.
                   
        Returns:
            Tuple of (ValidationResult, descriptive_message)
            
        Raises:
            HeaderValidationError: When validation fails and detailed error info is needed
        """
        # Normalize headers (strip whitespace, case-sensitive comparison)
        normalized_headers = [h.strip() for h in csv_headers]
        expected_set = set(cls.EXPECTED_COLUMNS)
        actual_set = set(normalized_headers)
        
        # Find missing and extra columns
        missing = expected_set - actual_set
        extra = actual_set - expected_set
        
        logger.debug(f"Header validation: expected={len(expected_set)}, actual={len(actual_set)}, missing={len(missing)}, extra={len(extra)}")
        
        # Check for missing required columns
        if missing:
            missing_list = sorted(list(missing))
            message = f"Missing required columns: {missing_list}"
            logger.error(message)
            raise HeaderValidationError(
                ValidationResult.MISSING_REQUIRED, 
                message, 
                missing=missing_list
            )
        
        # Check for extra columns in strict mode
        if strict and extra:
            extra_list = sorted(list(extra))
            message = f"Unexpected extra columns in strict mode: {extra_list}"
            logger.error(message)
            raise HeaderValidationError(
                ValidationResult.EXTRA_COLUMNS,
                message,
                extra=extra_list
            )
        
        # Check positional order (if headers match expected count)
        if len(normalized_headers) == len(cls.EXPECTED_COLUMNS):
            for i, (expected, actual) in enumerate(zip(cls.EXPECTED_COLUMNS, normalized_headers)):
                if expected != actual:
                    message = f"Column mismatch at position {i}: expected '{expected}', got '{actual}'"
                    logger.error(message)
                    raise HeaderValidationError(ValidationResult.MISMATCH, message)
        
        # Success case
        if extra and not strict:
            message = f"Headers valid with {len(extra)} extra columns (non-strict mode)"
            logger.info(message)
        else:
            message = f"Headers valid: all {len(cls.EXPECTED_COLUMNS)} expected columns found"
            logger.info(message)
            
        return ValidationResult.VALID, message
    
    @classmethod
    def get_expected_headers(cls) -> List[str]:
        """Get the list of expected column headers."""
        return cls.EXPECTED_COLUMNS.copy()
    
    @classmethod
    def get_expected_count(cls) -> int:
        """Get the number of expected columns."""
        return len(cls.EXPECTED_COLUMNS)
    
    @classmethod
    def is_valid_header_name(cls, header_name: str) -> bool:
        """Check if a header name is in the expected set."""
        return header_name.strip() in cls.EXPECTED_COLUMNS
    
    @classmethod
    def validate_header_subset(cls, csv_headers: List[str], required_subset: List[str]) -> Tuple[bool, List[str]]:
        """
        Validate that CSV contains a required subset of headers.
        
        Args:
            csv_headers: List of column headers from CSV file
            required_subset: List of required column names (subset of EXPECTED_COLUMNS)
            
        Returns:
            Tuple of (is_valid, missing_from_subset)
        """
        normalized_headers = set(h.strip() for h in csv_headers)
        required_set = set(required_subset)
        
        # Verify required_subset is valid
        invalid_required = required_set - set(cls.EXPECTED_COLUMNS)
        if invalid_required:
            raise ValueError(f"Required subset contains invalid columns: {sorted(list(invalid_required))}")
        
        missing = required_set - normalized_headers
        return len(missing) == 0, sorted(list(missing))