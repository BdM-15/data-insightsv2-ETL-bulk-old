# USASpending Bulk Award API - Constitution Field Verification (2025-09-24)

Purpose: Verify that all constitution-required fields (including `solicitation_identifier`) are available in the USASpending Bulk Award API CSV output.

Summary:

- Date range requested: last 2 days
- Endpoint: https://api.usaspending.gov/api/v2/bulk_download/awards/
- Result: SUCCESS — All 54 required fields present in CSV headers
- Solicitation fields found: `solicitation_date`, `solicitation_identifier`, `solicitation_procedures`, `solicitation_procedures_code`

Verified fields (54):

- contract_transaction_unique_key
- contract_award_unique_key
- action_date_fiscal_year
- action_date
- parent_award_id_piid
- award_id_piid
- modification_number
- federal_action_obligation
- total_dollars_obligated
- potential_total_value_of_award
- total_outlayed_amount_for_overall_award
- period_of_performance_start_date
- period_of_performance_current_end_date
- period_of_performance_potential_end_date
- ordering_period_end_date
- primary_place_of_performance_city_name
- primary_place_of_performance_state_code
- prime_award_base_transaction_description
- transaction_description
- naics_code
- naics_description
- product_or_service_code
- product_or_service_code_description
- dod_acquisition_program_description
- parent_award_agency_name
- awarding_sub_agency_name
- awarding_office_name
- funding_agency_name
- funding_sub_agency_name
- funding_office_name
- recipient_name
- recipient_uei
- recipient_parent_name
- recipient_parent_uei
- solicitation_date
- solicitation_identifier
- solicitation_procedures
- extent_competed
- type_of_set_aside
- fair_opportunity_limited_sources
- other_than_full_and_open_competition
- number_of_offers_received
- subcontracting_plan
- government_furnished_property
- type_of_contract_pricing
- action_type
- award_type
- type_of_idc
- idv_type
- undefinitized_action
- program_acronym
- multi_year_contract
- multiple_or_single_award_idv
- usaspending_permalink

Notes:

- The API provides many additional fields (297 headers total). We restrict to constitution fields for the slim table.
- CSV parsing needed unzipping the generated archive; direct GET of file_url as text truncates headers.
- Keep constitution as authoritative source; repo field list is a reference only.
