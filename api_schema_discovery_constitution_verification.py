#!/usr/bin/env python3
"""
USASpending Bulk Award API Schema Discovery

This script uses the Data Insights repo patterns to properly query the USASpending 
Bulk Award API and verify that ALL constitution fields are available, including 
solicitation_identifier.

Constitution is gospel - we verify API compatibility, not change requirements.
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set

# API Configuration (from Data Insights repo)
USASPENDING_API_URL = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
REQUEST_TIMEOUT = 60
DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Constitution TARGET_FIELDS (GOSPEL - these are our requirements)
CONSTITUTION_TARGET_FIELDS = [
    "contract_transaction_unique_key",
    "contract_award_unique_key", 
    "action_date_fiscal_year",
    "action_date",
    "parent_award_id_piid",
    "award_id_piid",
    "modification_number",
    "federal_action_obligation",
    "total_dollars_obligated",
    "potential_total_value_of_award",
    "total_outlayed_amount_for_overall_award",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "period_of_performance_potential_end_date",
    "ordering_period_end_date",
    "primary_place_of_performance_city_name",
    "primary_place_of_performance_state_code",
    "prime_award_base_transaction_description",
    "transaction_description",
    "naics_code",
    "naics_description",
    "product_or_service_code",
    "product_or_service_code_description",
    "dod_acquisition_program_description",
    "parent_award_agency_name",
    "awarding_sub_agency_name",
    "awarding_office_name",
    "funding_agency_name",
    "funding_sub_agency_name",
    "funding_office_name",
    "recipient_name",
    "recipient_uei",
    "recipient_parent_name",
    "recipient_parent_uei",
    "solicitation_date",
    "solicitation_procedures",
    "extent_competed",
    "type_of_set_aside",
    "fair_opportunity_limited_sources",
    "other_than_full_and_open_competition",
    "number_of_offers_received",
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
    "usaspending_permalink",
    "solicitation_identifier"  # CRITICAL: Must verify this field exists
]

def request_small_sample_download():
    """
    Request a small sample download to inspect available fields.
    Uses repo pattern but with minimal date range for schema discovery.
    """
    # Use very recent, small date range for quick response
    end_date = datetime.now()
    start_date = end_date - timedelta(days=2)  # Just 2 days for schema discovery
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    # Payload based on repo pattern (usaspending_primeawards_raw.py)
    payload = {
        "filters": {
            "prime_award_types": [
                "A", "B", "C", "D", "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", 
                "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"  # Procurement only
            ],
            "date_type": "action_date",
            "date_range": {
                "start_date": start_str,
                "end_date": end_str
            },
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "toptier",
                    "name": "All"
                }
            ]
        },
        "file_format": "csv"
        # NOTE: Not specifying "columns" to get ALL available fields for discovery
    }
    
    print(f"Requesting USASpending sample data for schema discovery ({start_str} to {end_str})")
    print(f"API URL: {USASPENDING_API_URL}")
    
    try:
        response = requests.post(
            USASPENDING_API_URL, 
            json=payload, 
            headers=DOWNLOAD_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        result = response.json()
        
        status_url = result.get("status_url")
        file_url = result.get("file_url")
        
        if not status_url or not file_url:
            raise ValueError("No download URL or status URL returned from USAspending API")
        
        print(f"✅ Request submitted successfully")
        print(f"   Status URL: {status_url}")
        print(f"   File URL: {file_url}")
        
        return status_url, file_url
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API Request failed: {e}")
        return None, None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None, None

def wait_for_file_generation(status_url, max_wait_minutes=5):
    """
    Wait for the API to generate the file.
    Based on repo pattern but with shorter timeout for schema discovery.
    """
    if not status_url:
        return False
        
    max_wait_seconds = max_wait_minutes * 60
    start_time = time.time()
    
    print(f"⏳ Waiting for file generation (max {max_wait_minutes} minutes)...")
    
    while (time.time() - start_time) < max_wait_seconds:
        try:
            response = requests.get(status_url, headers=DOWNLOAD_HEADERS, timeout=30)
            response.raise_for_status()
            status_data = response.json()
            
            status = status_data.get("status", "").lower()
            
            if status == "finished":
                print("✅ File generation completed!")
                return True
            elif status in ["failed", "error"]:
                print(f"❌ File generation failed: {status_data}")
                return False
            else:
                print(f"   Status: {status}")
                
        except Exception as e:
            print(f"   Status check error: {e}")
            
        time.sleep(10)  # Check every 10 seconds
    
    print(f"❌ File generation timed out after {max_wait_minutes} minutes")
    return False

def download_and_inspect_schema(file_url):
    """
    Download the generated ZIP and read the first CSV file headers to determine available fields.
    """
    if not file_url:
        return set()

    try:
        print("📥 Downloading generated ZIP file for schema inspection...")
        resp = requests.get(file_url, headers=DOWNLOAD_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        # Ensure it's a ZIP
        content = resp.content
        if not content.startswith(b"PK"):
            print("❌ Expected a ZIP archive but received non-ZIP content")
            print(f"First 100 bytes: {content[:100]}")
            return set()

        import io, zipfile
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_files = [n for n in zf.namelist() if n.lower().endswith('.csv')]
            if not csv_files:
                print("❌ No CSV files found in ZIP")
                return set()
            # Use the first CSV
            csv_name = csv_files[0]
            with zf.open(csv_name) as f:
                # Read only the header line safely
                import csv
                import codecs
                reader = csv.reader(codecs.iterdecode(f, 'utf-8', errors='ignore'))
                try:
                    header = next(reader)
                except StopIteration:
                    print("❌ CSV appears to be empty")
                    return set()
                field_names = [h.strip().strip('"') for h in header]
                print(f"✅ ZIP downloaded and CSV headers parsed ({csv_name})")
                print(f"   Total fields available: {len(field_names)}")
                return set(field_names)

    except Exception as e:
        print(f"❌ Error downloading/inspecting ZIP/CSV: {e}")
        return set()

def verify_constitution_fields(api_fields: Set[str]):
    """
    Verify that all constitution fields are available in the API.
    This is the critical validation step.
    """
    print("\n=== CONSTITUTION FIELD VERIFICATION ===")
    
    constitution_set = set(CONSTITUTION_TARGET_FIELDS)
    
    print(f"Constitution requirements: {len(constitution_set)} fields")
    print(f"API available fields: {len(api_fields)}")
    
    # Check each constitution field
    missing_fields = constitution_set - api_fields
    available_fields = constitution_set & api_fields
    
    print(f"\n✅ AVAILABLE FIELDS: {len(available_fields)}/{len(constitution_set)}")
    
    if missing_fields:
        print(f"\n❌ MISSING FIELDS IN API: {len(missing_fields)}")
        for field in sorted(missing_fields):
            print(f"   - {field}")
            
        print(f"\n🔍 SOLICITATION_IDENTIFIER STATUS:")
        if "solicitation_identifier" in missing_fields:
            print("   ❌ solicitation_identifier NOT found in API")
            print("   📋 ACTION REQUIRED: Verify if field exists under different name")
        else:
            print("   ✅ solicitation_identifier confirmed in API")
    else:
        print("\n🎉 ALL CONSTITUTION FIELDS AVAILABLE IN API!")
        print("   ✅ solicitation_identifier confirmed")
        print("   ✅ All 50+ fields verified")
    
    # Look for potential solicitation-related fields
    print(f"\n🔍 SOLICITATION-RELATED FIELDS IN API:")
    solicitation_related = [field for field in api_fields if 'solicitation' in field.lower()]
    if solicitation_related:
        for field in sorted(solicitation_related):
            print(f"   • {field}")
    else:
        print("   No solicitation-related fields found")
    
    return {
        "total_constitution_fields": len(constitution_set),
        "available_fields": len(available_fields),
        "missing_fields": list(missing_fields),
        "solicitation_identifier_available": "solicitation_identifier" in available_fields,
        "solicitation_related_fields": solicitation_related,
        "all_fields_available": len(missing_fields) == 0
    }

def main():
    """
    Main schema discovery process.
    """
    print("🔍 USASpending API Schema Discovery for Constitution Verification")
    print("=" * 70)
    
    # Step 1: Request sample download
    status_url, file_url = request_small_sample_download()
    if not status_url:
        print("❌ Failed to initiate API request")
        return False
    
    # Step 2: Wait for file generation
    if not wait_for_file_generation(status_url):
        print("❌ File generation failed or timed out")
        return False
    
    # Step 3: Download and inspect schema
    api_fields = download_and_inspect_schema(file_url)
    if not api_fields:
        print("❌ Failed to inspect API schema")
        return False
    
    # Step 4: Verify constitution requirements
    verification_results = verify_constitution_fields(api_fields)
    
    # Step 5: Summary and recommendations
    print(f"\n=== FINAL ASSESSMENT ===")
    if verification_results["all_fields_available"]:
        print("🎉 SUCCESS: All constitution fields verified in API")
        print("✅ No changes needed to constitution")
        print("✅ Proceed with ETL implementation using constitution as gospel")
    else:
        print("⚠️  ISSUES FOUND: Some constitution fields missing from API")
        print("📋 Next steps:")
        print("   1. Investigate if missing fields exist under different names")
        print("   2. Contact user for clarification on missing fields")
        print("   3. Consider if alternative fields provide same data")
    
    return verification_results["all_fields_available"]

if __name__ == "__main__":
    success = main()