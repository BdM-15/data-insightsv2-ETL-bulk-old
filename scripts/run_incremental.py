"""
Incremental Data Pipeline Orchestrator

Coordinates incremental ETL workflow for daily data updates,
processing only new/modified records since the last watermark.

Constitution v1.9.0 | Task: T040-T050
Dependencies: All ETL modules (acquisition, staging, sql, checks)
"""

import logging
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, Tuple
from uuid import uuid4
import json

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.config import get_config
from etl.utils.logging import get_logger
from etl.utils.disk import check_available_space_gb
from etl.acquisition.api_client import USASpendingAPIClient
from etl.staging.raw_loader import RawDataLoader
from etl.staging.progress import ProgressRecorder
from etl.staging.watermark import WatermarkManager
from etl.sql.incremental_merge import IncrementalMergeController
from etl.sql.semantic_description import SemanticDescriptionGenerator
from etl.checks.duplicate_diagnostics import DuplicateDiagnostics

logger = get_logger(__name__)

class IncrementalPipelineOrchestrator:
    """Orchestrate incremental data pipeline execution."""
    
    def __init__(self, pipeline_name: str = "prime_awards_incremental"):
        """Initialize incremental pipeline orchestrator.
        
        Args:
            pipeline_name: Name for this pipeline run
        """
        self.config = get_config()
        self.pipeline_name = pipeline_name
        self.correlation_id = str(uuid4())
        
        # Initialize components
        self.api_client = USASpendingAPIClient()
        self.raw_loader = RawDataLoader()
        self.progress_recorder = ProgressRecorder()
        self.watermark_manager = WatermarkManager(pipeline_name)
        self.incremental_merger = IncrementalMergeController()
        self.semantic_generator = SemanticDescriptionGenerator()
        self.diagnostics = DuplicateDiagnostics()
        
        logger.info("IncrementalPipelineOrchestrator initialized", extra={
            'correlation_id': self.correlation_id,
            'pipeline_name': self.pipeline_name
        })
    
    def determine_processing_window(self, 
                                  force_start_date: Optional[date] = None,
                                  force_end_date: Optional[date] = None) -> Tuple[date, date]:
        """Determine the processing window for incremental updates.
        
        Args:
            force_start_date: Override start date (for testing/recovery)
            force_end_date: Override end date (for testing/recovery)
            
        Returns:
            Tuple of (start_date, end_date) for processing
        """
        logger.info("Determining incremental processing window", extra={
            'correlation_id': self.correlation_id,
            'force_start_date': force_start_date.isoformat() if force_start_date else None,
            'force_end_date': force_end_date.isoformat() if force_end_date else None
        })
        
        if force_start_date and force_end_date:
            logger.info("Using forced date range", extra={
                'correlation_id': self.correlation_id,
                'start_date': force_start_date.isoformat(),
                'end_date': force_end_date.isoformat()
            })
            return force_start_date, force_end_date
        
        # Get current watermark
        watermark_info = self.watermark_manager.get_current_watermark()
        
        if watermark_info.last_modified_to is None:
            # No watermark exists - this shouldn't happen for incremental
            logger.warning("No watermark found for incremental processing")
            raise ValueError(
                "No watermark found - run historical pipeline first or use forced date range"
            )
        
        # Calculate processing window with overlap
        start_date = (watermark_info.last_modified_to - 
                     timedelta(days=self.config.current_days_lookback)).date()
        
        if force_end_date:
            end_date = force_end_date
        else:
            # Process through yesterday to avoid incomplete current day data
            end_date = (datetime.now() - timedelta(days=1)).date()
        
        # Validate window
        if start_date >= end_date:
            logger.info("No incremental processing needed - data is current", extra={
                'correlation_id': self.correlation_id,
                'watermark_date': watermark_info.last_modified_to.date().isoformat(),
                'calculated_end_date': end_date.isoformat()
            })
            raise ValueError("No incremental processing needed - data is current")
        
        logger.info("Incremental processing window determined", extra={
            'correlation_id': self.correlation_id,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'days': (end_date - start_date).days + 1,
            'watermark_date': watermark_info.last_modified_to.date().isoformat()
        })
        
        return start_date, end_date
    
    def validate_incremental_prerequisites(self) -> Dict[str, Any]:
        """Validate prerequisites specific to incremental processing.
        
        Returns:
            Dict with validation results
        """
        logger.info("Validating incremental prerequisites", extra={
            'correlation_id': self.correlation_id
        })
        
        validation_results = {
            'correlation_id': self.correlation_id,
            'validation_checks': [],
            'overall_status': 'pending',
            'can_proceed': False
        }
        
        try:
            # Check disk space
            available_space = check_available_space_gb()
            space_check = {
                'check': 'disk_space',
                'passed': available_space >= self.config.min_free_space_gb / 2,  # Incremental needs less
                'available_gb': available_space,
                'required_gb': self.config.min_free_space_gb / 2
            }
            validation_results['validation_checks'].append(space_check)
            
            # Check watermark exists
            watermark_check = {
                'check': 'watermark_exists',
                'passed': False
            }
            try:
                watermark_info = self.watermark_manager.get_current_watermark()
                watermark_check['passed'] = watermark_info.last_modified_to is not None
                if watermark_check['passed']:
                    watermark_check['current_watermark'] = watermark_info.last_modified_to.isoformat()
                else:
                    watermark_check['error'] = 'No watermark found'
            except Exception as e:
                watermark_check['error'] = str(e)
            
            validation_results['validation_checks'].append(watermark_check)
            
            # Check processed tables exist
            tables_check = {
                'check': 'processed_tables_exist',
                'passed': False
            }
            try:
                table_status = self.incremental_merger.validate_target_tables()
                tables_check['passed'] = table_status.get('all_tables_exist', False)
                tables_check['table_count'] = table_status.get('existing_tables', 0)
                if not tables_check['passed']:
                    tables_check['missing_tables'] = table_status.get('missing_tables', [])
            except Exception as e:
                tables_check['error'] = str(e)
            
            validation_results['validation_checks'].append(tables_check)
            
            # Check API connectivity
            api_check = {
                'check': 'api_connectivity',
                'passed': False
            }
            try:
                # Test API with a small request
                test_response = self.api_client.get_awards_data(
                    date_range=('2024-01-01', '2024-01-02'),
                    endpoint_type='prime_awards',
                    limit=1
                )
                api_check['passed'] = test_response.get('response_status') == 'success'
                api_check['test_response_status'] = test_response.get('response_status')
            except Exception as e:
                api_check['error'] = str(e)
            
            validation_results['validation_checks'].append(api_check)
            
            # Check incremental merge functionality
            merge_check = {
                'check': 'incremental_merge_functions',
                'passed': False
            }
            try:
                merge_validation = self.incremental_merger.validate_merge_functions()
                merge_check['passed'] = merge_validation.get('overall_status') == 'passed'
                merge_check['functions_validated'] = merge_validation.get('functions_validated', 0)
                if not merge_check['passed']:
                    merge_check['validation_errors'] = merge_validation.get('validation_errors', [])
            except Exception as e:
                merge_check['error'] = str(e)
            
            validation_results['validation_checks'].append(merge_check)
            
            # Determine overall status
            failed_checks = [check for check in validation_results['validation_checks'] if not check['passed']]
            validation_results['overall_status'] = 'passed' if len(failed_checks) == 0 else 'failed'
            validation_results['can_proceed'] = validation_results['overall_status'] == 'passed'
            validation_results['failed_checks'] = len(failed_checks)
            
            logger.info("Incremental prerequisites validation completed", extra={
                'correlation_id': self.correlation_id,
                'overall_status': validation_results['overall_status'],
                'failed_checks': validation_results['failed_checks']
            })
            
            return validation_results
            
        except Exception as e:
            validation_results.update({
                'overall_status': 'error',
                'error': str(e),
                'can_proceed': False
            })
            
            logger.error("Incremental prerequisites validation failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_incremental_acquisition(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Execute incremental data acquisition.
        
        Args:
            start_date: Start date for acquisition
            end_date: End date for acquisition
            
        Returns:
            Dict with acquisition results
        """
        logger.info("Starting incremental data acquisition", extra={
            'correlation_id': self.correlation_id,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        })
        
        phase_start_time = time.time()
        
        acquisition_results = {
            'phase': 'incremental_acquisition',
            'correlation_id': self.correlation_id,
            'processing_window': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': (end_date - start_date).days + 1
            }
        }
        
        try:
            # Acquire prime awards data
            logger.info("Acquiring prime awards data")
            prime_awards_result = self.api_client.get_awards_data(
                date_range=(start_date.isoformat(), end_date.isoformat()),
                endpoint_type='prime_awards'
            )
            acquisition_results['prime_awards'] = prime_awards_result
            
            # Acquire subawards data  
            logger.info("Acquiring subawards data")
            subawards_result = self.api_client.get_awards_data(
                date_range=(start_date.isoformat(), end_date.isoformat()),
                endpoint_type='subawards'
            )
            acquisition_results['subawards'] = subawards_result
            
            # Calculate totals
            total_records = (
                prime_awards_result.get('total_rows', 0) + 
                subawards_result.get('total_rows', 0)
            )
            
            acquisition_results.update({
                'total_records_acquired': total_records,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            })
            
            # Record progress
            self.progress_recorder.record_chunk_progress(
                pipeline_name=self.pipeline_name,
                chunk_window=(start_date.isoformat(), end_date.isoformat()),
                chunk_index=0,
                status='success',
                rows_staged=total_records,
                correlation_id=self.correlation_id
            )
            
            logger.info("Incremental data acquisition completed", extra={
                'correlation_id': self.correlation_id,
                'total_records': total_records
            })
            
            return acquisition_results
            
        except Exception as e:
            acquisition_results.update({
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Incremental data acquisition failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_incremental_staging(self, acquisition_results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute incremental data staging to temporary tables.
        
        Args:
            acquisition_results: Results from acquisition phase
            
        Returns:
            Dict with staging results
        """
        logger.info("Starting incremental data staging", extra={
            'correlation_id': self.correlation_id
        })
        
        phase_start_time = time.time()
        
        staging_results = {
            'phase': 'incremental_staging',
            'correlation_id': self.correlation_id,
            'load_results': []
        }
        
        try:
            # Load prime awards to temporary staging table
            if acquisition_results.get('prime_awards', {}).get('archive_path'):
                logger.info("Loading prime awards to temporary staging")
                prime_load_result = self.raw_loader.load_csv_to_raw_table(
                    csv_file_path=acquisition_results['prime_awards']['archive_path'],
                    table_name='s1_raw_usaspending_prime_awards_slimv2_temp',  # Temporary table
                    correlation_id=self.correlation_id
                )
                staging_results['load_results'].append({
                    'entity_type': 'prime_awards',
                    'temporary_table': 's1_raw_usaspending_prime_awards_slimv2_temp',
                    'result': prime_load_result
                })
            
            # Load subawards to temporary staging table
            if acquisition_results.get('subawards', {}).get('archive_path'):
                logger.info("Loading subawards to temporary staging")
                subaward_load_result = self.raw_loader.load_csv_to_raw_table(
                    csv_file_path=acquisition_results['subawards']['archive_path'],
                    table_name='s1_raw_usaspending_subawards_v2_temp',  # Temporary table
                    correlation_id=self.correlation_id
                )
                staging_results['load_results'].append({
                    'entity_type': 'subawards',
                    'temporary_table': 's1_raw_usaspending_subawards_v2_temp',
                    'result': subaward_load_result
                })
            
            # Calculate totals
            total_records_loaded = sum(
                result['result'].get('rows_loaded', 0) 
                for result in staging_results['load_results']
            )
            
            staging_results.update({
                'total_records_loaded': total_records_loaded,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info("Incremental data staging completed", extra={
                'correlation_id': self.correlation_id,
                'total_records_loaded': total_records_loaded
            })
            
            return staging_results
            
        except Exception as e:
            staging_results.update({
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Incremental data staging failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_incremental_merge(self, staging_results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute incremental merge from temporary tables to main tables.
        
        Args:
            staging_results: Results from staging phase
            
        Returns:
            Dict with merge results
        """
        logger.info("Starting incremental merge", extra={
            'correlation_id': self.correlation_id
        })
        
        phase_start_time = time.time()
        
        merge_results = {
            'phase': 'incremental_merge',
            'correlation_id': self.correlation_id,
            'merge_operations': []
        }
        
        try:
            # Merge each staged entity type
            for load_result in staging_results['load_results']:
                entity_type = load_result['entity_type']
                temp_table = load_result['temporary_table']
                
                logger.info(f"Merging {entity_type} from temporary table", extra={
                    'correlation_id': self.correlation_id,
                    'entity_type': entity_type,
                    'temporary_table': temp_table
                })
                
                # Execute merge based on entity type
                if entity_type == 'prime_awards':
                    merge_result = self.incremental_merger.merge_prime_awards_incremental(
                        temp_table_name=temp_table,
                        correlation_id=self.correlation_id
                    )
                elif entity_type == 'subawards':
                    merge_result = self.incremental_merger.merge_subawards_incremental(
                        temp_table_name=temp_table,
                        correlation_id=self.correlation_id
                    )
                else:
                    raise ValueError(f"Unknown entity type: {entity_type}")
                
                merge_results['merge_operations'].append({
                    'entity_type': entity_type,
                    'temporary_table': temp_table,
                    'result': merge_result
                })
            
            # Calculate merge totals
            total_records_merged = sum(
                op['result'].get('total_records_processed', 0) 
                for op in merge_results['merge_operations']
            )
            
            total_records_inserted = sum(
                op['result'].get('records_inserted', 0) 
                for op in merge_results['merge_operations']
            )
            
            total_records_updated = sum(
                op['result'].get('records_updated', 0) 
                for op in merge_results['merge_operations']
            )
            
            merge_results.update({
                'total_records_merged': total_records_merged,
                'total_records_inserted': total_records_inserted,
                'total_records_updated': total_records_updated,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info("Incremental merge completed", extra={
                'correlation_id': self.correlation_id,
                'total_merged': total_records_merged,
                'inserted': total_records_inserted,
                'updated': total_records_updated
            })
            
            return merge_results
            
        except Exception as e:
            merge_results.update({
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Incremental merge failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_incremental_validation(self) -> Dict[str, Any]:
        """Execute validation for incremental processing.
        
        Returns:
            Dict with validation results
        """
        logger.info("Starting incremental validation", extra={
            'correlation_id': self.correlation_id
        })
        
        phase_start_time = time.time()
        
        try:
            # Run focused diagnostics on recent data
            logger.info("Running recent data diagnostics")
            recent_diagnostics = self.diagnostics.run_recent_data_diagnostics(
                days_back=self.config.current_days_lookback * 2
            )
            
            # Update semantic descriptions for modified records
            logger.info("Updating semantic descriptions for recent records")
            semantic_results = self.semantic_generator.update_recent_semantic_descriptions(
                days_back=self.config.current_days_lookback * 2
            )
            
            validation_results = {
                'phase': 'incremental_validation',
                'correlation_id': self.correlation_id,
                'recent_diagnostics': recent_diagnostics,
                'semantic_updates': semantic_results,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            }
            
            # Check for issues
            diagnostics_passed = recent_diagnostics.get('summary', {}).get('tables_with_issues', 1) == 0
            semantic_passed = semantic_results.get('overall_status') == 'success'
            
            if not (diagnostics_passed and semantic_passed):
                validation_results['overall_status'] = 'warnings'
                validation_results['validation_warnings'] = []
                
                if not diagnostics_passed:
                    validation_results['validation_warnings'].append(
                        f"Recent data diagnostics found issues in {recent_diagnostics['summary']['tables_with_issues']} tables"
                    )
                
                if not semantic_passed:
                    validation_results['validation_warnings'].append(
                        f"Semantic description updates had issues: {semantic_results.get('error', 'Unknown error')}"
                    )
            
            logger.info("Incremental validation completed", extra={
                'correlation_id': self.correlation_id,
                'overall_status': validation_results['overall_status']
            })
            
            return validation_results
            
        except Exception as e:
            validation_results = {
                'phase': 'incremental_validation',
                'correlation_id': self.correlation_id,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            }
            
            logger.error("Incremental validation failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_complete_incremental(self, 
                                   force_start_date: Optional[date] = None,
                                   force_end_date: Optional[date] = None,
                                   skip_validation: bool = False) -> Dict[str, Any]:
        """Execute complete incremental pipeline.
        
        Args:
            force_start_date: Override start date (for testing/recovery)
            force_end_date: Override end date (for testing/recovery) 
            skip_validation: Skip validation phase for faster execution
            
        Returns:
            Dict with complete pipeline results
        """
        logger.info("Starting complete incremental pipeline execution", extra={
            'correlation_id': self.correlation_id,
            'force_start_date': force_start_date.isoformat() if force_start_date else None,
            'force_end_date': force_end_date.isoformat() if force_end_date else None
        })
        
        pipeline_start_time = time.time()
        
        pipeline_results = {
            'pipeline_type': 'incremental',
            'correlation_id': self.correlation_id,
            'pipeline_name': self.pipeline_name,
            'start_time': pipeline_start_time,
            'phase_results': {},
            'overall_status': 'pending'
        }
        
        try:
            # Phase 1: Prerequisites validation
            logger.info("Phase 1: Validating incremental prerequisites")
            prereq_results = self.validate_incremental_prerequisites()
            pipeline_results['phase_results']['prerequisites'] = prereq_results
            
            if not prereq_results['can_proceed']:
                raise RuntimeError("Incremental prerequisites validation failed - cannot proceed")
            
            # Phase 2: Determine processing window
            logger.info("Phase 2: Determining processing window")
            start_date, end_date = self.determine_processing_window(force_start_date, force_end_date)
            
            pipeline_results['processing_window'] = {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': (end_date - start_date).days + 1
            }
            
            # Phase 3: Incremental acquisition
            logger.info("Phase 3: Incremental data acquisition")
            acquisition_results = self.execute_incremental_acquisition(start_date, end_date)
            pipeline_results['phase_results']['acquisition'] = acquisition_results
            
            # Phase 4: Incremental staging
            logger.info("Phase 4: Incremental data staging")
            staging_results = self.execute_incremental_staging(acquisition_results)
            pipeline_results['phase_results']['staging'] = staging_results
            
            # Phase 5: Incremental merge
            logger.info("Phase 5: Incremental merge")
            merge_results = self.execute_incremental_merge(staging_results)
            pipeline_results['phase_results']['merge'] = merge_results
            
            # Phase 6: Validation (optional)
            if not skip_validation:
                logger.info("Phase 6: Incremental validation")
                validation_results = self.execute_incremental_validation()
                pipeline_results['phase_results']['validation'] = validation_results
            
            # Phase 7: Update watermark
            logger.info("Phase 7: Updating watermark")
            new_watermark = datetime.combine(end_date, datetime.min.time())
            watermark_info = self.watermark_manager.set_watermark(
                last_modified_to=new_watermark,
                overlap_days=self.config.current_days_lookback
            )
            
            pipeline_results.update({
                'overall_status': 'success',
                'total_execution_time_seconds': round(time.time() - pipeline_start_time, 3),
                'new_watermark': watermark_info.last_modified_to.isoformat(),
                'total_records_processed': merge_results.get('total_records_merged', 0),
                'records_inserted': merge_results.get('total_records_inserted', 0),
                'records_updated': merge_results.get('total_records_updated', 0)
            })
            
            logger.info("Complete incremental pipeline execution completed successfully", extra={
                'correlation_id': self.correlation_id,
                'total_execution_time': pipeline_results['total_execution_time_seconds'],
                'records_processed': pipeline_results['total_records_processed'],
                'records_inserted': pipeline_results['records_inserted'],
                'records_updated': pipeline_results['records_updated']
            })
            
            return pipeline_results
            
        except Exception as e:
            pipeline_results.update({
                'overall_status': 'failed',
                'total_execution_time_seconds': round(time.time() - pipeline_start_time, 3),
                'error': str(e),
                'error_type': type(e).__name__
            })
            
            logger.error("Complete incremental pipeline execution failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise


def main():
    """CLI entry point for incremental pipeline execution."""
    parser = argparse.ArgumentParser(description="Execute incremental ETL pipeline")
    parser.add_argument('--start-date', type=str,
                       help='Force start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str,
                       help='Force end date for processing (YYYY-MM-DD)')
    parser.add_argument('--pipeline-name', type=str, default='prime_awards_incremental',
                       help='Pipeline name (default: prime_awards_incremental)')
    parser.add_argument('--skip-validation', action='store_true',
                       help='Skip validation phase for faster execution')
    parser.add_argument('--output-file', type=str,
                       help='Save results to JSON file')
    parser.add_argument('--dry-run', action='store_true',
                       help='Validate prerequisites and show processing window only')
    
    args = parser.parse_args()
    
    try:
        # Parse dates if provided
        force_start_date = None
        force_end_date = None
        
        if args.start_date:
            force_start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        
        if args.end_date:
            force_end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        
        if force_start_date and force_end_date and force_start_date >= force_end_date:
            print("❌ Start date must be before end date")
            exit(1)
        
        # Initialize orchestrator
        orchestrator = IncrementalPipelineOrchestrator(args.pipeline_name)
        
        print(f"🔄 Incremental Pipeline Orchestrator")
        print(f"📊 Correlation ID: {orchestrator.correlation_id}")
        print(f"🔧 Pipeline name: {args.pipeline_name}")
        
        # Validate prerequisites
        print(f"\n🧪 Validating incremental prerequisites...")
        prereq_results = orchestrator.validate_incremental_prerequisites()
        
        print(f"✅ Prerequisites validation: {prereq_results['overall_status'].upper()}")
        print(f"📋 Checks: {len(prereq_results['validation_checks']) - prereq_results['failed_checks']}/{len(prereq_results['validation_checks'])} passed")
        
        if prereq_results['failed_checks'] > 0:
            print(f"\n❌ Failed checks:")
            for check in prereq_results['validation_checks']:
                if not check['passed']:
                    print(f"   - {check['check']}: {check.get('error', 'Failed')}")
            exit(1)
        
        # Determine processing window
        try:
            start_date, end_date = orchestrator.determine_processing_window(force_start_date, force_end_date)
            print(f"\n📅 Processing window: {start_date} to {end_date} ({(end_date - start_date).days + 1} days)")
            
            if args.dry_run:
                print(f"🧪 Dry-run completed - incremental pipeline ready to execute")
                exit(0)
        
        except ValueError as e:
            print(f"ℹ️  {e}")
            exit(0)
        
        # Execute complete incremental pipeline
        print(f"\n🔄 Executing complete incremental pipeline...")
        results = orchestrator.execute_complete_incremental(
            force_start_date=force_start_date,
            force_end_date=force_end_date,
            skip_validation=args.skip_validation
        )
        
        print(f"\n✅ Pipeline execution completed: {results['overall_status'].upper()}")
        print(f"⏱️  Total execution time: {results['total_execution_time_seconds']}s")
        print(f"📈 Total records processed: {results.get('total_records_processed', 0):,}")
        print(f"➕ Records inserted: {results.get('records_inserted', 0):,}")
        print(f"🔄 Records updated: {results.get('records_updated', 0):,}")
        print(f"💧 New watermark: {results.get('new_watermark', 'N/A')}")
        
        # Print phase summaries
        print(f"\n📋 Phase Summary:")
        for phase_name, phase_result in results['phase_results'].items():
            status = phase_result.get('overall_status', 'unknown')
            exec_time = phase_result.get('phase_execution_time_seconds', 0)
            print(f"   {phase_name.title()}: {status.upper()} ({exec_time}s)")
        
        # Save results if requested
        if args.output_file:
            output_path = Path(args.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"💾 Results saved to: {output_path}")
        
        # Check for warnings
        validation_result = results['phase_results'].get('validation', {})
        if validation_result.get('overall_status') == 'warnings':
            print(f"\n⚠️  Validation warnings:")
            for warning in validation_result.get('validation_warnings', []):
                print(f"   - {warning}")
    
    except Exception as e:
        print(f"❌ Incremental pipeline execution failed: {e}")
        logger.exception("Incremental pipeline execution failed")
        exit(1)


if __name__ == '__main__':
    main()