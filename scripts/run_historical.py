"""
Historical Data Pipeline Orchestrator

Coordinates the complete historical ETL workflow from API acquisition
through processed layer transformation for initial data loading.

Constitution v1.9.0 | Task: T040-T050
Dependencies: All ETL modules (acquisition, staging, sql, checks)
"""

import logging
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, List
from uuid import uuid4
import json

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.config import get_config
from etl.utils.logging import get_logger
from etl.utils.disk import check_available_space_gb
from etl.acquisition.api_client import USASpendingAPIClient
from etl.acquisition.chunk_planner import ChunkPlanner
from etl.staging.raw_loader import RawDataLoader
from etl.staging.progress import ProgressRecorder
from etl.staging.fail_fast import FailFastController
from etl.staging.watermark import WatermarkManager
from etl.sql.transform_interim import InterimTransformer
from etl.sql.semantic_description import SemanticDescriptionGenerator
from etl.sql.transform_processed import ProcessedLayerTransform
from etl.checks.duplicate_diagnostics import DuplicateDiagnostics

logger = get_logger(__name__)

class HistoricalPipelineOrchestrator:
    """Orchestrate complete historical data pipeline execution."""
    
    def __init__(self, pipeline_name: str = "prime_awards_historical"):
        """Initialize historical pipeline orchestrator.
        
        Args:
            pipeline_name: Name for this pipeline run
        """
        self.config = get_config()
        self.pipeline_name = pipeline_name
        self.correlation_id = str(uuid4())
        
        # Initialize all components
        self.api_client = USASpendingAPIClient()
        self.chunk_planner = ChunkPlanner()
        self.raw_loader = RawDataLoader()
        self.progress_recorder = ProgressRecorder()
        self.fail_fast_controller = FailFastController()
        self.watermark_manager = WatermarkManager(pipeline_name)
        self.interim_transformer = InterimTransformer()
        self.semantic_generator = SemanticDescriptionGenerator()
        self.processed_transformer = ProcessedLayerTransform()
        self.diagnostics = DuplicateDiagnostics()
        
        logger.info("HistoricalPipelineOrchestrator initialized", extra={
            'correlation_id': self.correlation_id,
            'pipeline_name': self.pipeline_name
        })
    
    def validate_prerequisites(self) -> Dict[str, Any]:
        """Validate system prerequisites before pipeline execution.
        
        Returns:
            Dict with validation results
        """
        logger.info("Validating pipeline prerequisites", extra={
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
                'passed': available_space >= self.config.min_free_space_gb,
                'available_gb': available_space,
                'required_gb': self.config.min_free_space_gb
            }
            validation_results['validation_checks'].append(space_check)
            
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
            
            # Check database connectivity
            db_check = {
                'check': 'database_connectivity',
                'passed': False
            }
            try:
                # Test progress recording (which tests DB connection)
                self.progress_recorder.get_pipeline_status(self.pipeline_name)
                db_check['passed'] = True
            except Exception as e:
                db_check['error'] = str(e)
            
            validation_results['validation_checks'].append(db_check)
            
            # Check clean_description function
            desc_check = {
                'check': 'semantic_description_function',
                'passed': False
            }
            try:
                verification = self.semantic_generator.verify_clean_description_function()
                desc_check['passed'] = verification['overall_status'] == 'passed'
                desc_check['tests_passed'] = verification.get('tests_passed', 0)
                desc_check['tests_failed'] = verification.get('tests_failed', 0)
            except Exception as e:
                desc_check['error'] = str(e)
            
            validation_results['validation_checks'].append(desc_check)
            
            # Determine overall status
            failed_checks = [check for check in validation_results['validation_checks'] if not check['passed']]
            validation_results['overall_status'] = 'passed' if len(failed_checks) == 0 else 'failed'
            validation_results['can_proceed'] = validation_results['overall_status'] == 'passed'
            validation_results['failed_checks'] = len(failed_checks)
            
            logger.info("Prerequisites validation completed", extra={
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
            
            logger.error("Prerequisites validation failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_acquisition_phase(self, start_date: date, end_date: date, 
                                chunk_size_days: int = 30) -> Dict[str, Any]:
        """Execute data acquisition phase.
        
        Args:
            start_date: Start date for data acquisition
            end_date: End date for data acquisition
            chunk_size_days: Size of each processing chunk in days
            
        Returns:
            Dict with acquisition results
        """
        logger.info("Starting data acquisition phase", extra={
            'correlation_id': self.correlation_id,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'chunk_size_days': chunk_size_days
        })
        
        phase_start_time = time.time()
        
        # Generate processing chunks
        chunks = self.chunk_planner.generate_date_chunks(
            start_date=start_date,
            end_date=end_date,
            chunk_size_days=chunk_size_days
        )
        
        acquisition_results = {
            'phase': 'acquisition',
            'correlation_id': self.correlation_id,
            'total_chunks': len(chunks),
            'successful_chunks': 0,
            'failed_chunks': 0,
            'chunk_results': [],
            'total_records_acquired': 0,
            'total_archives_downloaded': 0
        }
        
        for chunk_index, chunk in enumerate(chunks):
            logger.info(f"Processing acquisition chunk {chunk_index + 1}/{len(chunks)}", extra={
                'correlation_id': self.correlation_id,
                'chunk_start': chunk['start_date'],
                'chunk_end': chunk['end_date']
            })
            
            try:
                # Record chunk start
                self.progress_recorder.record_chunk_progress(
                    pipeline_name=self.pipeline_name,
                    chunk_window=(chunk['start_date'], chunk['end_date']),
                    chunk_index=chunk_index,
                    status='in_progress',
                    correlation_id=self.correlation_id
                )
                
                # Acquire prime awards data
                prime_awards_result = self.api_client.get_awards_data(
                    date_range=(chunk['start_date'], chunk['end_date']),
                    endpoint_type='prime_awards'
                )
                
                # Acquire subawards data  
                subawards_result = self.api_client.get_awards_data(
                    date_range=(chunk['start_date'], chunk['end_date']),
                    endpoint_type='subawards'
                )
                
                chunk_result = {
                    'chunk_index': chunk_index,
                    'chunk_window': chunk,
                    'prime_awards': prime_awards_result,
                    'subawards': subawards_result,
                    'status': 'success'
                }
                
                # Update progress
                total_rows = (
                    prime_awards_result.get('total_rows', 0) + 
                    subawards_result.get('total_rows', 0)
                )
                
                self.progress_recorder.record_chunk_progress(
                    pipeline_name=self.pipeline_name,
                    chunk_window=(chunk['start_date'], chunk['end_date']),
                    chunk_index=chunk_index,
                    status='success',
                    rows_staged=total_rows,
                    correlation_id=self.correlation_id
                )
                
                acquisition_results['successful_chunks'] += 1
                acquisition_results['total_records_acquired'] += total_rows
                acquisition_results['total_archives_downloaded'] += 2  # Prime + subawards
                
                logger.info(f"Chunk {chunk_index + 1} acquisition completed", extra={
                    'correlation_id': self.correlation_id,
                    'chunk_records': total_rows
                })
                
            except Exception as e:
                logger.error(f"Chunk {chunk_index + 1} acquisition failed", extra={
                    'correlation_id': self.correlation_id,
                    'error': str(e)
                })
                
                # Record failure
                self.progress_recorder.record_chunk_progress(
                    pipeline_name=self.pipeline_name,
                    chunk_window=(chunk['start_date'], chunk['end_date']),
                    chunk_index=chunk_index,
                    status='failed',
                    error_class=type(e).__name__,
                    error_message=str(e),
                    correlation_id=self.correlation_id
                )
                
                chunk_result = {
                    'chunk_index': chunk_index,
                    'chunk_window': chunk,
                    'status': 'failed',
                    'error': str(e)
                }
                
                acquisition_results['failed_chunks'] += 1
                
                # Check fail-fast conditions
                if self.fail_fast_controller.should_fail_fast(
                    total_chunks=len(chunks),
                    failed_chunks=acquisition_results['failed_chunks']
                ):
                    logger.error("Fail-fast threshold reached, aborting acquisition")
                    raise RuntimeError(f"Acquisition failed: {acquisition_results['failed_chunks']} chunks failed")
            
            acquisition_results['chunk_results'].append(chunk_result)
        
        acquisition_results.update({
            'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
            'overall_status': 'success' if acquisition_results['failed_chunks'] == 0 else 'partial_success'
        })
        
        logger.info("Data acquisition phase completed", extra={
            'correlation_id': self.correlation_id,
            'successful_chunks': acquisition_results['successful_chunks'],
            'failed_chunks': acquisition_results['failed_chunks'],
            'total_records': acquisition_results['total_records_acquired']
        })
        
        return acquisition_results
    
    def execute_staging_phase(self, acquisition_results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data staging and raw loading phase.
        
        Args:
            acquisition_results: Results from acquisition phase
            
        Returns:
            Dict with staging results
        """
        logger.info("Starting data staging phase", extra={
            'correlation_id': self.correlation_id
        })
        
        phase_start_time = time.time()
        
        staging_results = {
            'phase': 'staging',
            'correlation_id': self.correlation_id,
            'total_files_processed': 0,
            'total_records_loaded': 0,
            'load_results': []
        }
        
        try:
            # Process each successful chunk from acquisition
            successful_chunks = [
                chunk for chunk in acquisition_results['chunk_results'] 
                if chunk['status'] == 'success'
            ]
            
            for chunk_result in successful_chunks:
                # Load prime awards data
                if 'prime_awards' in chunk_result and chunk_result['prime_awards'].get('archive_path'):
                    prime_load_result = self.raw_loader.load_csv_to_raw_table(
                        csv_file_path=chunk_result['prime_awards']['archive_path'],
                        table_name='s1_raw_usaspending_prime_awards_slimv2',
                        correlation_id=self.correlation_id
                    )
                    staging_results['load_results'].append({
                        'chunk_index': chunk_result['chunk_index'],
                        'entity_type': 'prime_awards',
                        'result': prime_load_result
                    })
                    staging_results['total_records_loaded'] += prime_load_result.get('rows_loaded', 0)
                    staging_results['total_files_processed'] += 1
                
                # Load subawards data
                if 'subawards' in chunk_result and chunk_result['subawards'].get('archive_path'):
                    subaward_load_result = self.raw_loader.load_csv_to_raw_table(
                        csv_file_path=chunk_result['subawards']['archive_path'],
                        table_name='s1_raw_usaspending_subawards_v2',
                        correlation_id=self.correlation_id
                    )
                    staging_results['load_results'].append({
                        'chunk_index': chunk_result['chunk_index'],
                        'entity_type': 'subawards',
                        'result': subaward_load_result
                    })
                    staging_results['total_records_loaded'] += subaward_load_result.get('rows_loaded', 0)
                    staging_results['total_files_processed'] += 1
            
            staging_results.update({
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info("Data staging phase completed", extra={
                'correlation_id': self.correlation_id,
                'files_processed': staging_results['total_files_processed'],
                'records_loaded': staging_results['total_records_loaded']
            })
            
            return staging_results
            
        except Exception as e:
            staging_results.update({
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Data staging phase failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_transformation_phase(self) -> Dict[str, Any]:
        """Execute data transformation phase (interim + processed layers).
        
        Returns:
            Dict with transformation results
        """
        logger.info("Starting data transformation phase", extra={
            'correlation_id': self.correlation_id
        })
        
        phase_start_time = time.time()
        
        transformation_results = {
            'phase': 'transformation',
            'correlation_id': self.correlation_id,
            'interim_transform_results': None,
            'semantic_description_results': None,
            'processed_transform_results': None
        }
        
        try:
            # Transform to interim layer
            logger.info("Executing interim layer transformation")
            interim_results = self.interim_transformer.transform_all()
            transformation_results['interim_transform_results'] = interim_results
            
            # Generate semantic descriptions
            logger.info("Generating semantic descriptions")
            semantic_results = self.semantic_generator.update_all_semantic_descriptions()
            transformation_results['semantic_description_results'] = semantic_results
            
            # Transform to processed layer
            logger.info("Executing processed layer transformation")
            processed_results = self.processed_transformer.transform_all()
            transformation_results['processed_transform_results'] = processed_results
            
            transformation_results.update({
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info("Data transformation phase completed", extra={
                'correlation_id': self.correlation_id
            })
            
            return transformation_results
            
        except Exception as e:
            transformation_results.update({
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            })
            
            logger.error("Data transformation phase failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_validation_phase(self) -> Dict[str, Any]:
        """Execute data validation and diagnostics phase.
        
        Returns:
            Dict with validation results
        """
        logger.info("Starting data validation phase", extra={
            'correlation_id': self.correlation_id
        })
        
        phase_start_time = time.time()
        
        try:
            # Run comprehensive diagnostics
            diagnostics_results = self.diagnostics.run_comprehensive_diagnostics()
            
            # Validate processed table integrity
            integrity_results = self.processed_transformer.validate_processed_tables()
            
            validation_results = {
                'phase': 'validation',
                'correlation_id': self.correlation_id,
                'diagnostics_results': diagnostics_results,
                'integrity_results': integrity_results,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            }
            
            # Check if validation passed
            diagnostics_passed = diagnostics_results.get('summary', {}).get('tables_with_issues', 1) == 0
            integrity_passed = integrity_results.get('overall_status') == 'passed'
            
            if not (diagnostics_passed and integrity_passed):
                validation_results['overall_status'] = 'warnings'
                validation_results['validation_warnings'] = []
                
                if not diagnostics_passed:
                    validation_results['validation_warnings'].append(
                        f"Diagnostics found issues in {diagnostics_results['summary']['tables_with_issues']} tables"
                    )
                
                if not integrity_passed:
                    validation_results['validation_warnings'].append(
                        f"Integrity validation failed with {len(integrity_results.get('validation_errors', []))} errors"
                    )
            
            logger.info("Data validation phase completed", extra={
                'correlation_id': self.correlation_id,
                'overall_status': validation_results['overall_status']
            })
            
            return validation_results
            
        except Exception as e:
            validation_results = {
                'phase': 'validation',
                'correlation_id': self.correlation_id,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            }
            
            logger.error("Data validation phase failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def execute_complete_pipeline(self, start_date: date, end_date: date, 
                                chunk_size_days: int = 30, 
                                skip_validation: bool = False) -> Dict[str, Any]:
        """Execute complete historical pipeline from acquisition to validation.
        
        Args:
            start_date: Start date for data processing
            end_date: End date for data processing  
            chunk_size_days: Size of processing chunks in days
            skip_validation: Skip validation phase for faster execution
            
        Returns:
            Dict with complete pipeline results
        """
        logger.info("Starting complete historical pipeline execution", extra={
            'correlation_id': self.correlation_id,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'chunk_size_days': chunk_size_days
        })
        
        pipeline_start_time = time.time()
        
        pipeline_results = {
            'pipeline_type': 'historical',
            'correlation_id': self.correlation_id,
            'pipeline_name': self.pipeline_name,
            'processing_window': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': (end_date - start_date).days + 1
            },
            'start_time': pipeline_start_time,
            'phase_results': {},
            'overall_status': 'pending'
        }
        
        try:
            # Phase 1: Prerequisites validation
            logger.info("Phase 1: Validating prerequisites")
            prereq_results = self.validate_prerequisites()
            pipeline_results['phase_results']['prerequisites'] = prereq_results
            
            if not prereq_results['can_proceed']:
                raise RuntimeError("Prerequisites validation failed - cannot proceed")
            
            # Phase 2: Data acquisition
            logger.info("Phase 2: Data acquisition")
            acquisition_results = self.execute_acquisition_phase(start_date, end_date, chunk_size_days)
            pipeline_results['phase_results']['acquisition'] = acquisition_results
            
            # Phase 3: Data staging
            logger.info("Phase 3: Data staging")
            staging_results = self.execute_staging_phase(acquisition_results)
            pipeline_results['phase_results']['staging'] = staging_results
            
            # Phase 4: Data transformation
            logger.info("Phase 4: Data transformation")
            transformation_results = self.execute_transformation_phase()
            pipeline_results['phase_results']['transformation'] = transformation_results
            
            # Phase 5: Validation (optional)
            if not skip_validation:
                logger.info("Phase 5: Data validation")
                validation_results = self.execute_validation_phase()
                pipeline_results['phase_results']['validation'] = validation_results
            
            # Set final watermark
            logger.info("Setting final watermark")
            final_watermark = datetime.combine(end_date, datetime.min.time())
            watermark_info = self.watermark_manager.set_watermark(
                last_modified_to=final_watermark,
                overlap_days=self.config.current_days_lookback
            )
            
            pipeline_results.update({
                'overall_status': 'success',
                'total_execution_time_seconds': round(time.time() - pipeline_start_time, 3),
                'final_watermark': watermark_info.last_modified_to.isoformat(),
                'total_records_processed': transformation_results.get('processed_transform_results', {}).get('total_canonical_records', 0)
            })
            
            logger.info("Complete historical pipeline execution completed successfully", extra={
                'correlation_id': self.correlation_id,
                'total_execution_time': pipeline_results['total_execution_time_seconds'],
                'total_records': pipeline_results['total_records_processed']
            })
            
            return pipeline_results
            
        except Exception as e:
            pipeline_results.update({
                'overall_status': 'failed',
                'total_execution_time_seconds': round(time.time() - pipeline_start_time, 3),
                'error': str(e),
                'error_type': type(e).__name__
            })
            
            logger.error("Complete historical pipeline execution failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise


def main():
    """CLI entry point for historical pipeline execution."""
    parser = argparse.ArgumentParser(description="Execute historical ETL pipeline")
    parser.add_argument('--start-date', type=str, required=True,
                       help='Start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                       help='End date for processing (YYYY-MM-DD)')
    parser.add_argument('--chunk-size-days', type=int, default=30,
                       help='Chunk size in days (default: 30)')
    parser.add_argument('--pipeline-name', type=str, default='prime_awards_historical',
                       help='Pipeline name (default: prime_awards_historical)')
    parser.add_argument('--skip-validation', action='store_true',
                       help='Skip validation phase for faster execution')
    parser.add_argument('--output-file', type=str,
                       help='Save results to JSON file')
    parser.add_argument('--dry-run', action='store_true',
                       help='Validate prerequisites only, do not execute pipeline')
    
    args = parser.parse_args()
    
    try:
        # Parse dates
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        
        if start_date >= end_date:
            print("❌ Start date must be before end date")
            exit(1)
        
        # Initialize orchestrator
        orchestrator = HistoricalPipelineOrchestrator(args.pipeline_name)
        
        print(f"🚀 Historical Pipeline Orchestrator")
        print(f"📊 Correlation ID: {orchestrator.correlation_id}")
        print(f"📅 Processing window: {start_date} to {end_date} ({(end_date - start_date).days + 1} days)")
        print(f"🔧 Pipeline name: {args.pipeline_name}")
        
        if args.dry_run:
            print(f"\n🧪 Running dry-run (prerequisites validation only)")
            prereq_results = orchestrator.validate_prerequisites()
            
            print(f"✅ Prerequisites validation: {prereq_results['overall_status'].upper()}")
            print(f"📋 Checks: {len(prereq_results['validation_checks']) - prereq_results['failed_checks']}/{len(prereq_results['validation_checks'])} passed")
            
            if prereq_results['failed_checks'] > 0:
                print(f"\n❌ Failed checks:")
                for check in prereq_results['validation_checks']:
                    if not check['passed']:
                        print(f"   - {check['check']}: {check.get('error', 'Failed')}")
                exit(1)
            else:
                print(f"✅ All prerequisites passed - pipeline ready to execute")
        else:
            # Execute complete pipeline
            print(f"\n🔄 Executing complete historical pipeline...")
            results = orchestrator.execute_complete_pipeline(
                start_date=start_date,
                end_date=end_date,
                chunk_size_days=args.chunk_size_days,
                skip_validation=args.skip_validation
            )
            
            print(f"\n✅ Pipeline execution completed: {results['overall_status'].upper()}")
            print(f"⏱️  Total execution time: {results['total_execution_time_seconds']}s")
            print(f"📈 Total records processed: {results.get('total_records_processed', 0):,}")
            print(f"💧 Final watermark: {results.get('final_watermark', 'N/A')}")
            
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
        print(f"❌ Pipeline execution failed: {e}")
        logger.exception("Historical pipeline execution failed")
        exit(1)


if __name__ == '__main__':
    main()