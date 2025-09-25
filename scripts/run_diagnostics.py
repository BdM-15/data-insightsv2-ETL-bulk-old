"""
Diagnostics Pipeline Orchestrator

Runs comprehensive data quality diagnostics and generates reports
for monitoring ETL pipeline health and data integrity.

Constitution v1.9.0 | Task: T040-T050
Dependencies: DuplicateDiagnostics, SemanticDescriptionGenerator
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
from etl.checks.duplicate_diagnostics import DuplicateDiagnostics
from etl.sql.semantic_description import SemanticDescriptionGenerator
from etl.staging.watermark import WatermarkManager

logger = get_logger(__name__)

class DiagnosticsPipelineOrchestrator:
    """Orchestrate comprehensive data quality diagnostics."""
    
    def __init__(self, pipeline_name: str = "data_quality_diagnostics"):
        """Initialize diagnostics orchestrator.
        
        Args:
            pipeline_name: Name for this diagnostics run
        """
        self.config = get_config()
        self.pipeline_name = pipeline_name
        self.correlation_id = str(uuid4())
        
        # Initialize components
        self.diagnostics = DuplicateDiagnostics()
        self.semantic_generator = SemanticDescriptionGenerator()
        self.watermark_manager = WatermarkManager("prime_awards_incremental")
        
        logger.info("DiagnosticsPipelineOrchestrator initialized", extra={
            'correlation_id': self.correlation_id,
            'pipeline_name': self.pipeline_name
        })
    
    def run_duplicate_diagnostics(self, scope: str = 'comprehensive') -> Dict[str, Any]:
        """Run duplicate detection diagnostics.
        
        Args:
            scope: Diagnostic scope ('comprehensive', 'recent', 'summary')
            
        Returns:
            Dict with diagnostics results
        """
        logger.info(f"Running duplicate diagnostics - scope: {scope}", extra={
            'correlation_id': self.correlation_id,
            'scope': scope
        })
        
        phase_start_time = time.time()
        
        try:
            if scope == 'comprehensive':
                diagnostics_results = self.diagnostics.run_comprehensive_diagnostics()
            elif scope == 'recent':
                # Default to last 7 days for recent diagnostics
                days_back = self.config.current_days_lookback * 2
                diagnostics_results = self.diagnostics.run_recent_data_diagnostics(days_back)
            elif scope == 'summary':
                diagnostics_results = self.diagnostics.generate_diagnostics_summary()
            else:
                raise ValueError(f"Invalid diagnostics scope: {scope}")
            
            diagnostics_results.update({
                'phase': 'duplicate_diagnostics',
                'scope': scope,
                'correlation_id': self.correlation_id,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'success'
            })
            
            logger.info(f"Duplicate diagnostics completed - scope: {scope}", extra={
                'correlation_id': self.correlation_id,
                'scope': scope,
                'execution_time': diagnostics_results['phase_execution_time_seconds']
            })
            
            return diagnostics_results
            
        except Exception as e:
            diagnostics_results = {
                'phase': 'duplicate_diagnostics',
                'scope': scope,
                'correlation_id': self.correlation_id,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            }
            
            logger.error(f"Duplicate diagnostics failed - scope: {scope}", extra={
                'correlation_id': self.correlation_id,
                'scope': scope,
                'error': str(e)
            })
            
            raise
    
    def run_semantic_validation(self, scope: str = 'comprehensive') -> Dict[str, Any]:
        """Run semantic description validation.
        
        Args:
            scope: Validation scope ('comprehensive', 'recent', 'validation_only')
            
        Returns:
            Dict with validation results
        """
        logger.info(f"Running semantic validation - scope: {scope}", extra={
            'correlation_id': self.correlation_id,
            'scope': scope
        })
        
        phase_start_time = time.time()
        
        try:
            if scope == 'comprehensive':
                # Run full semantic description update
                validation_results = self.semantic_generator.update_all_semantic_descriptions()
            elif scope == 'recent':
                # Update only recent records
                days_back = self.config.current_days_lookback * 2
                validation_results = self.semantic_generator.update_recent_semantic_descriptions(days_back)
            elif scope == 'validation_only':
                # Just validate the function without updating
                validation_results = self.semantic_generator.verify_clean_description_function()
            else:
                raise ValueError(f"Invalid semantic validation scope: {scope}")
            
            validation_results.update({
                'phase': 'semantic_validation',
                'scope': scope,
                'correlation_id': self.correlation_id,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3)
            })
            
            logger.info(f"Semantic validation completed - scope: {scope}", extra={
                'correlation_id': self.correlation_id,
                'scope': scope,
                'execution_time': validation_results['phase_execution_time_seconds']
            })
            
            return validation_results
            
        except Exception as e:
            validation_results = {
                'phase': 'semantic_validation',
                'scope': scope,
                'correlation_id': self.correlation_id,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'failed',
                'error': str(e)
            }
            
            logger.error(f"Semantic validation failed - scope: {scope}", extra={
                'correlation_id': self.correlation_id,
                'scope': scope,
                'error': str(e)
            })
            
            raise
    
    def run_system_health_check(self) -> Dict[str, Any]:
        """Run system health and capacity checks.
        
        Returns:
            Dict with system health results
        """
        logger.info("Running system health check", extra={
            'correlation_id': self.correlation_id
        })
        
        phase_start_time = time.time()
        
        try:
            from etl.utils.disk import check_available_space_gb
            
            health_results = {
                'phase': 'system_health',
                'correlation_id': self.correlation_id,
                'checks': []
            }
            
            # Disk space check
            available_space = check_available_space_gb()
            disk_check = {
                'check_type': 'disk_space',
                'available_gb': available_space,
                'minimum_required_gb': self.config.min_free_space_gb,
                'status': 'healthy' if available_space >= self.config.min_free_space_gb else 'warning',
                'utilization_percent': max(0, 100 - (available_space / self.config.min_free_space_gb * 100))
            }
            
            if disk_check['status'] == 'warning':
                disk_check['message'] = f"Low disk space: {available_space:.1f}GB available, {self.config.min_free_space_gb}GB required"
            
            health_results['checks'].append(disk_check)
            
            # Database connectivity check
            db_check = {
                'check_type': 'database_connectivity',
                'status': 'unknown'
            }
            
            try:
                # Test database connection via watermark manager
                watermark_info = self.watermark_manager.get_current_watermark()
                db_check['status'] = 'healthy'
                db_check['current_watermark'] = watermark_info.last_modified_to.isoformat() if watermark_info.last_modified_to else None
                db_check['message'] = 'Database connection successful'
            except Exception as e:
                db_check['status'] = 'error'
                db_check['error'] = str(e)
                db_check['message'] = f'Database connection failed: {str(e)}'
            
            health_results['checks'].append(db_check)
            
            # Table health check
            table_check = {
                'check_type': 'table_health',
                'status': 'unknown'
            }
            
            try:
                # Quick table row counts
                table_stats = self.diagnostics.get_table_statistics()
                table_check['status'] = 'healthy'
                table_check['table_statistics'] = table_stats
                table_check['message'] = f'Found {len(table_stats)} tables with data'
            except Exception as e:
                table_check['status'] = 'error'
                table_check['error'] = str(e)
                table_check['message'] = f'Table health check failed: {str(e)}'
            
            health_results['checks'].append(table_check)
            
            # Determine overall health
            error_checks = [c for c in health_results['checks'] if c['status'] == 'error']
            warning_checks = [c for c in health_results['checks'] if c['status'] == 'warning']
            
            if error_checks:
                overall_status = 'unhealthy'
                health_message = f"{len(error_checks)} system errors detected"
            elif warning_checks:
                overall_status = 'degraded'
                health_message = f"{len(warning_checks)} system warnings detected"
            else:
                overall_status = 'healthy'
                health_message = 'All systems healthy'
            
            health_results.update({
                'overall_status': overall_status,
                'health_message': health_message,
                'errors_count': len(error_checks),
                'warnings_count': len(warning_checks),
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3)
            })
            
            logger.info("System health check completed", extra={
                'correlation_id': self.correlation_id,
                'overall_status': overall_status,
                'errors': len(error_checks),
                'warnings': len(warning_checks)
            })
            
            return health_results
            
        except Exception as e:
            health_results = {
                'phase': 'system_health',
                'correlation_id': self.correlation_id,
                'phase_execution_time_seconds': round(time.time() - phase_start_time, 3),
                'overall_status': 'error',
                'error': str(e)
            }
            
            logger.error("System health check failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive diagnostics report.
        
        Returns:
            Dict with complete diagnostics report
        """
        logger.info("Generating comprehensive diagnostics report", extra={
            'correlation_id': self.correlation_id
        })
        
        report_start_time = time.time()
        
        report_results = {
            'report_type': 'comprehensive_diagnostics',
            'correlation_id': self.correlation_id,
            'pipeline_name': self.pipeline_name,
            'report_timestamp': datetime.now().isoformat(),
            'sections': {},
            'overall_status': 'pending'
        }
        
        try:
            # Section 1: System health
            logger.info("Generating system health section")
            report_results['sections']['system_health'] = self.run_system_health_check()
            
            # Section 2: Duplicate diagnostics summary
            logger.info("Generating duplicate diagnostics section")
            report_results['sections']['duplicate_diagnostics'] = self.run_duplicate_diagnostics('summary')
            
            # Section 3: Semantic validation
            logger.info("Generating semantic validation section")
            report_results['sections']['semantic_validation'] = self.run_semantic_validation('validation_only')
            
            # Section 4: Recent data quality (last 7 days)
            logger.info("Generating recent data quality section")
            report_results['sections']['recent_data_quality'] = self.run_duplicate_diagnostics('recent')
            
            # Generate executive summary
            system_status = report_results['sections']['system_health']['overall_status']
            duplicate_issues = report_results['sections']['duplicate_diagnostics']['summary'].get('tables_with_issues', 0)
            semantic_status = report_results['sections']['semantic_validation']['overall_status']
            recent_issues = report_results['sections']['recent_data_quality']['summary'].get('tables_with_issues', 0)
            
            executive_summary = {
                'system_health': system_status,
                'duplicate_issues_count': duplicate_issues,
                'semantic_status': semantic_status,
                'recent_issues_count': recent_issues,
                'recommendations': []
            }
            
            # Generate recommendations
            if system_status in ['unhealthy', 'degraded']:
                executive_summary['recommendations'].append(
                    "Review system health issues - check disk space and database connectivity"
                )
            
            if duplicate_issues > 0:
                executive_summary['recommendations'].append(
                    f"Investigate duplicate data in {duplicate_issues} tables"
                )
            
            if semantic_status != 'passed':
                executive_summary['recommendations'].append(
                    "Semantic description function needs attention"
                )
            
            if recent_issues > 0:
                executive_summary['recommendations'].append(
                    f"Recent data quality issues detected in {recent_issues} tables - investigate incremental processing"
                )
            
            if not executive_summary['recommendations']:
                executive_summary['recommendations'].append("No immediate action required - all systems healthy")
            
            # Determine overall report status
            if system_status == 'unhealthy' or semantic_status == 'failed':
                overall_status = 'critical'
            elif system_status == 'degraded' or duplicate_issues > 0 or recent_issues > 0:
                overall_status = 'warnings'
            else:
                overall_status = 'healthy'
            
            report_results.update({
                'executive_summary': executive_summary,
                'overall_status': overall_status,
                'total_execution_time_seconds': round(time.time() - report_start_time, 3)
            })
            
            logger.info("Comprehensive diagnostics report completed", extra={
                'correlation_id': self.correlation_id,
                'overall_status': overall_status,
                'execution_time': report_results['total_execution_time_seconds']
            })
            
            return report_results
            
        except Exception as e:
            report_results.update({
                'overall_status': 'error',
                'total_execution_time_seconds': round(time.time() - report_start_time, 3),
                'error': str(e),
                'error_type': type(e).__name__
            })
            
            logger.error("Comprehensive diagnostics report failed", extra={
                'correlation_id': self.correlation_id,
                'error': str(e)
            })
            
            raise


def main():
    """CLI entry point for diagnostics pipeline execution."""
    parser = argparse.ArgumentParser(description="Execute data quality diagnostics")
    parser.add_argument('--mode', type=str, 
                       choices=['comprehensive', 'duplicates', 'semantic', 'health', 'recent'],
                       default='comprehensive',
                       help='Diagnostics mode (default: comprehensive)')
    parser.add_argument('--scope', type=str,
                       choices=['comprehensive', 'recent', 'summary', 'validation_only'],
                       help='Diagnostics scope (auto-selected based on mode if not specified)')
    parser.add_argument('--days-back', type=int, default=7,
                       help='Days back for recent data analysis (default: 7)')
    parser.add_argument('--output-file', type=str,
                       help='Save results to JSON file')
    parser.add_argument('--output-dir', type=str, default='diagnostics_reports',
                       help='Directory for output files (default: diagnostics_reports)')
    
    args = parser.parse_args()
    
    try:
        # Initialize orchestrator
        orchestrator = DiagnosticsPipelineOrchestrator()
        
        print(f"🔍 Data Quality Diagnostics Pipeline")
        print(f"📊 Correlation ID: {orchestrator.correlation_id}")
        print(f"🔧 Mode: {args.mode}")
        
        # Execute based on mode
        if args.mode == 'comprehensive':
            print(f"\n📋 Running comprehensive diagnostics report...")
            results = orchestrator.generate_comprehensive_report()
            
            print(f"\n✅ Comprehensive report completed: {results['overall_status'].upper()}")
            print(f"⏱️  Total execution time: {results['total_execution_time_seconds']}s")
            
            # Print executive summary
            summary = results.get('executive_summary', {})
            print(f"\n📊 Executive Summary:")
            print(f"   System Health: {summary.get('system_health', 'unknown').upper()}")
            print(f"   Duplicate Issues: {summary.get('duplicate_issues_count', 0)} tables")
            print(f"   Semantic Status: {summary.get('semantic_status', 'unknown').upper()}")
            print(f"   Recent Issues: {summary.get('recent_issues_count', 0)} tables")
            
            if summary.get('recommendations'):
                print(f"\n💡 Recommendations:")
                for rec in summary['recommendations']:
                    print(f"   - {rec}")
        
        elif args.mode == 'duplicates':
            scope = args.scope or 'comprehensive'
            print(f"\n🔍 Running duplicate diagnostics (scope: {scope})...")
            results = orchestrator.run_duplicate_diagnostics(scope)
            
            print(f"✅ Duplicate diagnostics completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['phase_execution_time_seconds']}s")
            
            if 'summary' in results:
                summary = results['summary']
                print(f"\n📊 Summary:")
                print(f"   Tables analyzed: {summary.get('tables_analyzed', 0)}")
                print(f"   Tables with issues: {summary.get('tables_with_issues', 0)}")
                print(f"   Total duplicates: {summary.get('total_duplicate_groups', 0)}")
        
        elif args.mode == 'semantic':
            scope = args.scope or 'validation_only'
            print(f"\n📝 Running semantic validation (scope: {scope})...")
            results = orchestrator.run_semantic_validation(scope)
            
            print(f"✅ Semantic validation completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['phase_execution_time_seconds']}s")
        
        elif args.mode == 'health':
            print(f"\n💊 Running system health check...")
            results = orchestrator.run_system_health_check()
            
            print(f"✅ System health check completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['phase_execution_time_seconds']}s")
            print(f"📋 Health message: {results.get('health_message', 'N/A')}")
            
            if results.get('errors_count', 0) > 0 or results.get('warnings_count', 0) > 0:
                print(f"\n⚠️  Issues found:")
                for check in results.get('checks', []):
                    if check['status'] in ['error', 'warning']:
                        print(f"   - {check['check_type']}: {check.get('message', check['status'])}")
        
        elif args.mode == 'recent':
            print(f"\n📅 Running recent data diagnostics ({args.days_back} days)...")
            results = orchestrator.run_duplicate_diagnostics('recent')
            
            print(f"✅ Recent data diagnostics completed: {results['overall_status'].upper()}")
            print(f"⏱️  Execution time: {results['phase_execution_time_seconds']}s")
        
        # Save results if requested or auto-generate filename
        output_path = None
        
        if args.output_file:
            output_path = Path(args.output_file)
        else:
            # Auto-generate filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"diagnostics_{args.mode}_{timestamp}.json"
            output_path = Path(args.output_dir) / filename
        
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"💾 Results saved to: {output_path}")
        
        # Exit with appropriate code
        if results.get('overall_status') in ['failed', 'error', 'critical']:
            exit(1)
        elif results.get('overall_status') in ['warnings', 'degraded']:
            exit(2)  # Warning exit code
    
    except Exception as e:
        print(f"❌ Diagnostics execution failed: {e}")
        logger.exception("Diagnostics pipeline execution failed")
        exit(1)


if __name__ == '__main__':
    main()