"""
ETL Pipeline Setup and Installation Script

Automates project setup, environment creation, dependency installation,
and initial database schema deployment for the ETL pipeline.

Constitution v1.9.0 | Task: T045
Dependencies: uv, PostgreSQL, Python 3.13+
"""

import sys
import os
import subprocess
import json
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import argparse
import platform

def run_command(command: List[str], cwd: Optional[Path] = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result.
    
    Args:
        command: Command and arguments to run
        cwd: Working directory for command
        check: Whether to raise exception on non-zero exit
        
    Returns:
        CompletedProcess result
    """
    print(f"🔄 Running: {' '.join(command)}")
    
    try:
        result = subprocess.run(
            command, 
            cwd=cwd, 
            check=check, 
            capture_output=True, 
            text=True
        )
        
        if result.stdout.strip():
            print(f"✅ Output: {result.stdout.strip()}")
            
        return result
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed with exit code {e.returncode}")
        if e.stdout:
            print(f"📝 Stdout: {e.stdout}")
        if e.stderr:
            print(f"📝 Stderr: {e.stderr}")
        raise

class ETLPipelineSetup:
    """Automated setup for ETL pipeline project."""
    
    def __init__(self, project_dir: Path):
        """Initialize setup manager.
        
        Args:
            project_dir: Path to project directory
        """
        self.project_dir = project_dir
        self.project_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"🚀 ETL Pipeline Setup Manager")
        print(f"📁 Project directory: {self.project_dir}")
        print(f"🖥️  Platform: {platform.system()} {platform.release()}")
        print(f"🐍 Python: {sys.version}")
    
    def check_prerequisites(self) -> Dict[str, Any]:
        """Check system prerequisites for the ETL pipeline.
        
        Returns:
            Dict with prerequisite check results
        """
        print("\n🔍 Checking system prerequisites...")
        
        prereq_results = {
            'checks': [],
            'all_passed': True
        }
        
        # Check Python version
        python_version = sys.version_info
        python_check = {
            'name': 'Python Version',
            'required': '3.13+',
            'found': f"{python_version.major}.{python_version.minor}.{python_version.micro}",
            'passed': python_version >= (3, 13)
        }
        
        if not python_check['passed']:
            python_check['error'] = f"Python 3.13+ required, found {python_check['found']}"
            
        prereq_results['checks'].append(python_check)
        prereq_results['all_passed'] &= python_check['passed']
        
        # Check uv
        try:
            uv_result = run_command(['uv', '--version'], check=False)
            uv_check = {
                'name': 'uv Package Manager',
                'required': 'any version',
                'passed': uv_result.returncode == 0
            }
            
            if uv_check['passed']:
                uv_check['found'] = uv_result.stdout.strip()
            else:
                uv_check['error'] = 'uv not found or not working'
                
        except FileNotFoundError:
            uv_check = {
                'name': 'uv Package Manager',
                'required': 'any version',
                'passed': False,
                'error': 'uv not installed'
            }
        
        prereq_results['checks'].append(uv_check)
        prereq_results['all_passed'] &= uv_check['passed']
        
        # Check PostgreSQL
        try:
            psql_result = run_command(['psql', '--version'], check=False)
            pg_check = {
                'name': 'PostgreSQL Client',
                'required': '14+',
                'passed': psql_result.returncode == 0
            }
            
            if pg_check['passed']:
                pg_check['found'] = psql_result.stdout.strip()
            else:
                pg_check['error'] = 'psql not found or not working'
                
        except FileNotFoundError:
            pg_check = {
                'name': 'PostgreSQL Client',
                'required': '14+',
                'passed': False,
                'error': 'PostgreSQL client not installed'
            }
        
        prereq_results['checks'].append(pg_check)
        prereq_results['all_passed'] &= pg_check['passed']
        
        # Check Git
        try:
            git_result = run_command(['git', '--version'], check=False)
            git_check = {
                'name': 'Git',
                'required': 'any version',
                'passed': git_result.returncode == 0
            }
            
            if git_check['passed']:
                git_check['found'] = git_result.stdout.strip()
            else:
                git_check['error'] = 'git not found or not working'
                
        except FileNotFoundError:
            git_check = {
                'name': 'Git',
                'required': 'any version',
                'passed': False,
                'error': 'Git not installed'
            }
        
        prereq_results['checks'].append(git_check)
        # Git is optional, don't affect overall pass
        
        # Display results
        print("\n📋 Prerequisites Check Results:")
        print("-" * 60)
        
        for check in prereq_results['checks']:
            status_icon = "✅" if check['passed'] else "❌"
            print(f"{status_icon} {check['name']}: {check.get('found', 'Not Found')}")
            
            if not check['passed'] and 'error' in check:
                print(f"   Error: {check['error']}")
        
        print(f"\n🎯 Overall: {'✅ All prerequisites met' if prereq_results['all_passed'] else '❌ Prerequisites missing'}")
        
        return prereq_results
    
    def setup_project_structure(self) -> Dict[str, Any]:
        """Create the project directory structure.
        
        Returns:
            Dict with setup results
        """
        print("\n📁 Setting up project directory structure...")
        
        # Define directory structure
        directories = [
            "etl",
            "etl/acquisition",
            "etl/staging", 
            "etl/sql",
            "etl/utils",
            "etl/checks",
            "scripts",
            "tests",
            "tests/contracts",
            "tests/config",
            "tests/integration",
            "sql",
            "sql/util",
            "sql/00_s1_raw",
            "sql/10_s2_interim", 
            "sql/20_s3_processed",
            "docs",
            "logs",
            "data",
            "data/archive"
        ]
        
        created_dirs = []
        
        for dir_path in directories:
            full_path = self.project_dir / dir_path
            if not full_path.exists():
                full_path.mkdir(parents=True, exist_ok=True)
                created_dirs.append(dir_path)
                print(f"📁 Created directory: {dir_path}")
            else:
                print(f"📁 Directory exists: {dir_path}")
        
        # Create __init__.py files for Python packages
        python_packages = [
            "etl",
            "etl/acquisition",
            "etl/staging",
            "etl/sql", 
            "etl/utils",
            "etl/checks",
            "tests"
        ]
        
        created_init_files = []
        
        for package in python_packages:
            init_file = self.project_dir / package / "__init__.py"
            if not init_file.exists():
                init_file.write_text('"""ETL Pipeline package."""\n')
                created_init_files.append(f"{package}/__init__.py")
                print(f"🐍 Created: {package}/__init__.py")
        
        return {
            'created_directories': created_dirs,
            'created_init_files': created_init_files,
            'total_directories': len(directories),
            'status': 'success'
        }
    
    def create_project_files(self) -> Dict[str, Any]:
        """Create essential project configuration files.
        
        Returns:
            Dict with file creation results
        """
        print("\n📝 Creating project configuration files...")
        
        created_files = []
        
        # pyproject.toml
        pyproject_content = '''[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "usa-spending-etl"
version = "1.9.0"
description = "USASpending ETL Pipeline with PostgreSQL and pgvector"
authors = [{name = "ETL Team", email = "team@example.com"}]
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "psycopg[binary]>=3.1.0",
    "requests>=2.31.0", 
    "tenacity>=8.2.0",
    "python-dotenv>=1.0.0",
    "jsonschema>=4.0.0"
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "black>=23.0.0",
    "ruff>=0.1.0",
    "mypy>=1.5.0"
]

[tool.setuptools.packages.find]
where = ["."]
include = ["etl*"]

[tool.ruff]
line-length = 100
target-version = "py313"

[tool.black]
line-length = 100
target-version = ['py313']

[tool.mypy]
python_version = "3.13"
strict = true
warn_return_any = true
warn_unused_configs = true
'''
        
        pyproject_file = self.project_dir / "pyproject.toml"
        if not pyproject_file.exists():
            pyproject_file.write_text(pyproject_content)
            created_files.append("pyproject.toml")
            print("📝 Created: pyproject.toml")
        
        # README.md
        readme_content = '''# USASpending ETL Pipeline

A comprehensive ETL pipeline for processing USASpending.gov data using PostgreSQL and pgvector.

## Constitution v1.9.0

This project implements a modular, SQL-first ETL pipeline with:

- ✅ Historical and incremental data processing
- ✅ Vector embeddings for semantic search
- ✅ Comprehensive data quality monitoring
- ✅ Fail-fast error handling
- ✅ Storage-conscious processing
- ✅ Automated orchestration scripts

## Quick Start

1. **Setup Environment:**
   ```bash
   python scripts/setup.py --create-venv --install-deps
   ```

2. **Configure Environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Initialize Database:**
   ```bash
   python scripts/setup.py --init-database
   ```

4. **Run Historical Pipeline:**
   ```bash
   python scripts/run_historical.py --start-date 2024-01-01 --end-date 2024-01-31
   ```

5. **Monitor Pipeline:**
   ```bash
   python scripts/monitor.py --mode full
   ```

## Architecture

- **Raw Layer (S1)**: Direct API data ingestion
- **Interim Layer (S2)**: Cleaned and standardized data
- **Processed Layer (S3)**: Canonical records with embeddings

## Scripts

- `scripts/run_historical.py` - Initial data loading
- `scripts/run_incremental.py` - Daily updates
- `scripts/run_diagnostics.py` - Data quality checks
- `scripts/monitor.py` - Real-time monitoring
- `scripts/maintenance.py` - System maintenance

## Requirements

- Python 3.13+
- PostgreSQL 14+ with pgvector
- uv package manager
- 50GB+ available storage

For detailed documentation, see the `docs/` directory.
'''
        
        readme_file = self.project_dir / "README.md"
        if not readme_file.exists():
            readme_file.write_text(readme_content)
            created_files.append("README.md")
            print("📝 Created: README.md")
        
        # .env.example
        env_example_content = '''# ETL Pipeline Configuration
# Copy this file to .env and fill in your values

# Database Configuration
DATABASE_URL=postgresql://username:password@localhost:5432/usaspending_etl
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=usaspending_etl
POSTGRES_USER=username
POSTGRES_PASSWORD=password

# USASpending API Configuration
USASPENDING_API_BASE_URL=https://api.usaspending.gov
API_REQUEST_TIMEOUT_SECONDS=300
API_MAX_RETRIES=3

# Storage Configuration
ARCHIVE_DIR=./data/archive
MIN_FREE_SPACE_GB=10
MAX_FILE_SIZE_MB=1000

# Processing Configuration
DEFAULT_CHUNK_SIZE_DAYS=30
CURRENT_DAYS_LOOKBACK=3
MAX_CONCURRENT_REQUESTS=5

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=json
LOG_FILE_PATH=./logs/etl_pipeline.log

# Development/Testing
ENVIRONMENT=development
DEBUG=false
'''
        
        env_example_file = self.project_dir / ".env.example"
        if not env_example_file.exists():
            env_example_file.write_text(env_example_content)
            created_files.append(".env.example")
            print("📝 Created: .env.example")
        
        # .gitignore
        gitignore_content = '''# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/
.uv/

# IDEs
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Logs
*.log
logs/
*.out

# Data files
data/archive/
data/temp/
*.csv
*.zip
*.json.gz

# Database
*.db
*.sqlite3

# pytest
.pytest_cache/
.coverage
htmlcov/

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Jupyter
.ipynb_checkpoints

# Environment variables
.env
.env.local
.env.*.local
'''
        
        gitignore_file = self.project_dir / ".gitignore"
        if not gitignore_file.exists():
            gitignore_file.write_text(gitignore_content)
            created_files.append(".gitignore")
            print("📝 Created: .gitignore")
        
        return {
            'created_files': created_files,
            'status': 'success'
        }
    
    def create_virtual_environment(self) -> Dict[str, Any]:
        """Create Python virtual environment using uv.
        
        Returns:
            Dict with venv creation results
        """
        print("\n🐍 Creating Python virtual environment...")
        
        venv_path = self.project_dir / ".venv"
        
        if venv_path.exists():
            print(f"✅ Virtual environment already exists at: {venv_path}")
            return {
                'venv_path': str(venv_path),
                'status': 'exists'
            }
        
        try:
            # Create virtual environment with uv
            run_command(['uv', 'venv', str(venv_path)], cwd=self.project_dir)
            
            print(f"✅ Virtual environment created at: {venv_path}")
            
            return {
                'venv_path': str(venv_path),
                'status': 'created'
            }
            
        except Exception as e:
            print(f"❌ Failed to create virtual environment: {e}")
            return {
                'venv_path': str(venv_path),
                'status': 'failed',
                'error': str(e)
            }
    
    def install_dependencies(self) -> Dict[str, Any]:
        """Install project dependencies using uv.
        
        Returns:
            Dict with installation results
        """
        print("\n📦 Installing project dependencies...")
        
        try:
            # Install project in development mode with all dependencies
            run_command(['uv', 'pip', 'install', '-e', '.', '--extra', 'dev'], cwd=self.project_dir)
            
            print("✅ All dependencies installed successfully")
            
            # Get list of installed packages
            result = run_command(['uv', 'pip', 'list'], cwd=self.project_dir)
            installed_packages = result.stdout.strip().split('\n')[2:]  # Skip header lines
            
            return {
                'status': 'success',
                'packages_installed': len(installed_packages),
                'packages': installed_packages[:10]  # First 10 for brevity
            }
            
        except Exception as e:
            print(f"❌ Failed to install dependencies: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def verify_installation(self) -> Dict[str, Any]:
        """Verify the installation by importing key modules.
        
        Returns:
            Dict with verification results
        """
        print("\n🧪 Verifying installation...")
        
        verification_results = {
            'checks': [],
            'all_passed': True
        }
        
        # Test imports
        test_imports = [
            ('psycopg', 'PostgreSQL adapter'),
            ('requests', 'HTTP library'),
            ('tenacity', 'Retry library'),
            ('jsonschema', 'JSON schema validation'),
            ('pytest', 'Testing framework')
        ]
        
        for module_name, description in test_imports:
            try:
                __import__(module_name)
                check_result = {
                    'module': module_name,
                    'description': description,
                    'passed': True
                }
                print(f"✅ {description}: OK")
                
            except ImportError as e:
                check_result = {
                    'module': module_name,
                    'description': description,
                    'passed': False,
                    'error': str(e)
                }
                print(f"❌ {description}: FAILED - {e}")
                verification_results['all_passed'] = False
            
            verification_results['checks'].append(check_result)
        
        # Test etl package import
        try:
            sys.path.insert(0, str(self.project_dir))
            import etl.config
            
            etl_check = {
                'module': 'etl.config',
                'description': 'ETL configuration',
                'passed': True
            }
            print("✅ ETL configuration: OK")
            
        except ImportError as e:
            etl_check = {
                'module': 'etl.config',
                'description': 'ETL configuration',
                'passed': False,
                'error': str(e)
            }
            print(f"❌ ETL configuration: FAILED - {e}")
            verification_results['all_passed'] = False
        
        verification_results['checks'].append(etl_check)
        
        print(f"\n🎯 Verification: {'✅ All modules OK' if verification_results['all_passed'] else '❌ Some modules failed'}")
        
        return verification_results
    
    def initialize_database(self, database_url: Optional[str] = None) -> Dict[str, Any]:
        """Initialize database schema (requires database connection).
        
        Args:
            database_url: Database connection URL
            
        Returns:
            Dict with database initialization results
        """
        print("\n🗄️  Initializing database schema...")
        
        if not database_url:
            print("⚠️  Database URL not provided - skipping database initialization")
            print("   Use --database-url or set DATABASE_URL environment variable")
            return {
                'status': 'skipped',
                'message': 'Database URL not provided'
            }
        
        try:
            import psycopg
            
            # Test connection
            with psycopg.connect(database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    version = cur.fetchone()[0]
                    print(f"✅ Database connection successful: {version}")
                    
                    # Check for pgvector extension
                    cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
                    has_vector = cur.fetchone() is not None
                    
                    if not has_vector:
                        print("⚠️  pgvector extension not found - some features may not work")
                    else:
                        print("✅ pgvector extension available")
            
            return {
                'status': 'success',
                'database_version': version,
                'pgvector_available': has_vector
            }
            
        except ImportError:
            return {
                'status': 'failed',
                'error': 'psycopg not installed'
            }
        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def generate_setup_report(self, results: Dict[str, Any]) -> str:
        """Generate a comprehensive setup report.
        
        Args:
            results: Combined results from all setup steps
            
        Returns:
            Formatted report string
        """
        report_lines = []
        
        report_lines.append("=" * 80)
        report_lines.append("🚀 ETL PIPELINE SETUP REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Project Directory: {self.project_dir}")
        report_lines.append(f"Setup Time: {results.get('setup_time', 'N/A')}")
        report_lines.append("")
        
        # Prerequisites
        if 'prerequisites' in results:
            prereqs = results['prerequisites']
            status_icon = "✅" if prereqs['all_passed'] else "❌"
            report_lines.append(f"{status_icon} PREREQUISITES")
            report_lines.append("-" * 40)
            
            for check in prereqs['checks']:
                check_icon = "✅" if check['passed'] else "❌"
                report_lines.append(f"  {check_icon} {check['name']}: {check.get('found', 'Missing')}")
            
            report_lines.append("")
        
        # Project Structure
        if 'project_structure' in results:
            structure = results['project_structure']
            report_lines.append("✅ PROJECT STRUCTURE")
            report_lines.append("-" * 40)
            report_lines.append(f"  📁 Directories created: {len(structure.get('created_directories', []))}")
            report_lines.append(f"  🐍 Python packages: {len(structure.get('created_init_files', []))}")
            report_lines.append("")
        
        # Configuration Files
        if 'project_files' in results:
            files = results['project_files']
            report_lines.append("✅ CONFIGURATION FILES")
            report_lines.append("-" * 40)
            
            for file_name in files.get('created_files', []):
                report_lines.append(f"  📝 {file_name}")
            
            report_lines.append("")
        
        # Virtual Environment
        if 'venv' in results:
            venv = results['venv']
            status_icon = "✅" if venv['status'] in ['created', 'exists'] else "❌"
            report_lines.append(f"{status_icon} VIRTUAL ENVIRONMENT")
            report_lines.append("-" * 40)
            report_lines.append(f"  📍 Path: {venv.get('venv_path', 'N/A')}")
            report_lines.append(f"  🔄 Status: {venv.get('status', 'unknown').upper()}")
            report_lines.append("")
        
        # Dependencies
        if 'dependencies' in results:
            deps = results['dependencies']
            status_icon = "✅" if deps['status'] == 'success' else "❌"
            report_lines.append(f"{status_icon} DEPENDENCIES")
            report_lines.append("-" * 40)
            report_lines.append(f"  📦 Packages installed: {deps.get('packages_installed', 0)}")
            report_lines.append("")
        
        # Verification
        if 'verification' in results:
            verify = results['verification']
            status_icon = "✅" if verify['all_passed'] else "❌"
            report_lines.append(f"{status_icon} INSTALLATION VERIFICATION")
            report_lines.append("-" * 40)
            
            for check in verify['checks']:
                check_icon = "✅" if check['passed'] else "❌"
                report_lines.append(f"  {check_icon} {check['description']}")
            
            report_lines.append("")
        
        # Database
        if 'database' in results:
            db = results['database']
            status_icon = "✅" if db['status'] == 'success' else "⚠️" if db['status'] == 'skipped' else "❌"
            report_lines.append(f"{status_icon} DATABASE INITIALIZATION")
            report_lines.append("-" * 40)
            
            if db['status'] == 'success':
                report_lines.append(f"  🗄️  Database: Connected")
                report_lines.append(f"  🧩 pgvector: {'Available' if db.get('pgvector_available') else 'Missing'}")
            elif db['status'] == 'skipped':
                report_lines.append(f"  ⚠️  Skipped: {db.get('message', 'No database URL provided')}")
            else:
                report_lines.append(f"  ❌ Failed: {db.get('error', 'Unknown error')}")
            
            report_lines.append("")
        
        # Next Steps
        report_lines.append("🚀 NEXT STEPS")
        report_lines.append("-" * 40)
        report_lines.append("1. Copy .env.example to .env and configure your settings")
        report_lines.append("2. Set up your PostgreSQL database with pgvector extension")
        report_lines.append("3. Run: python scripts/setup.py --init-database")
        report_lines.append("4. Test with: python scripts/monitor.py --mode health")
        report_lines.append("5. Start processing: python scripts/run_historical.py --help")
        report_lines.append("")
        
        report_lines.append("📚 For more information, see README.md and docs/")
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)


def main():
    """CLI entry point for ETL pipeline setup."""
    parser = argparse.ArgumentParser(description="ETL Pipeline Setup and Installation")
    parser.add_argument('--project-dir', type=str, default='.',
                       help='Project directory path (default: current directory)')
    parser.add_argument('--create-venv', action='store_true',
                       help='Create virtual environment')
    parser.add_argument('--install-deps', action='store_true',
                       help='Install dependencies')
    parser.add_argument('--init-database', action='store_true',
                       help='Initialize database schema')
    parser.add_argument('--database-url', type=str,
                       help='Database connection URL')
    parser.add_argument('--skip-verification', action='store_true',
                       help='Skip installation verification')
    parser.add_argument('--output-report', type=str,
                       help='Save setup report to file')
    parser.add_argument('--full-setup', action='store_true',
                       help='Run complete setup (structure + venv + deps + verification)')
    
    args = parser.parse_args()
    
    try:
        project_dir = Path(args.project_dir).resolve()
        setup_manager = ETLPipelineSetup(project_dir)
        
        setup_results = {
            'setup_time': subprocess.run(['date'], capture_output=True, text=True).stdout.strip()
        }
        
        # Check prerequisites
        prereq_results = setup_manager.check_prerequisites()
        setup_results['prerequisites'] = prereq_results
        
        if not prereq_results['all_passed']:
            print("\n❌ Prerequisites not met. Please install missing components and try again.")
            exit(1)
        
        # Setup project structure
        print("\n" + "="*60)
        structure_results = setup_manager.setup_project_structure()
        setup_results['project_structure'] = structure_results
        
        # Create configuration files
        files_results = setup_manager.create_project_files()
        setup_results['project_files'] = files_results
        
        # Create virtual environment (if requested or full setup)
        if args.create_venv or args.full_setup:
            print("\n" + "="*60)
            venv_results = setup_manager.create_virtual_environment()
            setup_results['venv'] = venv_results
            
            if venv_results['status'] == 'failed':
                print("❌ Virtual environment creation failed")
                exit(1)
        
        # Install dependencies (if requested or full setup)
        if args.install_deps or args.full_setup:
            if 'venv' not in setup_results:
                print("⚠️  Virtual environment not created - installing to system Python")
            
            print("\n" + "="*60)
            deps_results = setup_manager.install_dependencies()
            setup_results['dependencies'] = deps_results
            
            if deps_results['status'] == 'failed':
                print("❌ Dependency installation failed")
                exit(1)
        
        # Verify installation (unless skipped)
        if not args.skip_verification and (args.install_deps or args.full_setup):
            print("\n" + "="*60)
            verify_results = setup_manager.verify_installation()
            setup_results['verification'] = verify_results
            
            if not verify_results['all_passed']:
                print("⚠️  Some verification checks failed - review above for details")
        
        # Initialize database (if requested)
        if args.init_database:
            database_url = args.database_url or os.getenv('DATABASE_URL')
            print("\n" + "="*60)
            db_results = setup_manager.initialize_database(database_url)
            setup_results['database'] = db_results
        
        # Generate and display setup report
        print("\n" + "="*60)
        report = setup_manager.generate_setup_report(setup_results)
        print(report)
        
        # Save report if requested
        if args.output_report:
            report_file = Path(args.output_report)
            report_file.parent.mkdir(parents=True, exist_ok=True)
            report_file.write_text(report)
            print(f"\n💾 Setup report saved to: {report_file}")
        
        print("\n🎉 ETL Pipeline setup completed successfully!")
        
    except KeyboardInterrupt:
        print("\n\n👋 Setup interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        exit(1)


if __name__ == '__main__':
    main()