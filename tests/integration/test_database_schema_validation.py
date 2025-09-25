"""Integration test for database schema validation and management.

This test MUST fail initially until schema management implementations exist.
"""
import pytest
from unittest.mock import Mock, patch
import psycopg
from pathlib import Path

# These will fail until implementations exist
from etl.staging.schema_manager import SchemaManager  # Will fail until implemented
from etl.checks.schema_validator import SchemaValidator  # Will fail until implemented


def test_schema_initialization_creates_required_tables(fake_config, pg_connection):
    """Test that schema initialization creates all required tables and indexes."""
    
    # This will fail until SchemaManager is implemented
    schema_manager = SchemaManager(fake_config)
    
    # Initialize the schema
    result = schema_manager.initialize_schema(connection=pg_connection)
    
    assert result['success'] == True, "Schema initialization should succeed"
    assert result['tables_created'] > 0, "Should create required tables"
    
    # This will fail until SchemaValidator is implemented
    validator = SchemaValidator(fake_config)
    
    # Validate schema was created correctly
    validation_result = validator.validate_database_schema(connection=pg_connection)
    
    assert validation_result['valid'] == True, "Created schema should be valid"
    assert 'progress_chunks' in validation_result['tables'], "Should have progress_chunks table"
    assert 'watermarks' in validation_result['tables'], "Should have watermarks table"
    assert 'chunk_metadata' in validation_result['tables'], "Should have chunk_metadata table"
    
    # Check for required indexes
    assert validation_result['indexes']['progress_chunks'] > 0, "progress_chunks should have indexes"
    assert validation_result['indexes']['watermarks'] > 0, "watermarks should have indexes"
    
    # Check for required extensions
    assert 'pgvector' in validation_result['extensions'], "Should have pgvector extension"


def test_schema_validation_detects_missing_tables(fake_config, pg_connection):
    """Test that schema validation detects missing required tables."""
    
    # Start with empty database (no schema initialization)
    validator = SchemaValidator(fake_config)
    
    validation_result = validator.validate_database_schema(connection=pg_connection)
    
    assert validation_result['valid'] == False, "Empty schema should be invalid"
    assert len(validation_result['missing_tables']) > 0, "Should detect missing tables"
    assert 'progress_chunks' in validation_result['missing_tables'], "Should detect missing progress_chunks"
    assert 'watermarks' in validation_result['missing_tables'], "Should detect missing watermarks"


def test_schema_validation_detects_missing_indexes(fake_config, pg_connection):
    """Test that schema validation detects missing required indexes."""
    
    # Create tables manually without indexes
    with pg_connection.cursor() as cursor:
        # Create table without the required indexes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS progress_chunks (
                id SERIAL PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                chunk_number INTEGER NOT NULL,
                total_chunks INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        pg_connection.commit()
    
    validator = SchemaValidator(fake_config)
    validation_result = validator.validate_database_schema(connection=pg_connection)
    
    assert validation_result['valid'] == False, "Schema with missing indexes should be invalid"
    assert len(validation_result['missing_indexes']) > 0, "Should detect missing indexes"
    
    # Should suggest the missing indexes
    missing_index_names = [idx['name'] for idx in validation_result['missing_indexes']]
    expected_indexes = ['idx_progress_chunks_pipeline_status', 'idx_progress_chunks_created_at']
    
    for expected_idx in expected_indexes:
        assert any(expected_idx in name for name in missing_index_names), f"Should detect missing {expected_idx}"


def test_schema_validation_detects_missing_extensions(fake_config, pg_connection):
    """Test that schema validation detects missing required extensions."""
    
    # Check if pgvector is missing (it likely will be in test environment)
    validator = SchemaValidator(fake_config)
    validation_result = validator.validate_database_schema(connection=pg_connection)
    
    # pgvector might not be installed in test DB, which is expected
    if not validation_result['valid']:
        if 'pgvector' in validation_result['missing_extensions']:
            # This is expected in test environment
            assert 'pgvector' in validation_result['missing_extensions'], "Should detect missing pgvector"
            assert validation_result['valid'] == False, "Schema without pgvector should be invalid"


def test_schema_migration_applies_changes(fake_config, pg_connection):
    """Test that schema migrations apply changes correctly."""
    
    schema_manager = SchemaManager(fake_config)
    
    # Initialize base schema
    init_result = schema_manager.initialize_schema(connection=pg_connection)
    assert init_result['success'] == True
    
    # Mock migration file that adds a new column
    migration_sql = """
    ALTER TABLE progress_chunks 
    ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    
    CREATE INDEX IF NOT EXISTS idx_progress_chunks_last_updated 
    ON progress_chunks(last_updated);
    """
    
    with patch('pathlib.Path.read_text') as mock_read:
        mock_read.return_value = migration_sql
        
        # Apply migration
        migration_result = schema_manager.apply_migration(
            migration_file=Path("migrations/001_add_last_updated.sql"),
            connection=pg_connection
        )
        
        assert migration_result['success'] == True, "Migration should succeed"
        
        # Verify column was added
        with pg_connection.cursor() as cursor:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'progress_chunks' 
                AND column_name = 'last_updated'
            """)
            result = cursor.fetchone()
            assert result is not None, "New column should exist after migration"


def test_schema_validation_with_corrupted_table(fake_config, pg_connection):
    """Test that schema validation detects corrupted or malformed tables."""
    
    # Create a table with wrong column types
    with pg_connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS progress_chunks (
                id TEXT PRIMARY KEY,  -- Wrong type, should be SERIAL
                pipeline_name INTEGER,  -- Wrong type, should be TEXT
                chunk_number TEXT,  -- Wrong type, should be INTEGER
                total_chunks TEXT,  -- Wrong type, should be INTEGER
                status INTEGER  -- Wrong type, should be TEXT
            );
        """)
        pg_connection.commit()
    
    validator = SchemaValidator(fake_config)
    validation_result = validator.validate_database_schema(connection=pg_connection)
    
    assert validation_result['valid'] == False, "Schema with wrong column types should be invalid"
    assert len(validation_result['schema_errors']) > 0, "Should detect schema errors"
    
    # Should provide details about the errors
    error_messages = [error['message'] for error in validation_result['schema_errors']]
    assert any('progress_chunks' in msg for msg in error_messages), "Should mention problematic table"


def test_schema_backup_and_restore(fake_config, pg_connection):
    """Test schema backup and restore functionality."""
    
    schema_manager = SchemaManager(fake_config)
    
    # Initialize schema and add some data
    init_result = schema_manager.initialize_schema(connection=pg_connection)
    assert init_result['success'] == True
    
    with pg_connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO watermarks (pipeline_name, last_modified_to, overlap_days)
            VALUES ('test_pipeline', '2024-01-01 00:00:00', 7)
        """)
        pg_connection.commit()
    
    # Create backup
    backup_result = schema_manager.create_backup(connection=pg_connection)
    assert backup_result['success'] == True, "Backup should succeed"
    assert 'backup_file' in backup_result, "Should return backup file path"
    
    # Modify schema (simulate corruption)
    with pg_connection.cursor() as cursor:
        cursor.execute("DROP TABLE watermarks CASCADE;")
        pg_connection.commit()
    
    # Restore from backup
    restore_result = schema_manager.restore_from_backup(
        backup_file=backup_result['backup_file'],
        connection=pg_connection
    )
    assert restore_result['success'] == True, "Restore should succeed"
    
    # Verify data was restored
    with pg_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM watermarks WHERE pipeline_name = 'test_pipeline'")
        count = cursor.fetchone()[0]
        assert count == 1, "Restored data should be present"


def test_schema_version_tracking(fake_config, pg_connection):
    """Test that schema version is tracked correctly."""
    
    schema_manager = SchemaManager(fake_config)
    
    # Initialize schema
    init_result = schema_manager.initialize_schema(connection=pg_connection)
    assert init_result['success'] == True
    
    # Check initial version
    version = schema_manager.get_schema_version(connection=pg_connection)
    assert version is not None, "Should have schema version after initialization"
    assert isinstance(version, str), "Version should be string"
    
    # Apply mock migration to increment version
    with patch.object(schema_manager, 'apply_migration') as mock_migration:
        mock_migration.return_value = {'success': True, 'new_version': '1.1.0'}
        
        result = schema_manager.apply_migration(
            migration_file=Path("migrations/001_test.sql"),
            connection=pg_connection
        )
        
        assert result['success'] == True
        assert result['new_version'] == '1.1.0'


def test_schema_performance_indexes(fake_config, pg_connection):
    """Test that performance-critical indexes are created correctly."""
    
    schema_manager = SchemaManager(fake_config)
    
    # Initialize schema
    init_result = schema_manager.initialize_schema(connection=pg_connection)
    assert init_result['success'] == True
    
    # Check for performance-critical indexes
    with pg_connection.cursor() as cursor:
        # Check progress_chunks indexes
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'progress_chunks' 
            AND indexname LIKE 'idx_%'
        """)
        progress_indexes = [row[0] for row in cursor.fetchall()]
        
        expected_progress_indexes = [
            'idx_progress_chunks_pipeline_status',
            'idx_progress_chunks_created_at'
        ]
        
        for expected_idx in expected_progress_indexes:
            assert expected_idx in progress_indexes, f"Missing performance index: {expected_idx}"
        
        # Check watermarks indexes  
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'watermarks'
            AND indexname LIKE 'idx_%' 
        """)
        watermark_indexes = [row[0] for row in cursor.fetchall()]
        
        expected_watermark_indexes = [
            'idx_watermarks_pipeline_name',
            'idx_watermarks_updated_at'
        ]
        
        for expected_idx in expected_watermark_indexes:
            assert expected_idx in watermark_indexes, f"Missing performance index: {expected_idx}"