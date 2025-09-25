"""Test watermark table DDL contract expectations.

This test MUST fail initially until the table is created and matches the contract.
"""
import pytest

# Note: This will fail until we add psycopg to dependencies and set up DB connection
try:
    import psycopg
except ImportError:
    pytest.skip("psycopg not available", allow_module_level=True)


def test_watermark_table_exists(pg_connection):
    """Test that watermark table exists with correct structure."""
    if pg_connection is None:
        pytest.skip("Database connection not available")
    
    # This will fail until table is created
    with pg_connection.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'capture_insights'
            AND table_name = 'meta_refresh_watermarks'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
    
    # Expect these columns based on contract
    expected_columns = {
        'pipeline_name': ('text', 'NO'),
        'last_modified_to': ('timestamp with time zone', 'NO'),
        'overlap_days': ('integer', 'NO'),
        'updated_at': ('timestamp with time zone', 'NO')
    }
    
    assert len(columns) == len(expected_columns), f"Expected {len(expected_columns)} columns, got {len(columns)}"
    
    for col_name, data_type, is_nullable, column_default in columns:
        assert col_name in expected_columns, f"Unexpected column: {col_name}"
        expected_type, expected_nullable = expected_columns[col_name]
        
        # Handle PostgreSQL type variations
        if expected_type == 'text' and data_type in ('character varying', 'text'):
            pass  # Both are acceptable
        elif expected_type == 'timestamp with time zone' and data_type == 'timestamp with time zone':
            pass
        else:
            assert data_type == expected_type, f"Column {col_name}: expected {expected_type}, got {data_type}"
        
        assert is_nullable == expected_nullable, f"Column {col_name}: expected nullable={expected_nullable}, got {is_nullable}"
        
        # Check defaults
        if col_name == 'overlap_days':
            assert '7' in str(column_default), f"overlap_days should default to 7, got {column_default}"
        elif col_name == 'updated_at':
            assert 'now()' in str(column_default), f"updated_at should default to now(), got {column_default}"


def test_watermark_table_primary_key(pg_connection):
    """Test that watermark table has primary key on pipeline_name."""
    if pg_connection is None:
        pytest.skip("Database connection not available")
    
    # Check primary key
    with pg_connection.cursor() as cur:
        cur.execute("""
            SELECT constraint_name, constraint_type, column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'capture_insights'
            AND tc.table_name = 'meta_refresh_watermarks'
            AND tc.constraint_type = 'PRIMARY KEY'
        """)
        pk_columns = cur.fetchall()
    
    assert len(pk_columns) == 1, f"Expected 1 primary key column, got {len(pk_columns)}"
    assert pk_columns[0][2] == 'pipeline_name', f"Primary key should be on pipeline_name, got {pk_columns[0][2]}"


def test_watermark_table_defaults(pg_connection):
    """Test that watermark table defaults work correctly."""
    if pg_connection is None:
        pytest.skip("Database connection not available")
    
    # This will fail until table exists with proper defaults
    with pg_connection.cursor() as cur:
        cur.execute("""
            INSERT INTO capture_insights.meta_refresh_watermarks 
            (pipeline_name, last_modified_to)
            VALUES 
            ('test_pipeline', '2024-01-01T00:00:00Z')
            RETURNING overlap_days, updated_at
        """)
        row = cur.fetchone()
        overlap_days, updated_at = row
        
        assert overlap_days == 7, f"overlap_days should default to 7, got {overlap_days}"
        assert updated_at is not None, "updated_at should have default value"
        
        # Clean up
        cur.execute("ROLLBACK")


def test_watermark_table_upsert_capability(pg_connection):
    """Test that watermark table supports ON CONFLICT updates."""
    if pg_connection is None:
        pytest.skip("Database connection not available")
    
    # This will fail until table is created
    with pg_connection.cursor() as cur:
        # Insert initial record
        cur.execute("""
            INSERT INTO capture_insights.meta_refresh_watermarks 
            (pipeline_name, last_modified_to, overlap_days)
            VALUES 
            ('test_pipeline', '2024-01-01T00:00:00Z', 7)
        """)
        
        # Update with ON CONFLICT
        cur.execute("""
            INSERT INTO capture_insights.meta_refresh_watermarks 
            (pipeline_name, last_modified_to, overlap_days)
            VALUES 
            ('test_pipeline', '2024-01-02T00:00:00Z', 10)
            ON CONFLICT (pipeline_name) 
            DO UPDATE SET 
                last_modified_to = EXCLUDED.last_modified_to,
                overlap_days = EXCLUDED.overlap_days,
                updated_at = now()
            RETURNING last_modified_to, overlap_days
        """)
        
        row = cur.fetchone()
        last_modified_to, overlap_days = row
        
        # Should have updated values
        assert str(last_modified_to).startswith('2024-01-02'), "last_modified_to should be updated"
        assert overlap_days == 10, f"overlap_days should be updated to 10, got {overlap_days}"
        
        # Clean up
        cur.execute("ROLLBACK")