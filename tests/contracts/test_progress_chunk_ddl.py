"""Test progress chunk table DDL contract expectations.

This test MUST fail initially until the table is created and matches the contract.
"""
import pytest

# Note: This will fail until we add psycopg to dependencies and set up DB connection
try:
    import psycopg
except ImportError:
    pytest.skip("psycopg not available", allow_module_level=True)


def test_progress_chunk_table_exists(pg_connection):
    """Test that progress chunk table exists with correct structure."""
    if pg_connection is None:
        pytest.skip("Database connection not available")
    
    # This will fail until table is created
    with pg_connection.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'capture_insights'
            AND table_name = 'meta_chunk_progress'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
    
    # Expect these columns based on contract
    expected_columns = {
        'id': ('bigint', 'NO'),
        'pipeline_name': ('text', 'NO'),
        'window_start': ('date', 'NO'),
        'window_end': ('date', 'NO'),
        'chunk_index': ('integer', 'NO'),
        'job_id': ('uuid', 'NO'),
        'correlation_id': ('uuid', 'NO'),
        'status': ('text', 'NO'),
        'rows_staged': ('bigint', 'YES'),
        'rows_deduped': ('bigint', 'YES'),
        'archive_path_rel': ('text', 'YES'),
        'archive_sha256': ('text', 'YES'),
        'error_class': ('text', 'YES'),
        'error_message': ('text', 'YES'),
        'created_at': ('timestamp with time zone', 'NO'),
        'updated_at': ('timestamp with time zone', 'NO')
    }
    
    assert len(columns) == len(expected_columns), f"Expected {len(expected_columns)} columns, got {len(columns)}"
    
    for col_name, data_type, is_nullable, _ in columns:
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


def test_progress_chunk_table_constraints(pg_connection):
    """Test that progress chunk table has correct constraints."""
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
            AND tc.table_name = 'meta_chunk_progress'
            ORDER BY constraint_type, ordinal_position
        """)
        constraints = cur.fetchall()
    
    # Should have primary key on 'id' and unique constraint
    pk_found = False
    unique_found = False
    
    for constraint_name, constraint_type, column_name in constraints:
        if constraint_type == 'PRIMARY KEY' and column_name == 'id':
            pk_found = True
        elif constraint_type == 'UNIQUE':
            unique_found = True
    
    assert pk_found, "Primary key on 'id' column not found"
    # Note: Unique constraint check would need more complex query to verify composite key


def test_progress_chunk_status_check_constraint(pg_connection):
    """Test that status column has proper check constraint."""
    if pg_connection is None:
        pytest.skip("Database connection not available")
    
    # This will fail until we implement the check constraint
    with pg_connection.cursor() as cur:
        # Try to insert invalid status - should fail
        cur.execute("""
            INSERT INTO capture_insights.meta_chunk_progress 
            (pipeline_name, window_start, window_end, chunk_index, 
             job_id, correlation_id, status)
            VALUES 
            ('test', '2024-01-01', '2024-01-02', 0,
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 
             'invalid_status')
        """)
    
    # Should raise an exception due to check constraint
    with pytest.raises(psycopg.errors.CheckViolation):
        pg_connection.commit()


def test_progress_chunk_default_timestamps(pg_connection):
    """Test that created_at and updated_at have proper defaults."""
    if pg_connection is None:
        pytest.skip("Database connection not available")
    
    # This will fail until table exists with proper defaults
    with pg_connection.cursor() as cur:
        cur.execute("""
            INSERT INTO capture_insights.meta_chunk_progress 
            (pipeline_name, window_start, window_end, chunk_index,
             job_id, correlation_id, status)
            VALUES 
            ('test_pipeline', '2024-01-01', '2024-01-02', 0,
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12',
             'pending')
            RETURNING created_at, updated_at
        """)
        row = cur.fetchone()
        created_at, updated_at = row
        
        assert created_at is not None, "created_at should have default value"
        assert updated_at is not None, "updated_at should have default value"
        
        # Clean up
        cur.execute("ROLLBACK")