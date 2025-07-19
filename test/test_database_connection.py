"""
Tests for Database Connection Management

This module tests the database connection functionality including
connection management, query execution, transactions, and health monitoring.
"""

import pytest
import sqlite3
import tempfile
import os
import time
from pathlib import Path
import sys
from unittest.mock import Mock, patch, MagicMock

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from db.database.connection import DatabaseConnection


class TestDatabaseConnection:
    """Test DatabaseConnection class functionality."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database path."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        yield db_path
        # Cleanup - close any connections first
        try:
            if os.path.exists(db_path):
                os.unlink(db_path)
        except PermissionError:
            # File might be in use, skip cleanup
            pass
    
    @pytest.fixture
    def mock_logger(self):
        """Create mock logger."""
        return Mock()
    
    @pytest.fixture
    def db_connection(self, temp_db_path, mock_logger):
        """Create DatabaseConnection instance."""
        return DatabaseConnection(temp_db_path, logger=mock_logger)
    
    def test_initialization(self, temp_db_path, mock_logger):
        """Test DatabaseConnection initialization."""
        conn = DatabaseConnection(temp_db_path, logger=mock_logger)
        
        assert conn.database_path == Path(temp_db_path)
        assert conn.logger == mock_logger
        assert conn._connection is None
        assert conn.config['connection_timeout'] == 60.0
        assert conn.config['busy_timeout'] == 60000
        assert conn.config['retry_attempts'] == 3
    
    def test_initialization_with_custom_config(self, temp_db_path, mock_logger):
        """Test DatabaseConnection initialization with custom config."""
        custom_config = {
            'connection_timeout': 30.0,
            'busy_timeout': 30000,
            'retry_attempts': 5
        }
        
        conn = DatabaseConnection(temp_db_path, logger=mock_logger, config=custom_config)
        
        assert conn.config['connection_timeout'] == 30.0
        assert conn.config['busy_timeout'] == 30000
        assert conn.config['retry_attempts'] == 5
    
    def test_connect_success(self, db_connection):
        """Test successful database connection."""
        conn = db_connection.connect()
        
        assert conn is not None
        assert isinstance(conn, sqlite3.Connection)
        assert db_connection._connection is not None
        
        # Test that connection is configured properly
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys")
        result = cursor.fetchone()
        assert result[0] == 1  # Foreign keys should be enabled
    
    def test_connect_creates_directory(self, temp_db_path, mock_logger):
        """Test that connection creates database directory if needed."""
        # Create path in non-existent directory
        db_path = Path(temp_db_path).parent / "nonexistent" / "test.db"
        
        conn_manager = DatabaseConnection(str(db_path), logger=mock_logger)
        db_conn = conn_manager.connect()
        
        assert db_conn is not None
        assert db_path.parent.exists()
        assert db_path.exists()
    
    def test_connect_multiple_calls(self, db_connection):
        """Test that multiple connect calls return the same connection."""
        conn1 = db_connection.connect()
        conn2 = db_connection.connect()
        
        assert conn1 is conn2
        assert db_connection._connection is conn1
    
    def test_disconnect(self, db_connection):
        """Test database disconnection."""
        # Connect first
        db_connection.connect()
        assert db_connection._connection is not None
        
        # Disconnect
        db_connection.disconnect()
        assert db_connection._connection is None
    
    def test_disconnect_no_connection(self, db_connection):
        """Test disconnection when no connection exists."""
        # Should not raise an exception
        db_connection.disconnect()
        assert db_connection._connection is None
    
    def test_execute_query_success(self, db_connection):
        """Test successful query execution."""
        # Create a simple table
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute("INSERT INTO test VALUES (1, 'test1'), (2, 'test2')")
        conn.commit()
        
        # Execute query
        results = db_connection.execute_query("SELECT * FROM test ORDER BY id")
        
        assert results is not None
        assert len(results) == 2
        # Convert Row objects to tuples for comparison
        row1 = tuple(results[0])
        row2 = tuple(results[1])
        assert row1 == (1, 'test1')
        assert row2 == (2, 'test2')
    
    def test_execute_query_with_params(self, db_connection):
        """Test query execution with parameters."""
        # Create a simple table
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute("INSERT INTO test VALUES (1, 'test1'), (2, 'test2')")
        conn.commit()
        
        # Execute query with parameters
        results = db_connection.execute_query("SELECT * FROM test WHERE id = ?", (1,))
        
        assert results is not None
        assert len(results) == 1
        # Convert Row object to tuple for comparison
        row1 = tuple(results[0])
        assert row1 == (1, 'test1')
    
    def test_execute_query_no_results(self, db_connection):
        """Test query execution without fetching results."""
        # Create a simple table
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        conn.commit()
        
        # Execute INSERT without fetching results
        result = db_connection.execute_query("INSERT INTO test VALUES (1, 'test1')", fetch_results=False)
        
        assert result is None
        
        # Verify the insert worked
        results = db_connection.execute_query("SELECT * FROM test")
        assert len(results) == 1
        # Convert Row object to tuple for comparison
        row_tuple = tuple(results[0])
        assert row_tuple == (1, 'test1')
    
    def test_execute_query_retry_on_locked(self, db_connection):
        """Test query retry on database locked error."""
        # Mock the connection to simulate database locked error
        with patch.object(db_connection, 'connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            
            # First call raises OperationalError, second succeeds
            mock_cursor.execute.side_effect = [
                sqlite3.OperationalError("database is locked"),
                None
            ]
            mock_cursor.fetchall.return_value = [(1, 'test')]
            
            mock_connect.return_value = mock_conn
            
            # Execute query
            results = db_connection.execute_query("SELECT * FROM test")
            
            assert results == [(1, 'test')]
            assert mock_cursor.execute.call_count == 2
    
    def test_execute_query_max_retries_exceeded(self, db_connection):
        """Test query execution when max retries are exceeded."""
        # Mock the connection to always raise database locked error
        with patch.object(db_connection, 'connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            
            # Always raise OperationalError
            mock_cursor.execute.side_effect = sqlite3.OperationalError("database is locked")
            
            mock_connect.return_value = mock_conn
            
            # Execute query should raise exception
            with pytest.raises(sqlite3.OperationalError):
                db_connection.execute_query("SELECT * FROM test")
    
    def test_execute_many(self, db_connection):
        """Test batch query execution."""
        # Create a simple table
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        conn.commit()
        
        # Execute batch insert
        params_list = [(1, 'test1'), (2, 'test2'), (3, 'test3')]
        rows_affected = db_connection.execute_many("INSERT INTO test VALUES (?, ?)", params_list)
        
        assert rows_affected == 3
        
        # Verify inserts
        results = db_connection.execute_query("SELECT * FROM test ORDER BY id")
        assert len(results) == 3
        # Convert Row objects to tuples for comparison
        assert tuple(results[0]) == (1, 'test1')
        assert tuple(results[1]) == (2, 'test2')
        assert tuple(results[2]) == (3, 'test3')
    
    def test_transaction_success(self, db_connection):
        """Test successful transaction."""
        # Create a simple table
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        conn.commit()
        
        # Execute transaction
        with db_connection.transaction():
            db_connection.execute_query("INSERT INTO test VALUES (1, 'test1')", fetch_results=False)
            db_connection.execute_query("INSERT INTO test VALUES (2, 'test2')", fetch_results=False)
        
        # Verify both inserts were committed
        results = db_connection.execute_query("SELECT * FROM test ORDER BY id")
        assert len(results) == 2
        # Convert Row objects to tuples for comparison
        assert tuple(results[0]) == (1, 'test1')
        assert tuple(results[1]) == (2, 'test2')
    
    def test_transaction_rollback(self, db_connection):
        """Test transaction rollback on error."""
        # Create a simple table
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        conn.commit()
        
        # Execute transaction that fails
        with pytest.raises(Exception):
            with db_connection.transaction():
                db_connection.execute_query("INSERT INTO test VALUES (1, 'test1')", fetch_results=False)
                raise Exception("Simulated error")
        
        # Verify no inserts were committed
        results = db_connection.execute_query("SELECT * FROM test")
        assert len(results) == 0
    
    def test_check_health_success(self, db_connection):
        """Test successful health check."""
        # Create a simple table for testing
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER)")
        conn.commit()
        
        # Check health
        health_results = db_connection.check_health()
        
        assert health_results['connection_healthy'] is True
        assert health_results['integrity_ok'] is True
        assert health_results['table_count'] >= 1
        assert 'database_size_mb' in health_results
        assert 'journal_mode' in health_results
        assert 'cache_size' in health_results
        assert 'query_response_time_ms' in health_results
        assert health_results['cached'] is False
    
    def test_check_health_cached(self, db_connection):
        """Test health check caching."""
        # First health check
        health1 = db_connection.check_health()
        assert health1['cached'] is False
        
        # Second health check should be cached
        health2 = db_connection.check_health()
        assert health2['cached'] is True
    
    def test_check_health_failure(self, db_connection):
        """Test health check failure."""
        # Mock connection to raise exception
        with patch.object(db_connection, 'connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            health_results = db_connection.check_health()
            
            assert health_results['connection_healthy'] is False
            assert 'error' in health_results
            assert health_results['error'] == "Connection failed"
    
    def test_optimize_database(self, db_connection):
        """Test database optimization."""
        # Create some data for optimization
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        cursor.execute("CREATE INDEX idx_test_id ON test(id)")
        
        # Insert some data
        for i in range(100):
            cursor.execute("INSERT INTO test VALUES (?, ?)", (i, f"test{i}"))
        conn.commit()
        
        # Optimize database
        success = db_connection.optimize_database()
        
        assert success is True
    
    def test_optimize_database_failure(self, db_connection):
        """Test database optimization failure."""
        # Mock cursor to raise exception
        with patch.object(db_connection, 'connect') as mock_connect:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            
            # VACUUM fails
            mock_cursor.execute.side_effect = Exception("VACUUM failed")
            
            mock_connect.return_value = mock_conn
            
            success = db_connection.optimize_database()
            
            assert success is False
    
    def test_get_table_info(self, db_connection):
        """Test getting table information."""
        # Create a test table
        conn = db_connection.connect()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        cursor.execute("INSERT INTO test VALUES (1, 'test1'), (2, 'test2')")
        conn.commit()
        
        # Get table info
        table_info = db_connection.get_table_info('test')
        
        assert table_info is not None
        assert table_info['table_name'] == 'test'
        assert table_info['row_count'] == 2
        assert len(table_info['columns']) == 2
        
        # Check column details
        id_col = next(col for col in table_info['columns'] if col['name'] == 'id')
        name_col = next(col for col in table_info['columns'] if col['name'] == 'name')
        
        assert id_col['type'] == 'INTEGER'
        assert id_col['primary_key'] is True
        assert name_col['type'] == 'TEXT'
        assert name_col['not_null'] is True
    
    def test_get_table_info_nonexistent(self, db_connection):
        """Test getting info for nonexistent table."""
        db_connection.connect()
        
        table_info = db_connection.get_table_info('nonexistent')
        
        assert table_info is None
    
    def test_get_database_stats(self, db_connection):
        """Test getting database statistics."""
        # Create some test data
        conn = db_connection.connect()
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("CREATE TABLE test1 (id INTEGER)")
        cursor.execute("CREATE TABLE test2 (id INTEGER)")
        cursor.execute("CREATE VIEW test_view AS SELECT * FROM test1")
        cursor.execute("CREATE INDEX idx_test1 ON test1(id)")
        
        # Insert data
        cursor.execute("INSERT INTO test1 VALUES (1), (2)")
        cursor.execute("INSERT INTO test2 VALUES (3)")
        conn.commit()
        
        # Get database stats
        stats = db_connection.get_database_stats()
        
        assert stats['table_count'] == 2
        assert stats['view_count'] == 1
        assert stats['index_count'] >= 1
        assert stats['total_rows'] == 3
        assert 'test1' in stats['tables']
        assert 'test2' in stats['tables']
        assert stats['tables']['test1']['row_count'] == 2
        assert stats['tables']['test2']['row_count'] == 1
    
    def test_get_database_stats_error(self, db_connection):
        """Test database stats with error."""
        # Mock connection to raise exception
        with patch.object(db_connection, 'connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            stats = db_connection.get_database_stats()
            
            assert 'error' in stats
            assert stats['error'] == "Connection failed"
    
    def test_connection_configuration(self, db_connection):
        """Test that connection is properly configured."""
        conn = db_connection.connect()
        cursor = conn.cursor()
        
        # Check WAL mode
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        assert journal_mode == 'wal'
        
        # Check cache size
        cursor.execute("PRAGMA cache_size")
        cache_size = cursor.fetchone()[0]
        assert cache_size > 0
        
        # Check foreign keys
        cursor.execute("PRAGMA foreign_keys")
        foreign_keys = cursor.fetchone()[0]
        assert foreign_keys == 1
        
        # Check row factory
        assert conn.row_factory == sqlite3.Row
    
    def test_connection_thread_safety(self, db_connection):
        """Test connection thread safety."""
        import threading
        
        def connect_in_thread():
            return db_connection.connect()
        
        # Create multiple threads
        threads = []
        results = []
        
        for i in range(5):
            thread = threading.Thread(target=lambda: results.append(connect_in_thread()))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # All should return the same connection
        assert len(results) == 5
        assert all(conn is results[0] for conn in results)
    
    def test_connection_cleanup(self, db_connection):
        """Test proper connection cleanup."""
        # Connect and disconnect multiple times
        for i in range(3):
            conn = db_connection.connect()
            assert conn is not None
            db_connection.disconnect()
            assert db_connection._connection is None 