"""
Tests for Database Schema Management

This module tests the complete database schema functionality including
table creation, view creation, validation, and schema management.
"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
import sys
from unittest.mock import Mock, patch

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from db.database.schema import DatabaseSchema


class TestDatabaseSchema:
    """Test DatabaseSchema class functionality."""
    
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
    def schema(self, mock_logger):
        """Create DatabaseSchema instance."""
        return DatabaseSchema(logger=mock_logger)
    
    @pytest.fixture
    def db_connection(self, temp_db_path):
        """Create database connection."""
        return sqlite3.connect(temp_db_path)
    
    def test_schema_initialization(self, schema):
        """Test schema initialization with all components."""
        # Check core tables
        expected_core_tables = {
            'universe_historical', 'portfolio_historical', 'combined_runs_historical',
            'run_monitor', 'gspread_analytics'
        }
        assert set(schema.core_tables.keys()) == expected_core_tables
        
        # Check CUSIP tracking tables
        expected_tracking_tables = {
            'unmatched_cusips_all_dates', 'unmatched_cusips_last_date'
        }
        assert set(schema.cusip_tracking_tables.keys()) == expected_tracking_tables
        
        # Check audit tables
        expected_audit_tables = {
            'data_quality_log', 'processing_metadata', 'schema_version_history'
        }
        assert set(schema.audit_tables.keys()) == expected_audit_tables
        
        # Check views
        expected_views = {
            'current_universe', 'current_portfolio', 'current_runs',
            'cusip_match_summary', 'data_quality_dashboard'
        }
        assert set(schema.views.keys()) == expected_views
        
        # Check indexes
        assert len(schema.indexes) > 0
        assert all('idx_' in index_name for index_name in schema.indexes.keys())
    
    def test_create_complete_schema_success(self, schema, db_connection):
        """Test successful schema creation."""
        # Create schema
        success = schema.create_complete_schema(db_connection)
        assert success is True
        
        # Verify tables were created
        cursor = db_connection.cursor()
        
        # Check core tables
        for table_name in schema.core_tables.keys():
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            result = cursor.fetchone()
            assert result is not None, f"Table {table_name} was not created"
        
        # Check tracking tables
        for table_name in schema.cusip_tracking_tables.keys():
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            result = cursor.fetchone()
            assert result is not None, f"Table {table_name} was not created"
        
        # Check audit tables
        for table_name in schema.audit_tables.keys():
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            result = cursor.fetchone()
            assert result is not None, f"Table {table_name} was not created"
        
        # Check views
        for view_name in schema.views.keys():
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='view' AND name='{view_name}'")
            result = cursor.fetchone()
            assert result is not None, f"View {view_name} was not created"
        
        # Check indexes
        for index_name in schema.indexes.keys():
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='index' AND name='{index_name}'")
            result = cursor.fetchone()
            assert result is not None, f"Index {index_name} was not created"
        
        # Check schema version history
        cursor.execute("SELECT COUNT(*) FROM schema_version_history")
        count = cursor.fetchone()[0]
        assert count > 0, "Schema version history should contain at least one entry"
    
    def test_create_complete_schema_failure(self, schema, db_connection):
        """Test schema creation failure handling."""
        # Test with invalid SQL instead of mocking cursor
        # Create a schema with invalid SQL to trigger failure
        original_schema = schema.core_tables['universe_historical']
        schema.core_tables['universe_historical'] = "INVALID SQL STATEMENT"
        
        try:
            success = schema.create_complete_schema(db_connection)
            assert success is False
        finally:
            # Restore original schema
            schema.core_tables['universe_historical'] = original_schema
    
    def test_validate_schema_success(self, schema, db_connection):
        """Test schema validation with complete schema."""
        # Create complete schema first
        schema.create_complete_schema(db_connection)
        
        # Validate schema
        validation_results = schema.validate_schema(db_connection)
        
        assert validation_results['schema_valid'] is True
        assert len(validation_results['missing_tables']) == 0
        assert len(validation_results['missing_views']) == 0
        assert len(validation_results['existing_tables']) > 0
        assert len(validation_results['existing_views']) > 0
        assert len(validation_results['existing_indexes']) > 0
    
    def test_validate_schema_missing_components(self, schema, db_connection):
        """Test schema validation with missing components."""
        # Create only some tables
        cursor = db_connection.cursor()
        cursor.execute(schema.core_tables['universe_historical'])
        cursor.execute(schema.core_tables['portfolio_historical'])
        db_connection.commit()
        
        # Validate schema
        validation_results = schema.validate_schema(db_connection)
        
        assert validation_results['schema_valid'] is False
        assert len(validation_results['missing_tables']) > 0
        assert len(validation_results['missing_views']) > 0
        assert 'universe_historical' in validation_results['existing_tables']
        assert 'portfolio_historical' in validation_results['existing_tables']
    
    def test_validate_schema_database_error(self, schema, db_connection):
        """Test schema validation with database error."""
        # Test with closed connection to trigger error
        db_connection.close()
        
        validation_results = schema.validate_schema(db_connection)
        
        assert validation_results['schema_valid'] is False
        assert 'error' in validation_results
    
    def test_universe_historical_schema(self, schema):
        """Test universe_historical table schema."""
        sql = schema._get_universe_historical_schema()
        
        # Check required fields
        assert '"Date" DATE NOT NULL' in sql
        assert '"CUSIP" TEXT NOT NULL' in sql
        assert 'cusip_standardized TEXT NOT NULL' in sql
        
        # Check unique constraint
        assert 'UNIQUE("Date", "CUSIP", cusip_standardized)' in sql
        
        # Check primary key
        assert 'id INTEGER PRIMARY KEY AUTOINCREMENT' in sql
        
        # Check bond fields
        assert '"Security" TEXT' in sql
        assert '"Coupon" REAL' in sql
        assert '"Maturity Date" DATE' in sql
        
        # Check spread fields
        assert '"G Sprd" REAL' in sql
        assert '"OAS (Mid)" REAL' in sql
        assert '"YTM (Mid)" REAL' in sql
    
    def test_portfolio_historical_schema(self, schema):
        """Test portfolio_historical table schema."""
        sql = schema._get_portfolio_historical_schema()
        
        # Check required fields
        assert '"Date" DATE NOT NULL' in sql
        assert '"CUSIP" TEXT NOT NULL' in sql
        assert 'cusip_standardized TEXT NOT NULL' in sql
        
        # Check unique constraint
        assert 'UNIQUE("Date", "CUSIP", cusip_standardized)' in sql
        
        # Check position fields
        assert '"QUANTITY" REAL NOT NULL' in sql
        assert '"PRICE" REAL' in sql
        assert '"MARKET VALUE" REAL' in sql
        assert '"WEIGHT" REAL' in sql
        
        # Check matching status
        assert 'universe_match_status TEXT DEFAULT' in sql
    
    def test_combined_runs_historical_schema(self, schema):
        """Test combined_runs_historical table schema."""
        sql = schema._get_combined_runs_historical_schema()
        
        # Check required fields
        assert '"Date" DATE NOT NULL' in sql
        assert '"CUSIP" TEXT NOT NULL' in sql
        assert '"Dealer" TEXT NOT NULL' in sql
        
        # Check unique constraint
        assert 'UNIQUE("Date", "CUSIP", cusip_standardized, "Dealer")' in sql
        
        # Check trade fields
        assert '"Bid" REAL' in sql
        assert '"Ask" REAL' in sql
        assert '"Size" REAL' in sql
        assert '"Spread" REAL' in sql
    
    def test_run_monitor_schema(self, schema):
        """Test run_monitor table schema."""
        sql = schema._get_run_monitor_schema()
        
        # Check required fields
        assert '"CUSIP" TEXT NOT NULL' in sql
        assert 'cusip_standardized TEXT NOT NULL' in sql
        
        # Check unique constraint
        assert 'UNIQUE(cusip_standardized)' in sql
        
        # Check monitoring fields
        assert '"Bid Spread" REAL' in sql
        assert '"Ask Spread" REAL' in sql
        assert '"Total Size" REAL' in sql
        assert '"Total Count" INTEGER' in sql
    
    def test_gspread_analytics_schema(self, schema):
        """Test gspread_analytics table schema."""
        sql = schema._get_gspread_analytics_schema()
        
        # Check required fields
        assert '"CUSIP_1" TEXT NOT NULL' in sql
        assert '"CUSIP_2" TEXT NOT NULL' in sql
        assert '"Date" DATE NOT NULL' in sql
        
        # Check unique constraint
        assert 'UNIQUE("Date", cusip_1_standardized, cusip_2_standardized)' in sql
        
        # Check analytics fields
        assert '"G_Spread_1" REAL' in sql
        assert '"G_Spread_2" REAL' in sql
        assert '"Spread_Diff" REAL' in sql
        assert '"Z_Score" REAL' in sql
        assert '"Correlation" REAL' in sql
    
    def test_current_universe_view(self, schema):
        """Test current_universe view definition."""
        sql = schema._get_current_universe_view()
        
        assert 'CREATE VIEW current_universe AS' in sql
        assert 'SELECT * FROM universe_historical' in sql
        assert 'WHERE date = (SELECT MAX(date) FROM universe_historical)' in sql
    
    def test_current_portfolio_view(self, schema):
        """Test current_portfolio view definition."""
        sql = schema._get_current_portfolio_view()
        
        assert 'CREATE VIEW current_portfolio AS' in sql
        assert 'SELECT * FROM portfolio_historical' in sql
        assert 'WHERE date = (SELECT MAX(date) FROM portfolio_historical)' in sql
    
    def test_current_runs_view(self, schema):
        """Test current_runs view definition."""
        sql = schema._get_current_runs_view()
        
        assert 'CREATE VIEW current_runs AS' in sql
        assert 'SELECT * FROM combined_runs_historical' in sql
        assert 'WHERE date = (SELECT MAX(date) FROM combined_runs_historical)' in sql
    
    def test_cusip_match_summary_view(self, schema):
        """Test cusip_match_summary view definition."""
        sql = schema._get_cusip_match_summary_view()
        
        assert 'CREATE VIEW cusip_match_summary AS' in sql
        assert 'UNION ALL' in sql
        assert 'universe_match_status' in sql
        assert 'match_rate_percent' in sql
    
    def test_data_quality_dashboard_view(self, schema):
        """Test data_quality_dashboard view definition."""
        sql = schema._get_data_quality_dashboard_view()
        
        assert 'CREATE VIEW data_quality_dashboard AS' in sql
        assert 'data_quality_log' in sql
        assert 'check_result' in sql
        assert 'pass_rate_percent' in sql
    
    def test_indexes_creation(self, schema):
        """Test index definitions."""
        indexes = schema._get_indexes()
        
        # Check that all indexes have proper names
        assert all(index_name.startswith('idx_') for index_name in indexes.keys())
        
        # Check that all indexes are CREATE INDEX statements
        assert all('CREATE INDEX' in sql for sql in indexes.values())
        
        # Check specific important indexes
        assert 'idx_universe_date_cusip' in indexes
        assert 'idx_portfolio_date_cusip' in indexes
        assert 'idx_runs_date_cusip' in indexes
        assert 'idx_gspread_cusips' in indexes
    
    def test_schema_rollback_on_error(self, schema, db_connection):
        """Test that schema creation rolls back on error."""
        # Create schema partially
        cursor = db_connection.cursor()
        cursor.execute(schema.core_tables['universe_historical'])
        db_connection.commit()
        
        # Try to create schema again (should fail due to existing table)
        success = schema.create_complete_schema(db_connection)
        
        # Should fail but not crash
        assert success is False
        
        # Check that original table still exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='universe_historical'")
        result = cursor.fetchone()
        assert result is not None, "Original table should still exist after rollback"
    
    def test_schema_version_history(self, schema, db_connection):
        """Test schema version history table."""
        # Create schema
        schema.create_complete_schema(db_connection)
        
        # Check schema version history
        cursor = db_connection.cursor()
        cursor.execute("SELECT schema_version, version_description FROM schema_version_history")
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == '1.0.0'
        assert 'Initial schema creation' in result[1]
    
    def test_audit_tables_schema(self, schema):
        """Test audit tables schema."""
        # Test data quality log
        sql = schema._get_data_quality_log_schema()
        assert 'data_quality_log' in sql
        assert 'check_type TEXT NOT NULL' in sql
        assert 'check_result TEXT NOT NULL' in sql
        
        # Test processing metadata
        sql = schema._get_processing_metadata_schema()
        assert 'processing_metadata' in sql
        assert 'operation_id TEXT NOT NULL' in sql
        assert 'processing_status TEXT NOT NULL' in sql
        
        # Test schema version history
        sql = schema._get_schema_version_history_schema()
        assert 'schema_version_history' in sql
        assert 'schema_version TEXT NOT NULL' in sql
        assert 'applied_at TIMESTAMP' in sql
    
    def test_cusip_tracking_tables_schema(self, schema):
        """Test CUSIP tracking tables schema."""
        # Test unmatched CUSIPs all dates
        sql = schema._get_unmatched_cusips_all_dates_schema()
        assert 'unmatched_cusips_all_dates' in sql
        assert 'source_table TEXT NOT NULL' in sql
        assert 'cusip_original TEXT NOT NULL' in sql
        assert 'match_status TEXT DEFAULT' in sql
        
        # Test unmatched CUSIPs last date
        sql = schema._get_unmatched_cusips_last_date_schema()
        assert 'unmatched_cusips_last_date' in sql
        assert 'source_table TEXT NOT NULL' in sql
        assert 'cusip_original TEXT NOT NULL' in sql
        assert 'match_status TEXT DEFAULT' in sql 