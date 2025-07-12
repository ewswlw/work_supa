"""
Test suite for the dtale application

Tests all major functionality including:
- Data loading and validation
- View creation and filtering
- Performance optimizations
- Error handling
"""

import pytest
import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock
import tempfile

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import the modules to test
try:
    from src.analytics.bond_analytics import BondAnalytics as BondDtaleApp
    from src.utils.dtale_manager import (
        DtaleInstanceManager, 
        DataOptimizer, 
        PerformanceMonitor,
        validate_data_file,
        get_column_info,
        format_memory_usage
    )
    IMPORTS_AVAILABLE = True
except ImportError as e:
    IMPORTS_AVAILABLE = False
    IMPORT_ERROR = str(e)

class TestDtaleApp:
    """Test the main dtale application functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        if not IMPORTS_AVAILABLE:
            pytest.skip(f"Required imports not available: {IMPORT_ERROR}")
        
        # Create sample data
        self.sample_data = self._create_sample_bond_data()
        self.temp_file = None
    
    def teardown_method(self):
        """Clean up after tests."""
        if self.temp_file and self.temp_file.exists():
            self.temp_file.unlink()
    
    def _create_sample_bond_data(self) -> pd.DataFrame:
        """Create sample bond data for testing."""
        np.random.seed(42)
        n_rows = 1000
        
        data = {
            'Security_1': [f'BOND_{i:04d}' for i in range(n_rows)],
            'Security_2': [f'BOND_{i+1000:04d}' for i in range(n_rows)],
            'Last_Spread': np.random.normal(100, 20, n_rows),
            'Z_Score': np.random.normal(0, 1, n_rows),
            'Max': np.random.normal(120, 25, n_rows),
            'Min': np.random.normal(80, 15, n_rows),
            'Last_vs_Max': np.random.normal(-10, 5, n_rows),
            'Last_vs_Min': np.random.normal(10, 5, n_rows),
            'Percentile': np.random.uniform(0, 100, n_rows),
            'XCCY': np.random.normal(0, 5, n_rows),
            'Custom_Sector_1': np.random.choice(['Financial', 'Energy', 'Technology'], n_rows),
            'Custom_Sector_2': np.random.choice(['Financial', 'Energy', 'Technology'], n_rows),
            'Rating_1': np.random.choice(['AAA', 'AA', 'A', 'BBB'], n_rows),
            'Rating_2': np.random.choice(['AAA', 'AA', 'A', 'BBB'], n_rows),
            'Currency_1': np.random.choice(['CAD', 'USD'], n_rows, p=[0.7, 0.3]),
            'Currency_2': np.random.choice(['CAD', 'USD'], n_rows, p=[0.7, 0.3]),
            'Ticker_1': np.random.choice(['AAPL', 'GOOGL', 'MSFT', 'TSLA'], n_rows),
            'Ticker_2': np.random.choice(['AAPL', 'GOOGL', 'MSFT', 'TSLA'], n_rows),
            'Own?': np.random.choice([0, 1], n_rows, p=[0.8, 0.2]),
            'Size @ Best Offer_runs1': np.random.uniform(1000000, 5000000, n_rows),
            'Size @ Best Bid_runs2': np.random.uniform(1000000, 5000000, n_rows),
            'Bid/Offer_runs1': np.random.uniform(0.5, 5.0, n_rows),
            'Bid/Offer_runs2': np.random.uniform(0.5, 5.0, n_rows),
        }
        
        return pd.DataFrame(data)
    
    def _create_temp_parquet_file(self) -> Path:
        """Create a temporary parquet file for testing."""
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / 'test_bond_data.parquet'
        self.sample_data.to_parquet(temp_file, index=False)
        self.temp_file = temp_file
        return temp_file
    
    def test_bond_dtale_app_initialization(self):
        """Test BondDtaleApp initialization."""
        app = BondDtaleApp(sample_size=500, port=40001)
        
        assert app.sample_size == 500
        assert app.port == 40001
        assert app.df_full is None
        assert app.df_sample is None
        assert len(app.views) == 7  # Should have 7 predefined views
        assert 'all' in app.views
        assert 'cad-only' in app.views
        assert 'portfolio' in app.views
    
    def test_data_loading_success(self):
        """Test successful data loading."""
        temp_file = self._create_temp_parquet_file()
        app = BondDtaleApp(data_path=str(temp_file), sample_size=500)
        
        success = app.load_data()
        
        assert success is True
        assert app.df_full is not None
        assert app.df_sample is not None
        assert len(app.df_full) == 1000
        assert len(app.df_sample) <= 500
        assert 'total_rows' in app.stats
        assert app.stats['total_rows'] == 1000
    
    def test_data_loading_file_not_found(self):
        """Test data loading with non-existent file."""
        app = BondDtaleApp(data_path='nonexistent_file.parquet')
        
        success = app.load_data()
        
        assert success is False
        assert app.df_full is None
        assert app.df_sample is None
    
    def test_smart_sampling(self):
        """Test smart sampling functionality."""
        temp_file = self._create_temp_parquet_file()
        app = BondDtaleApp(data_path=str(temp_file), sample_size=100)
        
        app.load_data()
        
        # Should create a sample smaller than original
        assert len(app.df_sample) <= 100
        assert len(app.df_sample) <= len(app.df_full)
        
        # Should include extreme Z-scores
        extreme_z_in_sample = app.df_sample['Z_Score'].abs().max()
        extreme_z_in_full = app.df_full['Z_Score'].abs().max()
        assert extreme_z_in_sample >= extreme_z_in_full * 0.8  # Should capture most extreme values
    
    def test_filter_functions(self):
        """Test all filter functions."""
        temp_file = self._create_temp_parquet_file()
        app = BondDtaleApp(data_path=str(temp_file))
        app.load_data()
        
        # Test CAD only filter
        cad_filtered = app._filter_cad_only(app.df_sample)
        assert all(cad_filtered['Currency_1'] == 'CAD')
        assert all(cad_filtered['Currency_2'] == 'CAD')
        
        # Test same sector filter
        same_sector = app._filter_same_sector(app.df_sample)
        assert all(same_sector['Custom_Sector_1'] == same_sector['Custom_Sector_2'])
        
        # Test portfolio filter
        portfolio = app._filter_portfolio(app.df_sample)
        assert all(portfolio['Own?'] == 1)
        
        # Test same ticker filter
        same_ticker = app._filter_same_ticker(app.df_sample)
        assert all(same_ticker['Currency_1'] == 'CAD')
        assert all(same_ticker['Currency_2'] == 'CAD')
        assert all(same_ticker['Ticker_1'] == same_ticker['Ticker_2'])
    
    @patch('dtale.show')
    def test_create_view_success(self, mock_dtale_show):
        """Test successful view creation."""
        # Mock dtale.show to return a mock instance
        mock_instance = MagicMock()
        mock_instance._url = 'http://localhost:40000/dtale/main/1'
        mock_dtale_show.return_value = mock_instance
        
        temp_file = self._create_temp_parquet_file()
        app = BondDtaleApp(data_path=str(temp_file))
        app.load_data()
        
        result = app.create_view('cad-only')
        
        assert result is not None
        assert 'cad-only' in app.dtale_instances
        mock_dtale_show.assert_called_once()
    
    def test_create_view_invalid_name(self):
        """Test view creation with invalid view name."""
        temp_file = self._create_temp_parquet_file()
        app = BondDtaleApp(data_path=str(temp_file))
        app.load_data()
        
        result = app.create_view('invalid-view')
        
        assert result is None
    
    def test_list_views(self):
        """Test listing available views."""
        app = BondDtaleApp()
        
        # Should not raise an exception
        app.list_views()
        
        # Verify all expected views are present
        expected_views = ['all', 'cad-only', 'same-sector', 'same-ticker', 'portfolio', 'executable', 'cross-currency']
        for view in expected_views:
            assert view in app.views

class TestDtaleInstanceManager:
    """Test the dtale instance manager."""
    
    def setup_method(self):
        """Set up test fixtures."""
        if not IMPORTS_AVAILABLE:
            pytest.skip(f"Required imports not available: {IMPORT_ERROR}")
        
        self.manager = DtaleInstanceManager(base_port=41000)
        self.sample_df = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c'],
            'C': [1.1, 2.2, 3.3]
        })
    
    def teardown_method(self):
        """Clean up after tests."""
        if hasattr(self, 'manager'):
            self.manager.close_all_instances()
    
    @patch('dtale.show')
    def test_create_instance(self, mock_dtale_show):
        """Test creating a dtale instance."""
        mock_instance = MagicMock()
        mock_instance._url = 'http://localhost:41000/dtale/main/1'
        mock_dtale_show.return_value = mock_instance
        
        result = self.manager.create_instance('test', self.sample_df)
        
        assert result is not None
        assert 'test' in self.manager.instances
        assert 'test' in self.manager.stats
        assert self.manager.stats['test']['rows'] == 3
        assert self.manager.stats['test']['columns'] == 3
    
    def test_list_instances(self):
        """Test listing instances."""
        # Initially should be empty
        instances = self.manager.list_instances()
        assert len(instances) == 0
        
        # Add some mock stats
        self.manager.stats['test1'] = {'rows': 100, 'columns': 5}
        self.manager.stats['test2'] = {'rows': 200, 'columns': 10}
        
        instances = self.manager.list_instances()
        assert len(instances) == 2
        assert 'test1' in instances
        assert 'test2' in instances

class TestDataOptimizer:
    """Test the data optimizer utilities."""
    
    def setup_method(self):
        """Set up test fixtures."""
        if not IMPORTS_AVAILABLE:
            pytest.skip(f"Required imports not available: {IMPORT_ERROR}")
    
    def test_optimize_dtypes(self):
        """Test data type optimization."""
        # Create test data with suboptimal dtypes
        df = pd.DataFrame({
            'int_col': pd.Series([1, 2, 3], dtype='int64'),
            'float_col': pd.Series([1.1, 2.2, 3.3], dtype='float64'),
            'cat_col': pd.Series(['A', 'B', 'A'], dtype='object'),
            'unique_col': pd.Series(['X', 'Y', 'Z'], dtype='object')
        })
        
        optimized = DataOptimizer.optimize_dtypes(df)
        
        # Should optimize integer and float columns
        assert optimized['int_col'].dtype == 'int32'
        assert optimized['float_col'].dtype == 'float32'
        
        # Should convert low-cardinality object to category
        assert optimized['cat_col'].dtype.name == 'category'
        
        # Should keep high-cardinality object as object
        assert optimized['unique_col'].dtype == 'object'
    
    def test_create_smart_sample(self):
        """Test smart sampling functionality."""
        # Create test data
        np.random.seed(42)
        df = pd.DataFrame({
            'numeric_col': np.random.normal(0, 1, 1000),
            'category_col': np.random.choice(['A', 'B', 'C'], 1000),
            'priority_col': np.random.choice([1, 2, 3], 1000)
        })
        
        sample = DataOptimizer.create_smart_sample(
            df, 
            sample_size=100, 
            priority_columns=['numeric_col', 'category_col']
        )
        
        assert len(sample) <= 100
        assert len(sample) > 0
        assert set(sample.columns) == set(df.columns)

class TestPerformanceMonitor:
    """Test the performance monitoring utilities."""
    
    def setup_method(self):
        """Set up test fixtures."""
        if not IMPORTS_AVAILABLE:
            pytest.skip(f"Required imports not available: {IMPORT_ERROR}")
        
        self.monitor = PerformanceMonitor()
    
    def test_record_metrics(self):
        """Test recording performance metrics."""
        self.monitor.record_load_time('test_dataset', 1.5)
        self.monitor.record_memory_usage('test_dataset', 100.0)
        self.monitor.record_row_count('test_dataset', 10000)
        
        metrics = self.monitor.get_metrics('test_dataset')
        
        assert metrics['load_time'] == 1.5
        assert metrics['memory_mb'] == 100.0
        assert metrics['rows'] == 10000
    
    def test_get_all_metrics(self):
        """Test getting all metrics."""
        self.monitor.record_load_time('dataset1', 1.0)
        self.monitor.record_load_time('dataset2', 2.0)
        
        all_metrics = self.monitor.get_all_metrics()
        
        assert len(all_metrics) == 2
        assert 'dataset1' in all_metrics
        assert 'dataset2' in all_metrics
    
    def test_print_summary(self):
        """Test printing performance summary."""
        self.monitor.record_load_time('test', 1.5)
        self.monitor.record_memory_usage('test', 100.0)
        self.monitor.record_row_count('test', 10000)
        
        # Should not raise an exception
        self.monitor.print_summary()

class TestUtilityFunctions:
    """Test utility functions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        if not IMPORTS_AVAILABLE:
            pytest.skip(f"Required imports not available: {IMPORT_ERROR}")
    
    def test_validate_data_file_success(self):
        """Test successful file validation."""
        # Create a temporary CSV file
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / 'test_data.csv'
        
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        df.to_csv(temp_file, index=False)
        
        try:
            is_valid, message = validate_data_file(str(temp_file))
            assert is_valid is True
            assert message == "File is valid"
        finally:
            if temp_file.exists():
                temp_file.unlink()
    
    def test_validate_data_file_not_found(self):
        """Test file validation with non-existent file."""
        is_valid, message = validate_data_file('nonexistent_file.csv')
        
        assert is_valid is False
        assert "File not found" in message
    
    def test_get_column_info(self):
        """Test getting column information."""
        df = pd.DataFrame({
            'numeric_col': [1, 2, 3, 4, 5],
            'text_col': ['a', 'b', 'c', 'd', 'e'],
            'null_col': [1, 2, None, 4, None]
        })
        
        column_info = get_column_info(df)
        
        assert len(column_info) == 3
        assert 'numeric_col' in column_info
        assert 'text_col' in column_info
        assert 'null_col' in column_info
        
        # Check numeric column info
        numeric_info = column_info['numeric_col']
        assert numeric_info['non_null_count'] == 5
        assert numeric_info['null_count'] == 0
        assert numeric_info['min'] == 1
        assert numeric_info['max'] == 5
        
        # Check text column info
        text_info = column_info['text_col']
        assert 'sample_values' in text_info
        assert len(text_info['sample_values']) == 5
        
        # Check null column info
        null_info = column_info['null_col']
        assert null_info['non_null_count'] == 3
        assert null_info['null_count'] == 2
    
    def test_format_memory_usage(self):
        """Test memory usage formatting."""
        assert format_memory_usage(512) == "512.0 B"
        assert format_memory_usage(1024) == "1.0 KB"
        assert format_memory_usage(1024 * 1024) == "1.0 MB"
        assert format_memory_usage(1024 * 1024 * 1024) == "1.0 GB"

class TestIntegration:
    """Integration tests for the complete dtale application."""
    
    def setup_method(self):
        """Set up test fixtures."""
        if not IMPORTS_AVAILABLE:
            pytest.skip(f"Required imports not available: {IMPORT_ERROR}")
    
    def test_full_workflow(self):
        """Test the complete workflow from data loading to view creation."""
        # Create sample data
        np.random.seed(42)
        sample_data = pd.DataFrame({
            'Security_1': [f'BOND_{i:04d}' for i in range(100)],
            'Security_2': [f'BOND_{i+100:04d}' for i in range(100)],
            'Z_Score': np.random.normal(0, 1, 100),
            'Currency_1': np.random.choice(['CAD', 'USD'], 100),
            'Currency_2': np.random.choice(['CAD', 'USD'], 100),
            'Custom_Sector_1': np.random.choice(['Financial', 'Energy'], 100),
            'Custom_Sector_2': np.random.choice(['Financial', 'Energy'], 100),
            'Own?': np.random.choice([0, 1], 100),
        })
        
        # Save to temporary file
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / 'integration_test.parquet'
        sample_data.to_parquet(temp_file, index=False)
        
        try:
            # Initialize app
            app = BondDtaleApp(data_path=str(temp_file), sample_size=50)
            
            # Load data
            success = app.load_data()
            assert success is True
            
            # Test filtering
            cad_data = app._filter_cad_only(app.df_sample)
            portfolio_data = app._filter_portfolio(app.df_sample)
            
            assert len(cad_data) <= len(app.df_sample)
            assert len(portfolio_data) <= len(app.df_sample)
            
            # Verify data integrity
            assert set(app.df_sample.columns) == set(sample_data.columns)
            
        finally:
            if temp_file.exists():
                temp_file.unlink()

def run_basic_tests():
    """Run basic tests that don't require dtale to be installed."""
    print("🧪 Running Basic dtale App Tests")
    print("=" * 50)
    
    try:
        # Test data validation
        is_valid, message = validate_data_file('nonexistent_file.csv')
        assert is_valid is False
        print("✅ File validation test passed")
        
        # Test memory formatting
        assert format_memory_usage(1024) == "1.0 KB"
        print("✅ Memory formatting test passed")
        
        # Test column info
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        column_info = get_column_info(df)
        assert len(column_info) == 2
        print("✅ Column info test passed")
        
        print("\n🎉 All basic tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    # Run basic tests if dtale is not available
    if not IMPORTS_AVAILABLE:
        print(f"⚠️  dtale not available: {IMPORT_ERROR}")
        print("Running basic tests only...")
        success = run_basic_tests()
        exit(0 if success else 1)
    else:
        # Run full test suite
        pytest.main([__file__, "-v"]) 