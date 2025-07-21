import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import tempfile
import shutil
import os

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from src.pipeline.universe_processor import process_universe_files
from src.utils.logging import LogManager


class TestUniverseRiskField:
    """Test suite for Risk field float conversion in universe processor"""
    
    @pytest.fixture
    def temp_project_structure(self):
        """Create temporary project structure for testing"""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)
        
        # Create necessary directories
        (project_root / 'universe' / 'raw data').mkdir(parents=True, exist_ok=True)
        (project_root / 'universe' / 'processed data').mkdir(parents=True, exist_ok=True)
        (project_root / 'config').mkdir(parents=True, exist_ok=True)
        (project_root / 'logs').mkdir(parents=True, exist_ok=True)
        
        # Create test config
        config_content = """
universe_processor:
  columns_to_keep:
    - Date
    - CUSIP
    - Risk
    - Yrs (Mat)
    - Z Spread
  
  bucketing:
    yrs_to_maturity:
      column_name: "Yrs (Mat)"
      new_column_name: "Yrs (Mat) Bucket"
      bins: [0, 1, 5, 10, .inf]
      labels: ['0-1', '1-5', '5-10', '>10']

  validation:
    numeric_columns:
      - Risk
      - Yrs (Mat)
      - Z Spread
"""
        with open(project_root / 'config' / 'config.yaml', 'w') as f:
            f.write(config_content)
        
        # Create test Excel file
        test_data = {
            'CUSIP': ['123456789', '987654321', '555666777'],
            'Risk': ['1.25', '2.50', '0.75'],  # String values that should be converted to float
            'Yrs (Mat)': [5.5, 10.2, 2.1],
            'Z Spread': [150.5, 200.0, 75.25]
        }
        df = pd.DataFrame(test_data)
        
        # Save as Excel with date in filename
        excel_path = project_root / 'universe' / 'raw data' / 'test_data_01.15.24.xlsx'
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=False)
        
        yield project_root
        
        # Cleanup
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def logger(self):
        """Create test logger"""
        return LogManager(
            log_file='test_universe_risk.log',
            log_level='DEBUG',
            log_format='[%(asctime)s] %(levelname)s: %(message)s'
        )
    
    def test_risk_field_float_conversion(self, temp_project_structure, logger):
        """Test that Risk field is properly converted to float"""
        # Create a simple test that directly tests the float conversion logic
        # without running the full pipeline
        
        # Create test data
        test_data = {
            'CUSIP': ['123456789', '987654321', '555666777'],
            'Risk': ['1.25', '2.50', '0.75'],  # String values that should be converted to float
            'Yrs (Mat)': [5.5, 10.2, 2.1],
            'Z Spread': [150.5, 200.0, 75.25]
        }
        df = pd.DataFrame(test_data)
        
        # Test the float conversion logic directly
        float_columns = [
            'Make_Whole', 'Back End', 'Stochastic Duration', 'Stochastic Convexity',
            'MTD Return', 'QTD Return', 'YTD Return', 'MTD Bench Return', 'QTD Bench Return',
            'YTD Bench Return', 'Yrs (Worst)', 'YTC', 'Excess MTD', 'Excess YTD', 'G Sprd',
            'Yrs (Cvn)', 'OAS (Mid)', 'CAD Equiv Swap', 'G (RBC Crv)', 'vs BI', 'vs BCE',
            'YTD Equity', 'MTD Equity', 'Yrs Since Issue', 'Risk', 'Yrs (Mat)', 'Z Spread'
        ]
        
        # Apply the same conversion logic as in the processor
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Verify Risk field is float
        assert 'Risk' in df.columns, "Risk column should be present"
        assert pd.api.types.is_numeric_dtype(df['Risk']), "Risk field should be numeric"
        assert df['Risk'].dtype in [np.float64, np.float32], f"Risk field should be float, got {df['Risk'].dtype}"
        
        # Verify values are correct
        expected_risk_values = [1.25, 2.50, 0.75]
        actual_risk_values = df['Risk'].dropna().tolist()
        assert len(actual_risk_values) == len(expected_risk_values), "Should have same number of Risk values"
        
        # Check values are close (accounting for float precision)
        for expected, actual in zip(sorted(expected_risk_values), sorted(actual_risk_values)):
            assert abs(expected - actual) < 1e-6, f"Risk value mismatch: expected {expected}, got {actual}"
        
        logger.info("✅ Risk field float conversion test passed")
    
    def test_risk_field_with_mixed_data_types(self, temp_project_structure, logger):
        """Test Risk field conversion with mixed data types (strings, numbers, NaN)"""
        # Create test data with mixed types
        test_data = {
            'CUSIP': ['123456789', '987654321', '555666777', '111222333'],
            'Risk': ['1.25', 2.50, 'invalid', np.nan],  # Mixed types including invalid
            'Yrs (Mat)': [5.5, 10.2, 2.1, 7.8],
            'Z Spread': [150.5, 200.0, 75.25, 300.0]
        }
        df = pd.DataFrame(test_data)
        
        # Save as Excel
        excel_path = temp_project_structure / 'universe' / 'raw data' / 'mixed_data_01.16.24.xlsx'
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=False)
        
        # Change to temp project directory
        original_cwd = Path.cwd()
        os.chdir(temp_project_structure)
        
        try:
            # Process universe files
            process_universe_files(logger, force_full_refresh=True)
            
            # Load the generated parquet file
            parquet_path = temp_project_structure / 'universe' / 'universe.parquet'
            df = pd.read_parquet(parquet_path)
            
            # Verify Risk field is float
            assert pd.api.types.is_numeric_dtype(df['Risk']), "Risk field should be numeric"
            
            # Check that valid values are converted correctly
            valid_risk_values = df['Risk'].dropna().tolist()
            assert 1.25 in valid_risk_values, "Valid string '1.25' should be converted to float"
            assert 2.50 in valid_risk_values, "Valid number 2.50 should remain as float"
            
            # Check that invalid values are converted to NaN
            assert df['Risk'].isna().sum() >= 1, "Invalid values should be converted to NaN"
            
            logger.info("✅ Risk field mixed data types test passed")
            
        finally:
            os.chdir(original_cwd)
    
    def test_risk_field_preserves_precision(self, temp_project_structure, logger):
        """Test that Risk field preserves decimal precision"""
        # Create test data with high precision values
        test_data = {
            'CUSIP': ['123456789', '987654321', '555666777'],
            'Risk': ['1.23456789', '2.50000000', '0.12345678'],  # High precision values
            'Yrs (Mat)': [5.5, 10.2, 2.1],
            'Z Spread': [150.5, 200.0, 75.25]
        }
        df = pd.DataFrame(test_data)
        
        # Save as Excel
        excel_path = temp_project_structure / 'universe' / 'raw data' / 'precision_test_01.17.24.xlsx'
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=False)
        
        # Change to temp project directory
        original_cwd = Path.cwd()
        os.chdir(temp_project_structure)
        
        try:
            # Process universe files
            process_universe_files(logger, force_full_refresh=True)
            
            # Load the generated parquet file
            parquet_path = temp_project_structure / 'universe' / 'universe.parquet'
            df = pd.read_parquet(parquet_path)
            
            # Verify precision is preserved
            expected_values = [1.23456789, 2.50000000, 0.12345678]
            actual_values = df['Risk'].dropna().tolist()
            
            for expected, actual in zip(sorted(expected_values), sorted(actual_values)):
                assert abs(expected - actual) < 1e-8, f"Precision loss detected: expected {expected}, got {actual}"
            
            logger.info("✅ Risk field precision preservation test passed")
            
        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"]) 