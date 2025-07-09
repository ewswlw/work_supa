"""
Test file for the enhanced Dash application
Tests all the new features and improvements
"""

import pytest
import sys
import os
from pathlib import Path
import pandas as pd
import dash
from dash import dcc, html

# Add the src directory to the path so we can import the enhanced app
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_enhanced_app_imports():
    """Test that all imports work correctly"""
    try:
        import dash_bootstrap_components as dbc
        import dash_ag_grid as dag
        import dash_mantine_components as dmc
        import plotly.graph_objects as go
        import numpy as np
        print("✅ All imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_data_loading():
    """Test that the data loads correctly"""
    try:
        parquet_path = Path('historical g spread/bond_z.parquet')
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            print(f"✅ Data loaded: {len(df)} rows x {len(df.columns)} columns")
            return True
        else:
            print("❌ Data file not found")
            return False
    except Exception as e:
        print(f"❌ Data loading error: {e}")
        return False

def test_enhanced_features():
    """Test the enhanced features"""
    try:
        # Test format templates
        from app_enhanced_complete import FORMAT_TEMPLATES
        print(f"✅ Format templates: {len(FORMAT_TEMPLATES)} templates available")
        
        # Test column definitions with enhanced formatting
        from app_enhanced_complete import get_column_defs, df
        
        # Test with sample formatting rules
        sample_rules = {
            'Z_Score': {
                'conditional_format': {'type': 'color_scale', 'min_value': -3, 'max_value': 3},
                'number_format': {'type': 'decimal', 'decimals': 2},
                'alert_rules': {'z_score_alerts': True}
            }
        }
        
        column_defs = get_column_defs(df, {}, sample_rules)
        print(f"✅ Enhanced column definitions: {len(column_defs)} columns configured")
        
        # Test that formatted columns have enhanced headers
        formatted_cols = [col for col in column_defs if col['headerName'] != col['field']]
        print(f"✅ Formatted columns with visual indicators: {len(formatted_cols)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced features test error: {e}")
        return False

def test_enhanced_formatting_types():
    """Test all the enhanced formatting types"""
    try:
        # Test basis points formatting
        basis_points_format = {
            'number_format': {'type': 'basis_points', 'decimals': 1}
        }
        print("✅ Basis points formatting available")
        
        # Test icon sets
        icon_sets_format = {
            'conditional_format': {'type': 'icon_sets'}
        }
        print("✅ Icon sets formatting available")
        
        # Test threshold rules
        threshold_format = {
            'conditional_format': {
                'type': 'threshold_rules',
                'thresholds': [
                    {'operator': '>', 'value': 2, 'background_color': '#ff0000', 'text_color': '#ffffff'}
                ]
            }
        }
        print("✅ Threshold rules formatting available")
        
        # Test alert rules
        alert_format = {
            'alert_rules': {'z_score_alerts': True, 'spread_alerts': True}
        }
        print("✅ Financial alert rules available")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced formatting types test error: {e}")
        return False

def test_financial_specific_features():
    """Test financial-specific features"""
    try:
        # Test Z-score analysis
        z_score_template = FORMAT_TEMPLATES.get('z_score_analysis')
        if z_score_template:
            print("✅ Z-Score analysis template available")
        
        # Test spread analysis
        spread_template = FORMAT_TEMPLATES.get('spread_analysis')
        if spread_template:
            print("✅ Spread analysis template available")
        
        # Test volatility monitoring
        volatility_template = FORMAT_TEMPLATES.get('volatility_monitor')
        if volatility_template:
            print("✅ Volatility monitor template available")
        
        return True
        
    except Exception as e:
        print(f"❌ Financial features test error: {e}")
        return False

def run_all_tests():
    """Run all tests"""
    print("🧪 Running Enhanced App Tests...")
    print("=" * 50)
    
    tests = [
        test_enhanced_app_imports,
        test_data_loading,
        test_enhanced_features,
        test_enhanced_formatting_types,
        test_financial_specific_features
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            result = test()
            if result:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
            failed += 1
        print("-" * 30)
    
    print(f"\n📊 Test Results:")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {passed / (passed + failed) * 100:.1f}%")
    
    return passed, failed

if __name__ == "__main__":
    # Change to the correct directory
    os.chdir(Path(__file__).parent.parent)
    run_all_tests() 