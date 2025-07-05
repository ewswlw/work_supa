#!/usr/bin/env python3
"""
Test script to verify the Bond Z-Score Dashboard functionality
"""

import pandas as pd
import requests
import time
import subprocess
import sys
import os
from concurrent.futures import ThreadPoolExecutor
import signal

def test_data_loading():
    """Test if the data loads correctly"""
    print("🔍 Testing data loading...")
    try:
        df = pd.read_parquet('historical g spread/bond_z.parquet')
        print(f"✅ Data loaded successfully: {len(df):,} rows, {len(df.columns)} columns")
        
        # Test key columns exist
        required_columns = ['Security_1', 'Security_2', 'Z_Score', 'Last_Spread', 'Custom_Sector_1']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return False
        
        print("✅ All required columns present")
        
        # Test data quality
        print(f"✅ Z-Score range: {df['Z_Score'].min():.2f} to {df['Z_Score'].max():.2f}")
        print(f"✅ Unique sectors: {df['Custom_Sector_1'].nunique()}")
        print(f"✅ Unique ratings: {df['Rating_1'].nunique()}")
        return True
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return False

def test_dashboard_response():
    """Test if the dashboard responds correctly"""
    print("\n🌐 Testing dashboard response...")
    try:
        # Start dashboard in background
        process = subprocess.Popen([sys.executable, 'bond_dashboard.py'], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE)
        
        # Wait for startup
        time.sleep(5)
        
        # Test if dashboard is responding
        response = requests.get('http://localhost:8050', timeout=10)
        if response.status_code == 200:
            print("✅ Dashboard responding correctly (HTTP 200)")
            print(f"✅ Response size: {len(response.content)} bytes")
            
            # Check if it contains expected content
            if 'dash' in response.text.lower():
                print("✅ Dashboard content loads properly")
            else:
                print("⚠️  Dashboard content may not be fully loaded")
                
        else:
            print(f"❌ Dashboard not responding correctly (HTTP {response.status_code})")
            return False
            
        # Clean up
        process.terminate()
        process.wait(timeout=5)
        return True
        
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to dashboard")
        return False
    except Exception as e:
        print(f"❌ Error testing dashboard: {e}")
        return False
    finally:
        # Make sure process is terminated
        try:
            process.terminate()
            process.wait(timeout=2)
        except:
            try:
                process.kill()
            except:
                pass

def test_imports():
    """Test if all required packages are available"""
    print("\n📦 Testing imports...")
    try:
        import pandas as pd
        import dash
        import dash_bootstrap_components as dbc
        import plotly.express as px
        import plotly.graph_objects as go
        from dash import dcc, html, dash_table
        
        print("✅ All required packages imported successfully")
        print(f"✅ Pandas version: {pd.__version__}")
        print(f"✅ Dash version: {dash.__version__}")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Running Bond Z-Score Dashboard Tests")
    print("=" * 50)
    
    tests = [
        ("Package Imports", test_imports),
        ("Data Loading", test_data_loading),
        ("Dashboard Response", test_dashboard_response)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 50)
    print("📋 Test Results Summary")
    print("=" * 50)
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if not result:
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All tests passed! Dashboard is working perfectly!")
        print("🚀 Run 'python3 bond_dashboard.py' to start the dashboard")
        print("🌐 Then open http://localhost:8050 in your browser")
    else:
        print("⚠️  Some tests failed. Please check the issues above.")
    
    return all_passed

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)