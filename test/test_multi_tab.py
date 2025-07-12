#!/usr/bin/env python3
"""
Simple test script for the multi-tab dtale application
"""

import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_imports():
    """Test if all imports work correctly."""
    print("🧪 Testing imports...")
    
    try:
        from src.analytics.bond_analytics import BondAnalytics as BondDtaleApp
        print("✅ BondDtaleApp import successful")
    except Exception as e:
        print(f"❌ BondDtaleApp import failed: {e}")
        return False
    
    try:
        from src.analytics.dtale_dashboard import MultiTabDtaleApp
        print("✅ MultiTabDtaleApp import successful")
    except Exception as e:
        print(f"❌ MultiTabDtaleApp import failed: {e}")
        return False
    
    return True

def test_data_loading():
    """Test data loading."""
    print("\n🧪 Testing data loading...")
    
    try:
        from src.analytics.bond_analytics import BondAnalytics as BondDtaleApp
        
        # Check if data file exists
        data_path = Path('historical g spread/bond_z.parquet')
        if not data_path.exists():
            print(f"❌ Data file not found: {data_path}")
            return False
        
        print(f"✅ Data file found: {data_path}")
        
        # Test basic app initialization
        app = BondDtaleApp(data_path=str(data_path), sample_size=1000)
        print("✅ BondDtaleApp initialized")
        
        # Test data loading
        success = app.load_data()
        if success:
            print(f"✅ Data loaded: {app.stats['total_rows']:,} rows")
            return True
        else:
            print("❌ Data loading failed")
            return False
            
    except Exception as e:
        print(f"❌ Data loading test failed: {e}")
        return False

def test_view_creation():
    """Test view creation."""
    print("\n🧪 Testing view creation...")
    
    try:
        from src.analytics.bond_analytics import BondAnalytics as BondDtaleApp
        
        app = BondDtaleApp(data_path='historical g spread/bond_z.parquet', sample_size=500)
        
        if not app.load_data():
            print("❌ Could not load data for view test")
            return False
        
        # Test creating a simple view
        print("Creating CAD-only view...")
        instance = app.create_view('cad-only')
        
        if instance:
            print(f"✅ View created successfully: {instance._url}")
            
            # Clean up
            try:
                instance.kill()
                print("✅ View cleaned up")
            except:
                pass
            
            return True
        else:
            print("❌ View creation failed")
            return False
            
    except Exception as e:
        print(f"❌ View creation test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Testing Multi-Tab dtale Application")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_data_loading,
        test_view_creation
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS:")
    
    test_names = ["Imports", "Data Loading", "View Creation"]
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {status} {name}")
    
    passed = sum(results)
    total = len(results)
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Multi-tab app should work.")
        print("\nTo launch the multi-tab app:")
        print("poetry run python dtale_multi_tab_app.py --sample-size 10000")
    else:
        print("⚠️  Some tests failed. Check the errors above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 