import requests
import json
import time
import pandas as pd
from pathlib import Path

class BasicDashAppTester:
    """
    Basic testing for Dash app without browser automation
    Tests connectivity, data availability, and basic app health
    """
    
    def __init__(self, app_url="http://localhost:8056"):
        self.app_url = app_url
        self.results = {}
    
    def test_app_connectivity(self):
        """Test if the app is running and responsive"""
        print("🔗 Testing app connectivity...")
        try:
            response = requests.get(self.app_url, timeout=10)
            if response.status_code == 200:
                print("✅ App is responding (HTTP 200)")
                self.results["connectivity"] = True
                
                # Check if response contains expected content
                if "Z Analytics Dashboard" in response.text:
                    print("✅ App title found in response")
                    self.results["title_check"] = True
                else:
                    print("⚠️ App title not found in HTML")
                    self.results["title_check"] = False
                    
                return True
            else:
                print(f"❌ App returned status: {response.status_code}")
                self.results["connectivity"] = False
                return False
        except requests.exceptions.ConnectionError:
            print("❌ Cannot connect to app - is it running?")
            self.results["connectivity"] = False
            return False
        except Exception as e:
            print(f"❌ Error testing app: {e}")
            self.results["connectivity"] = False
            return False
    
    def test_data_file_exists(self):
        """Test if the required data file exists"""
        print("📁 Testing data file availability...")
        parquet_path = Path('historical g spread/bond_z.parquet')
        
        if parquet_path.exists():
            print("✅ Data file exists")
            try:
                df = pd.read_parquet(parquet_path)
                print(f"✅ Data loaded: {len(df)} rows × {len(df.columns)} columns")
                print(f"📊 Columns: {list(df.columns[:5])}..." if len(df.columns) > 5 else f"📊 Columns: {list(df.columns)}")
                self.results["data_file"] = True
                self.results["data_stats"] = {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": list(df.columns)
                }
                return True
            except Exception as e:
                print(f"❌ Error reading data file: {e}")
                self.results["data_file"] = False
                return False
        else:
            print("❌ Data file not found")
            self.results["data_file"] = False
            return False
    
    def test_app_performance(self):
        """Test app response time"""
        print("⏱️ Testing app performance...")
        try:
            start_time = time.time()
            response = requests.get(self.app_url, timeout=10)
            end_time = time.time()
            
            response_time = end_time - start_time
            print(f"📊 Response time: {response_time:.2f} seconds")
            
            if response_time < 3:
                print("✅ Good response time")
                self.results["performance"] = "good"
            elif response_time < 8:
                print("⚠️ Acceptable response time")
                self.results["performance"] = "acceptable"
            else:
                print("❌ Slow response time")
                self.results["performance"] = "slow"
            
            self.results["response_time"] = response_time
            return True
            
        except Exception as e:
            print(f"❌ Performance test failed: {e}")
            self.results["performance"] = "failed"
            return False
    
    def test_app_dependencies(self):
        """Test if required Python packages are available"""
        print("📦 Testing app dependencies...")
        required_packages = [
            'dash', 'dash_ag_grid', 'pandas', 
            'dash_bootstrap_components', 'pathlib'
        ]
        
        missing_packages = []
        for package in required_packages:
            try:
                __import__(package)
                print(f"✅ {package} available")
            except ImportError:
                print(f"❌ {package} missing")
                missing_packages.append(package)
        
        if not missing_packages:
            print("✅ All required packages available")
            self.results["dependencies"] = "complete"
            return True
        else:
            print(f"❌ Missing packages: {missing_packages}")
            self.results["dependencies"] = "incomplete"
            self.results["missing_packages"] = missing_packages
            return False
    
    def test_config_files(self):
        """Test if configuration files exist"""
        print("⚙️ Testing configuration files...")
        
        config_files = [
            'config/config.yaml',
            'pyproject.toml',
        ]
        
        file_status = {}
        for config_file in config_files:
            if Path(config_file).exists():
                print(f"✅ {config_file} exists")
                file_status[config_file] = True
            else:
                print(f"⚠️ {config_file} not found")
                file_status[config_file] = False
        
        self.results["config_files"] = file_status
        return True
    
    def run_basic_tests(self):
        """Run all basic tests"""
        print("🧪 Starting Basic Dash App Tests")
        print("=" * 50)
        
        test_results = {}
        
        # Test 1: Dependencies
        test_results["dependencies"] = self.test_app_dependencies()
        
        # Test 2: Data file
        test_results["data_file"] = self.test_data_file_exists()
        
        # Test 3: Config files
        test_results["config_files"] = self.test_config_files()
        
        # Test 4: Connectivity (only if basics pass)
        if test_results["dependencies"] and test_results["data_file"]:
            test_results["connectivity"] = self.test_app_connectivity()
            
            # Test 5: Performance (only if app is running)
            if test_results["connectivity"]:
                test_results["performance"] = self.test_app_performance()
        else:
            print("⚠️ Skipping connectivity tests due to missing dependencies/data")
        
        # Generate summary report
        self.generate_report(test_results)
        return test_results
    
    def generate_report(self, test_results):
        """Generate a detailed test report"""
        print("\n" + "=" * 50)
        print("📊 BASIC TEST RESULTS SUMMARY")
        print("=" * 50)
        
        total_tests = len(test_results)
        passed_tests = sum(test_results.values())
        
        for test_name, passed in test_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{test_name.replace('_', ' ').title()}: {status}")
        
        print(f"\nOverall: {passed_tests}/{total_tests} basic tests passed")
        
        # Detailed results
        if hasattr(self, 'results') and self.results:
            print("\n" + "🔍 DETAILED RESULTS:")
            print("-" * 30)
            
            if "data_stats" in self.results:
                stats = self.results["data_stats"]
                print(f"Data: {stats['rows']:,} rows × {stats['columns']} columns")
            
            if "response_time" in self.results:
                print(f"Response time: {self.results['response_time']:.2f}s")
            
            if "missing_packages" in self.results:
                print(f"Missing packages: {self.results['missing_packages']}")
        
        # Recommendations
        print("\n" + "💡 RECOMMENDATIONS:")
        print("-" * 30)
        
        if not test_results.get("dependencies", True):
            print("• Install missing Python packages")
        
        if not test_results.get("data_file", True):
            print("• Ensure bond_z.parquet file exists in 'historical g spread/' folder")
        
        if not test_results.get("connectivity", True):
            print("• Start the Dash app: python app_column_formatting_fixed.py")
        
        if test_results.get("performance", "") == "slow":
            print("• App performance is slow - check data size and system resources")
        
        print("\n🎯 Next Steps:")
        print("• For full UI testing, install Selenium: pip install selenium")
        print("• Run comprehensive tests: python test/test_dash_app_integration.py")
        
        return self.results

def run_basic_dash_tests():
    """Main function to run basic tests"""
    tester = BasicDashAppTester()
    return tester.run_basic_tests()

if __name__ == "__main__":
    # Run basic tests when script is executed directly
    run_basic_dash_tests() 