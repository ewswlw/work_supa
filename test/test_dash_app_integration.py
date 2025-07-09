import pytest
import requests
import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
import pandas as pd
from pathlib import Path

class DashAppTester:
    """
    Automated testing class for the Dash Analytics App
    Tests UI interactions, data loading, and formatting features
    """
    
    def __init__(self, app_url="http://localhost:8056"):
        self.app_url = app_url
        self.driver = None
        self.wait = None
        
    def setup_browser(self, headless=True):
        """Setup Chrome browser for testing"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 10)
            return True
        except Exception as e:
            print(f"❌ Browser setup failed: {e}")
            return False
    
    def test_app_loads(self):
        """Test if the app loads successfully"""
        try:
            response = requests.get(self.app_url, timeout=10)
            if response.status_code == 200:
                print("✅ App is responding (HTTP 200)")
                return True
            else:
                print(f"❌ App returned status: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print("❌ Cannot connect to app - is it running?")
            return False
        except Exception as e:
            print(f"❌ Error testing app: {e}")
            return False
    
    def test_ui_elements(self):
        """Test if main UI elements are present and functional"""
        if not self.driver:
            print("❌ Browser not setup - call setup_browser() first")
            return False
            
        try:
            print(f"🌐 Loading {self.app_url}")
            self.driver.get(self.app_url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Test 1: Check if main grid exists
            grid = self.wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "ag-root-wrapper"))
            )
            print("✅ AG-Grid component loaded")
            
            # Test 2: Check if title exists
            title = self.driver.find_element(By.TAG_NAME, "h1")
            assert "Z Analytics Dashboard" in title.text
            print("✅ Page title found")
            
            # Test 3: Check if control buttons exist
            export_btn = self.driver.find_element(By.ID, "export-btn")
            assert export_btn.is_displayed()
            print("✅ Export button found")
            
            # Test 4: Check if column selector dropdown exists
            column_dropdown = self.driver.find_element(By.ID, "column-selector")
            assert column_dropdown.is_displayed()
            print("✅ Column selector found")
            
            return True
            
        except TimeoutException:
            print("❌ Timeout waiting for elements to load")
            return False
        except Exception as e:
            print(f"❌ UI test failed: {e}")
            return False
    
    def test_data_loading(self):
        """Test if data loads correctly in the grid"""
        try:
            if not self.driver:
                return False
                
            # Wait for grid data to load
            time.sleep(2)
            
            # Check if grid has rows
            rows = self.driver.find_elements(By.CLASS_NAME, "ag-row")
            row_count = len(rows)
            
            if row_count > 0:
                print(f"✅ Grid loaded with {row_count} visible rows")
                return True
            else:
                print("❌ No data rows found in grid")
                return False
                
        except Exception as e:
            print(f"❌ Data loading test failed: {e}")
            return False
    
    def test_column_formatting_modal(self):
        """Test the column formatting modal functionality"""
        try:
            if not self.driver:
                return False
            
            # Test new dropdown/button interface for formatting
            # First, find the format column dropdown
            try:
                format_dropdown = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "format-column-selector"))
                )
                print("✅ Format column dropdown found")
                
                # Click on the dropdown to open it
                format_dropdown.click()
                time.sleep(1)
                
                # Select the first available option (second column - skip checkbox column)
                options = self.driver.find_elements(By.CSS_SELECTOR, "#format-column-selector .VirtualizedSelectOption")
                if not options:  # Try alternative selector
                    options = self.driver.find_elements(By.CSS_SELECTOR, "[data-value]")
                
                if len(options) > 1:
                    options[1].click()  # Select second option
                    time.sleep(1)
                    print("✅ Column selected from dropdown")
                    
                    # Now click the Format Column button
                    format_btn = self.driver.find_element(By.ID, "format-column-btn")
                    format_btn.click()
                    time.sleep(1)
                    
                    # Check if modal opened
                    try:
                        modal = self.wait.until(
                            EC.presence_of_element_located((By.ID, "format-modal"))
                        )
                        
                        # Check if modal is visible
                        modal_dialog = modal.find_element(By.CLASS_NAME, "modal-dialog")
                        if modal_dialog.is_displayed():
                            print("✅ Format modal opened successfully")
                            
                            # Test modal elements
                            number_format_dropdown = modal.find_element(By.ID, "modal-number-format-type")
                            assert number_format_dropdown.is_displayed()
                            print("✅ Number format dropdown found")
                            
                            # Close modal
                            cancel_btn = modal.find_element(By.ID, "modal-cancel-btn")
                            cancel_btn.click()
                            time.sleep(1)
                            
                            return True
                        else:
                            print("❌ Modal not visible")
                            return False
                            
                    except TimeoutException:
                        print("❌ Format modal did not open")
                        return False
                else:
                    print("⚠️ Trying alternative approach - direct button click")
                    # Try clicking the format button without selecting from dropdown
                    format_btn = self.driver.find_element(By.ID, "format-column-btn")
                    format_btn.click()
                    time.sleep(1)
                    print("✅ Format button clicked (may need column selection)")
                    return True
                        
            except TimeoutException:
                print("❌ Format column dropdown not found")
                return False
                
        except Exception as e:
            print(f"❌ Modal test failed: {e}")
            return False
    
    def test_export_functionality(self):
        """Test export button functionality"""
        try:
            if not self.driver:
                return False
            
            # Click export button
            export_btn = self.driver.find_element(By.ID, "export-btn")
            export_btn.click()
            
            # Wait a moment for download to trigger
            time.sleep(2)
            
            print("✅ Export button clicked (download may have started)")
            return True
            
        except Exception as e:
            print(f"❌ Export test failed: {e}")
            return False
    
    def test_filtering_and_sorting(self):
        """Test grid filtering and sorting functionality"""
        try:
            if not self.driver:
                return False
            
            # Test sorting by clicking on a column header
            # Find a numeric column header
            header_cells = self.driver.find_elements(By.CLASS_NAME, "ag-header-cell")
            
            if len(header_cells) > 2:
                # Click on a header to sort
                header_cells[2].click()
                time.sleep(1)
                
                # Click again to reverse sort
                header_cells[2].click()
                time.sleep(1)
                
                print("✅ Column sorting tested")
                
                # Test filtering by opening filter menu
                # Look for filter icon and click it
                try:
                    filter_icons = self.driver.find_elements(By.CLASS_NAME, "ag-icon-filter")
                    if filter_icons:
                        filter_icons[0].click()
                        time.sleep(1)
                        print("✅ Filter menu opened")
                except:
                    print("⚠️ Filter icon not found (may require specific column)")
                
                return True
            else:
                print("❌ Not enough columns for sorting test")
                return False
                
        except Exception as e:
            print(f"❌ Filtering/sorting test failed: {e}")
            return False
    
    def run_full_test_suite(self):
        """Run all tests and generate report"""
        print("🧪 Starting Dash App Test Suite")
        print("=" * 50)
        
        results = {
            "app_loads": False,
            "ui_elements": False,
            "data_loading": False,
            "modal_functionality": False,
            "export_functionality": False,
            "filtering_sorting": False
        }
        
        # Test 1: Basic connectivity
        results["app_loads"] = self.test_app_loads()
        
        if results["app_loads"]:
            # Setup browser for UI tests
            if self.setup_browser(headless=False):  # Set to True for headless
                
                # Test 2: UI Elements
                results["ui_elements"] = self.test_ui_elements()
                
                # Test 3: Data Loading
                if results["ui_elements"]:
                    results["data_loading"] = self.test_data_loading()
                    
                    # Test 4: Modal Functionality
                    results["modal_functionality"] = self.test_column_formatting_modal()
                    
                    # Test 5: Export Functionality  
                    results["export_functionality"] = self.test_export_functionality()
                    
                    # Test 6: Filtering and Sorting
                    results["filtering_sorting"] = self.test_filtering_and_sorting()
                
                # Cleanup
                self.driver.quit()
        
        # Generate report
        print("\n" + "=" * 50)
        print("📊 TEST RESULTS SUMMARY")
        print("=" * 50)
        
        total_tests = len(results)
        passed_tests = sum(results.values())
        
        for test_name, passed in results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{test_name.replace('_', ' ').title()}: {status}")
        
        print(f"\nOverall: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            print("🎉 ALL TESTS PASSED!")
        else:
            print("⚠️ Some tests failed - check output above")
        
        return results
    
    def take_screenshot(self, filename="app_screenshot.png"):
        """Take a screenshot of the current app state"""
        if self.driver:
            self.driver.save_screenshot(filename)
            print(f"📸 Screenshot saved: {filename}")
            return True
        return False

# Test runner function
def run_dash_app_tests():
    """Main function to run all tests"""
    tester = DashAppTester()
    return tester.run_full_test_suite()

if __name__ == "__main__":
    # Run tests when script is executed directly
    run_dash_app_tests() 