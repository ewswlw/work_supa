"""
Professional Dash App Testing using Microsoft Playwright MCP Server
This uses the MCP protocol to communicate with the Playwright server for browser automation
"""

import asyncio
import json
import time
import aiohttp
import websockets
from pathlib import Path
import requests
from datetime import datetime

class PlaywrightMCPTester:
    """
    Advanced Dash app tester using Microsoft Playwright MCP Server
    Provides professional browser automation and comprehensive testing
    """
    
    def __init__(self, app_url="http://localhost:8056", mcp_port=3001):
        self.app_url = app_url
        self.mcp_port = mcp_port
        self.mcp_url = f"http://localhost:{mcp_port}"
        self.session = None
        self.page_id = None
        self.test_results = {}
        
    async def connect_to_mcp_server(self):
        """Connect to the Playwright MCP server"""
        try:
            # Test if MCP server is running
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.mcp_url}/health") as response:
                    if response.status == 200:
                        print("✅ MCP Server is running")
                        self.session = session
                        return True
        except Exception as e:
            print(f"❌ MCP Server not responding: {e}")
            print("💡 Make sure to start it with: npx @playwright/mcp --port 3001")
            return False
    
    async def create_browser_page(self):
        """Create a new browser page through MCP"""
        try:
            if not self.session:
                async with aiohttp.ClientSession() as session:
                    self.session = session
                    return await self._create_page()
            else:
                return await self._create_page()
        except Exception as e:
            print(f"❌ Failed to create browser page: {e}")
            return False
    
    async def _create_page(self):
        """Internal method to create page"""
        try:
            # Send MCP command to create new page
            payload = {
                "action": "new_page",
                "options": {
                    "viewport": {"width": 1920, "height": 1080},
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            }
            
            async with self.session.post(f"{self.mcp_url}/api/page", json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    self.page_id = result.get("page_id")
                    print(f"✅ Browser page created: {self.page_id}")
                    return True
                else:
                    print(f"❌ Failed to create page: {response.status}")
                    return False
        except Exception as e:
            print(f"❌ Page creation error: {e}")
            return False
    
    async def navigate_to_app(self):
        """Navigate to the Dash app"""
        try:
            payload = {
                "action": "navigate",
                "page_id": self.page_id,
                "url": self.app_url
            }
            
            async with self.session.post(f"{self.mcp_url}/api/navigate", json=payload) as response:
                if response.status == 200:
                    print(f"✅ Navigated to {self.app_url}")
                    await asyncio.sleep(3)  # Wait for page load
                    return True
                else:
                    print(f"❌ Navigation failed: {response.status}")
                    return False
        except Exception as e:
            print(f"❌ Navigation error: {e}")
            return False
    
    async def take_screenshot(self, filename="screenshot.png"):
        """Take a screenshot of the current page"""
        try:
            payload = {
                "action": "screenshot",
                "page_id": self.page_id,
                "path": f"./test/playwright-output/{filename}"
            }
            
            async with self.session.post(f"{self.mcp_url}/api/screenshot", json=payload) as response:
                if response.status == 200:
                    print(f"📸 Screenshot saved: {filename}")
                    return True
                else:
                    print(f"❌ Screenshot failed: {response.status}")
                    return False
        except Exception as e:
            print(f"❌ Screenshot error: {e}")
            return False
    
    async def test_page_elements(self):
        """Test if key page elements are present"""
        print("\n🔍 Testing page elements...")
        
        elements_to_test = [
            {"selector": "h1", "name": "Main Title"},
            {"selector": ".ag-root-wrapper", "name": "AG-Grid"},
            {"selector": "#export-btn", "name": "Export Button"},
            {"selector": "#format-column-selector", "name": "Format Column Dropdown"},
            {"selector": "#format-column-btn", "name": "Format Column Button"},
            {"selector": "#column-selector", "name": "Column Selector"}
        ]
        
        test_results = {}
        
        for element in elements_to_test:
            try:
                payload = {
                    "action": "query_selector",
                    "page_id": self.page_id,
                    "selector": element["selector"]
                }
                
                async with self.session.post(f"{self.mcp_url}/api/query", json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("found"):
                            print(f"✅ {element['name']} found")
                            test_results[element['name']] = True
                        else:
                            print(f"❌ {element['name']} not found")
                            test_results[element['name']] = False
                    else:
                        print(f"⚠️ {element['name']} test failed")
                        test_results[element['name']] = False
                        
                await asyncio.sleep(0.5)  # Small delay between tests
                
            except Exception as e:
                print(f"❌ Error testing {element['name']}: {e}")
                test_results[element['name']] = False
        
        return test_results
    
    async def test_modal_functionality(self):
        """Test the new modal formatting interface"""
        print("\n🎨 Testing modal functionality...")
        
        try:
            # Step 1: Select a column from dropdown
            print("📝 Selecting column from dropdown...")
            payload = {
                "action": "click",
                "page_id": self.page_id,
                "selector": "#format-column-selector"
            }
            
            async with self.session.post(f"{self.mcp_url}/api/click", json=payload) as response:
                if response.status == 200:
                    print("✅ Dropdown clicked")
                    await asyncio.sleep(1)
                    
                    # Select an option (try to select second option)
                    option_payload = {
                        "action": "click",
                        "page_id": self.page_id,
                        "selector": "div[data-value]:nth-child(2)"  # Second option
                    }
                    
                    async with self.session.post(f"{self.mcp_url}/api/click", json=option_payload) as response:
                        if response.status == 200:
                            print("✅ Column selected")
                            await asyncio.sleep(1)
                            
                            # Step 2: Click Format Column button
                            btn_payload = {
                                "action": "click",
                                "page_id": self.page_id,
                                "selector": "#format-column-btn"
                            }
                            
                            async with self.session.post(f"{self.mcp_url}/api/click", json=btn_payload) as response:
                                if response.status == 200:
                                    print("✅ Format button clicked")
                                    await asyncio.sleep(2)
                                    
                                    # Step 3: Check if modal opened
                                    modal_payload = {
                                        "action": "query_selector",
                                        "page_id": self.page_id,
                                        "selector": "#format-modal.show"
                                    }
                                    
                                    async with self.session.post(f"{self.mcp_url}/api/query", json=modal_payload) as response:
                                        if response.status == 200:
                                            result = await response.json()
                                            if result.get("found"):
                                                print("🎉 Modal opened successfully!")
                                                await self.take_screenshot("modal_opened.png")
                                                
                                                # Close modal
                                                close_payload = {
                                                    "action": "click",
                                                    "page_id": self.page_id,
                                                    "selector": "#modal-cancel-btn"
                                                }
                                                
                                                async with self.session.post(f"{self.mcp_url}/api/click", json=close_payload):
                                                    print("✅ Modal closed")
                                                    return True
                                            else:
                                                print("❌ Modal did not open")
                                                return False
                                        else:
                                            print("❌ Could not check modal state")
                                            return False
                                else:
                                    print("❌ Format button click failed")
                                    return False
                        else:
                            print("❌ Column selection failed")
                            return False
                else:
                    print("❌ Dropdown click failed")
                    return False
                    
        except Exception as e:
            print(f"❌ Modal test error: {e}")
            return False
    
    async def test_grid_interactions(self):
        """Test grid sorting and interactions"""
        print("\n📊 Testing grid interactions...")
        
        try:
            # Test column header click for sorting
            payload = {
                "action": "click",
                "page_id": self.page_id,
                "selector": ".ag-header-cell[col-id='Last_Spread']"
            }
            
            async with self.session.post(f"{self.mcp_url}/api/click", json=payload) as response:
                if response.status == 200:
                    print("✅ Column header clicked (sorting)")
                    await asyncio.sleep(1)
                    await self.take_screenshot("grid_sorted.png")
                    return True
                else:
                    print("❌ Grid interaction failed")
                    return False
        except Exception as e:
            print(f"❌ Grid interaction error: {e}")
            return False
    
    async def test_export_functionality(self):
        """Test export buttons"""
        print("\n📤 Testing export functionality...")
        
        try:
            payload = {
                "action": "click",
                "page_id": self.page_id,
                "selector": "#export-btn"
            }
            
            async with self.session.post(f"{self.mcp_url}/api/click", json=payload) as response:
                if response.status == 200:
                    print("✅ Export button clicked")
                    await asyncio.sleep(2)
                    return True
                else:
                    print("❌ Export button click failed")
                    return False
        except Exception as e:
            print(f"❌ Export test error: {e}")
            return False
    
    async def run_comprehensive_test_suite(self):
        """Run all tests and generate comprehensive report"""
        print("🧪 STARTING COMPREHENSIVE PLAYWRIGHT MCP TEST SUITE")
        print("=" * 60)
        
        # Pre-flight check
        if not self.check_app_running():
            return False
        
        try:
            # Connect to MCP server
            if not await self.connect_to_mcp_server():
                return False
            
            # Create browser page
            if not await self.create_browser_page():
                return False
            
            # Navigate to app
            if not await self.navigate_to_app():
                return False
            
            # Take initial screenshot
            await self.take_screenshot("app_loaded.png")
            
            # Run test suite
            print("\n🧪 RUNNING TEST SUITE")
            print("-" * 40)
            
            test_results = {
                "page_elements": await self.test_page_elements(),
                "modal_functionality": await self.test_modal_functionality(),
                "grid_interactions": await self.test_grid_interactions(),
                "export_functionality": await self.test_export_functionality()
            }
            
            # Take final screenshot
            await self.take_screenshot("test_completed.png")
            
            # Generate report
            self.generate_comprehensive_report(test_results)
            
            return test_results
            
        except Exception as e:
            print(f"❌ Test suite error: {e}")
            return False
        finally:
            # Cleanup
            await self.cleanup()
    
    def check_app_running(self):
        """Check if Dash app is running"""
        try:
            response = requests.get(self.app_url, timeout=5)
            if response.status_code == 200:
                print("✅ Dash app is running")
                return True
            else:
                print(f"❌ Dash app returned status: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print("❌ Dash app not running")
            print("💡 Start with: poetry run python app_column_formatting_fixed.py")
            return False
    
    def generate_comprehensive_report(self, test_results):
        """Generate detailed test report"""
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE PLAYWRIGHT MCP TEST RESULTS")
        print("=" * 60)
        
        total_tests = 0
        passed_tests = 0
        
        for category, results in test_results.items():
            print(f"\n📋 {category.replace('_', ' ').title()}:")
            
            if isinstance(results, dict):
                for test_name, passed in results.items():
                    status = "✅ PASS" if passed else "❌ FAIL"
                    print(f"  {test_name}: {status}")
                    total_tests += 1
                    if passed:
                        passed_tests += 1
            else:
                status = "✅ PASS" if results else "❌ FAIL"
                print(f"  Overall: {status}")
                total_tests += 1
                if results:
                    passed_tests += 1
        
        print(f"\n🎯 FINAL SCORE: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            print("🎉 PERFECT SCORE! All tests passed!")
            print("✨ Your Dash app is working flawlessly with Playwright MCP!")
        else:
            print("⚠️ Some tests failed - check individual results above")
        
        # Save report to file
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "test_results": test_results,
            "app_url": self.app_url,
            "mcp_server": f"localhost:{self.mcp_port}"
        }
        
        report_file = Path("test/playwright-output/test_report.json")
        report_file.parent.mkdir(exist_ok=True)
        
        with open(report_file, "w") as f:
            json.dump(report_data, f, indent=2)
        
        print(f"\n📄 Detailed report saved: {report_file}")
        print("📸 Screenshots saved in: test/playwright-output/")
        
        return report_data
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            if self.page_id and self.session:
                payload = {
                    "action": "close_page",
                    "page_id": self.page_id
                }
                async with self.session.post(f"{self.mcp_url}/api/close", json=payload):
                    print("🧹 Browser page closed")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")

async def run_playwright_mcp_tests():
    """Main function to run Playwright MCP tests"""
    tester = PlaywrightMCPTester()
    results = await tester.run_comprehensive_test_suite()
    return results

if __name__ == "__main__":
    # Run the comprehensive test suite
    asyncio.run(run_playwright_mcp_tests()) 