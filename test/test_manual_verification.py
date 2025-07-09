"""
Manual verification script for testing individual Dash app features
Run this to manually verify specific functionality works correctly
"""

import requests
import time
import webbrowser
from pathlib import Path

class ManualDashAppVerifier:
    """
    Manual verification helper for testing Dash app features
    Opens browser and provides step-by-step verification instructions
    """
    
    def __init__(self, app_url="http://localhost:8056"):
        self.app_url = app_url
    
    def check_app_status(self):
        """Check if app is running"""
        try:
            response = requests.get(self.app_url, timeout=5)
            if response.status_code == 200:
                print("✅ App is running and responding")
                return True
            else:
                print(f"❌ App returned status: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print("❌ Cannot connect to app - is it running?")
            print("💡 Start app with: poetry run python app_column_formatting_fixed.py")
            return False
    
    def open_app_in_browser(self):
        """Open the app in default browser"""
        if self.check_app_status():
            print(f"🌐 Opening {self.app_url} in your browser...")
            webbrowser.open(self.app_url)
            time.sleep(2)
            return True
        return False
    
    def run_manual_verification(self):
        """Run step-by-step manual verification"""
        print("🧪 MANUAL DASH APP VERIFICATION")
        print("=" * 50)
        
        if not self.open_app_in_browser():
            return False
        
        print("\n📋 VERIFICATION CHECKLIST")
        print("=" * 30)
        print("Please manually verify the following in your browser:")
        
        # Test 1: Basic UI
        print("\n1️⃣ BASIC UI ELEMENTS:")
        print("   ✓ Page loads with 'Z Analytics Dashboard' title")
        print("   ✓ AG-Grid with data is visible")
        print("   ✓ Control buttons are visible (Export, Reset, etc.)")
        print("   ✓ Column selector dropdown works")
        
        input("Press Enter when you've verified the basic UI...")
        
        # Test 2: New Modal Interface
        print("\n2️⃣ NEW MODAL FORMATTING INTERFACE:")
        print("   ✓ 'Format Column' dropdown is visible (new!)")
        print("   ✓ Select any column from the dropdown")
        print("   ✓ Click 'Format Column' button")
        print("   ✓ Modal should open with formatting options")
        print("   ✓ Try changing number format, colors, etc.")
        print("   ✓ Click 'Apply Format' or 'Cancel'")
        print("   ✓ Modal should close properly")
        
        input("Press Enter when you've tested the modal interface...")
        
        # Test 3: Data Interaction
        print("\n3️⃣ DATA INTERACTION:")
        print("   ✓ Click column headers to sort")
        print("   ✓ Use filter icons if visible")
        print("   ✓ Try selecting rows with checkboxes")
        print("   ✓ Test export buttons (Excel/CSV)")
        
        input("Press Enter when you've tested data interactions...")
        
        # Test 4: Performance
        print("\n4️⃣ PERFORMANCE:")
        print("   ✓ Page loads quickly (< 5 seconds)")
        print("   ✓ Interactions are responsive")
        print("   ✓ No obvious errors in browser console (F12)")
        
        input("Press Enter when you've checked performance...")
        
        # Summary
        print("\n📊 VERIFICATION SUMMARY")
        print("=" * 30)
        
        results = {}
        results["basic_ui"] = input("Basic UI working? (y/n): ").lower().startswith('y')
        results["modal_interface"] = input("Modal interface working? (y/n): ").lower().startswith('y')
        results["data_interaction"] = input("Data interaction working? (y/n): ").lower().startswith('y')
        results["performance"] = input("Performance acceptable? (y/n): ").lower().startswith('y')
        
        # Report results
        print("\n🎯 FINAL RESULTS:")
        total_tests = len(results)
        passed_tests = sum(results.values())
        
        for test_name, passed in results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{test_name.replace('_', ' ').title()}: {status}")
        
        print(f"\nOverall: {passed_tests}/{total_tests} manual tests passed")
        
        if passed_tests == total_tests:
            print("🎉 ALL MANUAL TESTS PASSED!")
            print("✨ Your Dash app is working perfectly!")
        else:
            print("⚠️ Some issues found during manual testing")
            print("💡 Review the failed areas and iterate on improvements")
        
        return results

if __name__ == "__main__":
    verifier = ManualDashAppVerifier()
    verifier.run_manual_verification() 