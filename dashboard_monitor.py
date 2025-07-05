#!/usr/bin/env python3
"""
Comprehensive Dashboard Monitor and Audit System
Real-time monitoring, health checks, and bug detection for Bond Z-Score Dashboard
"""

import requests
import time
import json
import pandas as pd
import subprocess
import sys
import os
from datetime import datetime
from urllib.parse import urljoin
import re
from concurrent.futures import ThreadPoolExecutor
import threading
import signal

class DashboardMonitor:
    def __init__(self, base_url="http://localhost:8050"):
        self.base_url = base_url
        self.session = requests.Session()
        self.monitoring = True
        self.checks_passed = 0
        self.checks_failed = 0
        self.last_check_time = None
        self.errors = []
        self.performance_log = []
        
    def log_status(self, message, status="INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_emoji = {
            "INFO": "ℹ️",
            "SUCCESS": "✅", 
            "WARNING": "⚠️",
            "ERROR": "❌",
            "DEBUG": "🔍"
        }
        print(f"{status_emoji.get(status, 'ℹ️')} [{timestamp}] {message}")
        
    def check_dashboard_process(self):
        """Check if dashboard process is running"""
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            if 'bond_dashboard.py' in result.stdout:
                process_count = result.stdout.count('bond_dashboard.py')
                self.log_status(f"Dashboard process running ({process_count} instances)", "SUCCESS")
                return True
            else:
                self.log_status("Dashboard process not found", "ERROR")
                return False
        except Exception as e:
            self.log_status(f"Error checking process: {e}", "ERROR")
            return False
    
    def check_http_response(self):
        """Check basic HTTP response"""
        try:
            start_time = time.time()
            response = self.session.get(self.base_url, timeout=10)
            response_time = time.time() - start_time
            
            self.performance_log.append({
                'timestamp': datetime.now(),
                'response_time': response_time,
                'status_code': response.status_code,
                'content_length': len(response.content)
            })
            
            if response.status_code == 200:
                self.log_status(f"HTTP OK (200) - {response_time:.3f}s - {len(response.content)} bytes", "SUCCESS")
                return True, response
            else:
                self.log_status(f"HTTP Error {response.status_code}", "ERROR")
                return False, response
                
        except requests.exceptions.ConnectionError:
            self.log_status("Connection refused - Dashboard not accessible", "ERROR")
            return False, None
        except Exception as e:
            self.log_status(f"HTTP check failed: {e}", "ERROR")
            return False, None
    
    def check_dashboard_content(self, response):
        """Check if dashboard content is properly rendered"""
        if not response:
            return False
            
        content = response.text.lower()
        
        # Check for essential Dash components
        essential_checks = {
            'dash_app': 'dash' in content,
            'react_components': 'react' in content or '_dash' in content,
            'bootstrap_css': 'bootstrap' in content,
            'app_container': 'div' in content,
            'script_tags': '<script' in content
        }
        
        passed_checks = sum(essential_checks.values())
        total_checks = len(essential_checks)
        
        for check_name, passed in essential_checks.items():
            status = "SUCCESS" if passed else "ERROR"
            self.log_status(f"Content check '{check_name}': {'PASS' if passed else 'FAIL'}", status)
        
        self.log_status(f"Content validation: {passed_checks}/{total_checks} checks passed", 
                       "SUCCESS" if passed_checks == total_checks else "WARNING")
        
        return passed_checks == total_checks
    
    def check_dash_assets(self):
        """Check if Dash assets are loading properly"""
        asset_endpoints = [
            '_dash-layout',
            '_dash-dependencies', 
            '_dash-config',
            '_favicon.ico'
        ]
        
        assets_ok = True
        for endpoint in asset_endpoints:
            try:
                url = urljoin(self.base_url + '/', endpoint)
                response = self.session.get(url, timeout=5)
                if response.status_code in [200, 404]:  # 404 is ok for favicon
                    self.log_status(f"Asset '{endpoint}': OK", "SUCCESS")
                else:
                    self.log_status(f"Asset '{endpoint}': Error {response.status_code}", "WARNING")
                    assets_ok = False
            except Exception as e:
                self.log_status(f"Asset '{endpoint}': Failed - {e}", "ERROR")
                assets_ok = False
        
        return assets_ok
    
    def check_data_loading(self):
        """Verify data is loaded correctly"""
        try:
            df = pd.read_parquet('historical g spread/bond_z.parquet')
            
            # Data quality checks
            checks = {
                'row_count': len(df) > 0,
                'column_count': len(df.columns) >= 40,
                'zscore_column': 'Z_Score' in df.columns,
                'no_all_nan_columns': not any(df[col].isnull().all() for col in df.columns),
                'zscore_range_valid': df['Z_Score'].between(-10, 10).all()
            }
            
            for check_name, passed in checks.items():
                status = "SUCCESS" if passed else "ERROR"
                self.log_status(f"Data check '{check_name}': {'PASS' if passed else 'FAIL'}", status)
            
            data_info = {
                'rows': len(df),
                'columns': len(df.columns),
                'zscore_range': f"{df['Z_Score'].min():.2f} to {df['Z_Score'].max():.2f}",
                'unique_sectors': df['Custom_Sector_1'].nunique(),
                'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB"
            }
            
            self.log_status(f"Data stats: {data_info}", "INFO")
            return all(checks.values())
            
        except Exception as e:
            self.log_status(f"Data loading check failed: {e}", "ERROR")
            return False
    
    def test_dashboard_interactions(self):
        """Test dashboard interactive components"""
        try:
            # Test callback endpoints
            callback_tests = [
                '_dash-update-component',
                '_dash-layout',
                '_dash-dependencies'
            ]
            
            interactions_ok = True
            for endpoint in callback_tests:
                try:
                    url = urljoin(self.base_url + '/', endpoint)
                    response = self.session.post(url, json={}, timeout=5)
                    # Even if it returns an error, the endpoint should be reachable
                    self.log_status(f"Interaction endpoint '{endpoint}': Reachable", "SUCCESS")
                except Exception as e:
                    self.log_status(f"Interaction endpoint '{endpoint}': Failed - {e}", "WARNING")
                    interactions_ok = False
            
            return interactions_ok
            
        except Exception as e:
            self.log_status(f"Interaction test failed: {e}", "ERROR")
            return False
    
    def check_memory_usage(self):
        """Monitor memory usage of dashboard process"""
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            lines = result.stdout.split('\n')
            
            total_memory = 0
            process_count = 0
            
            for line in lines:
                if 'bond_dashboard.py' in line:
                    parts = line.split()
                    if len(parts) >= 6:
                        memory_percent = float(parts[3])
                        memory_kb = float(parts[5])
                        total_memory += memory_kb
                        process_count += 1
            
            if process_count > 0:
                memory_mb = total_memory / 1024
                self.log_status(f"Memory usage: {memory_mb:.1f} MB across {process_count} processes", 
                               "WARNING" if memory_mb > 500 else "SUCCESS")
                return memory_mb < 1000  # Alert if over 1GB
            else:
                self.log_status("No dashboard processes found for memory check", "ERROR")
                return False
                
        except Exception as e:
            self.log_status(f"Memory check failed: {e}", "ERROR")
            return False
    
    def check_port_accessibility(self):
        """Check if port 8050 is accessible"""
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 8050))
            sock.close()
            
            if result == 0:
                self.log_status("Port 8050 is accessible", "SUCCESS")
                return True
            else:
                self.log_status("Port 8050 is not accessible", "ERROR")
                return False
        except Exception as e:
            self.log_status(f"Port check failed: {e}", "ERROR")
            return False
    
    def run_comprehensive_audit(self):
        """Run all audit checks"""
        self.log_status("🔍 Starting Comprehensive Dashboard Audit", "INFO")
        self.log_status("=" * 60, "INFO")
        
        checks = [
            ("Process Check", self.check_dashboard_process),
            ("Port Accessibility", self.check_port_accessibility),
            ("Data Loading", self.check_data_loading),
            ("Memory Usage", self.check_memory_usage),
        ]
        
        # HTTP and content checks
        http_ok, response = self.check_http_response()
        if http_ok:
            checks.extend([
                ("Content Validation", lambda: self.check_dashboard_content(response)),
                ("Asset Loading", self.check_dash_assets),
                ("Interactions", self.test_dashboard_interactions)
            ])
        
        results = {}
        for check_name, check_func in checks:
            try:
                result = check_func()
                results[check_name] = result
                if result:
                    self.checks_passed += 1
                else:
                    self.checks_failed += 1
            except Exception as e:
                self.log_status(f"{check_name} failed with exception: {e}", "ERROR")
                results[check_name] = False
                self.checks_failed += 1
        
        self.last_check_time = datetime.now()
        
        # Summary
        self.log_status("=" * 60, "INFO")
        self.log_status("📋 AUDIT SUMMARY", "INFO")
        self.log_status("=" * 60, "INFO")
        
        for check_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            self.log_status(f"{status} {check_name}", "SUCCESS" if result else "ERROR")
        
        overall_health = all(results.values())
        total_checks = len(results)
        passed_checks = sum(results.values())
        
        self.log_status("=" * 60, "INFO")
        self.log_status(f"Overall Health: {'🟢 HEALTHY' if overall_health else '🔴 ISSUES DETECTED'}", 
                       "SUCCESS" if overall_health else "ERROR")
        self.log_status(f"Checks Passed: {passed_checks}/{total_checks}", "INFO")
        
        if self.performance_log:
            avg_response_time = sum(p['response_time'] for p in self.performance_log[-10:]) / min(10, len(self.performance_log))
            self.log_status(f"Average Response Time: {avg_response_time:.3f}s", "INFO")
        
        return overall_health, results
    
    def continuous_monitoring(self, interval=30):
        """Run continuous monitoring"""
        self.log_status(f"🔄 Starting continuous monitoring (every {interval}s)", "INFO")
        
        def signal_handler(signum, frame):
            self.log_status("📊 Monitoring stopped by user", "INFO")
            self.monitoring = False
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            while self.monitoring:
                self.run_comprehensive_audit()
                self.log_status(f"⏰ Next check in {interval} seconds...", "INFO")
                time.sleep(interval)
        except KeyboardInterrupt:
            self.log_status("📊 Monitoring stopped", "INFO")
    
    def generate_health_report(self):
        """Generate a comprehensive health report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'checks_passed': self.checks_passed,
            'checks_failed': self.checks_failed,
            'last_check': self.last_check_time.isoformat() if self.last_check_time else None,
            'performance_summary': {
                'total_requests': len(self.performance_log),
                'avg_response_time': sum(p['response_time'] for p in self.performance_log) / max(1, len(self.performance_log)),
                'success_rate': sum(1 for p in self.performance_log if p['status_code'] == 200) / max(1, len(self.performance_log))
            } if self.performance_log else {}
        }
        
        with open('dashboard_health_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        self.log_status("📄 Health report saved to dashboard_health_report.json", "SUCCESS")
        return report

def main():
    """Main monitoring function"""
    print("🚀 Bond Z-Score Dashboard Monitor & Audit System")
    print("=" * 60)
    
    monitor = DashboardMonitor()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'continuous':
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        monitor.continuous_monitoring(interval)
    else:
        # Single audit run
        healthy, results = monitor.run_comprehensive_audit()
        monitor.generate_health_report()
        
        if not healthy:
            print("\n🔧 RECOMMENDED ACTIONS:")
            if not results.get('Process Check', True):
                print("• Restart the dashboard: python3 bond_dashboard.py")
            if not results.get('Data Loading', True):
                print("• Check if bond_z.parquet file exists and is readable")
            if not results.get('Memory Usage', True):
                print("• Consider restarting dashboard to free memory")
            if not results.get('Port Accessibility', True):
                print("• Check if port 8050 is blocked by firewall")
        
        sys.exit(0 if healthy else 1)

if __name__ == '__main__':
    main()