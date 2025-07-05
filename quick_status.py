#!/usr/bin/env python3
"""
Quick Dashboard Status Checker
Provides instant health status with minimal overhead
"""

import subprocess
import socket
import time
from datetime import datetime

def check_process():
    """Quick process check"""
    try:
        result = subprocess.run(['pgrep', '-f', 'bond_dashboard.py'], 
                              capture_output=True, text=True, timeout=2)
        pids = result.stdout.strip().split('\n') if result.stdout.strip() else []
        return len([p for p in pids if p]), pids
    except:
        return 0, []

def check_port():
    """Quick port check"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', 8050))
        sock.close()
        return result == 0
    except:
        return False

def check_memory():
    """Quick memory check"""
    try:
        result = subprocess.run(['ps', '-p'] + get_pids() + ['-o', 'rss='], 
                              capture_output=True, text=True, timeout=2)
        if result.stdout:
            memory_kb = sum(int(line.strip()) for line in result.stdout.strip().split('\n') if line.strip())
            return memory_kb / 1024  # Convert to MB
    except:
        pass
    return 0

def get_pids():
    """Get dashboard PIDs"""
    try:
        result = subprocess.run(['pgrep', '-f', 'bond_dashboard.py'], 
                              capture_output=True, text=True, timeout=2)
        return result.stdout.strip().split('\n') if result.stdout.strip() else []
    except:
        return []

def main():
    print("⚡ QUICK DASHBOARD STATUS CHECK")
    print("=" * 40)
    print(f"🕐 Time: {datetime.now().strftime('%H:%M:%S')}")
    
    # Process check
    process_count, pids = check_process()
    process_status = "🟢 RUNNING" if process_count > 0 else "🔴 STOPPED"
    print(f"📱 Process: {process_status} ({process_count} instances)")
    
    # Port check
    port_open = check_port()
    port_status = "🟢 OPEN" if port_open else "🔴 CLOSED"
    print(f"🌐 Port 8050: {port_status}")
    
    # Memory check
    memory_mb = check_memory()
    memory_status = "🟢 OK" if memory_mb < 500 else "⚠️ HIGH" if memory_mb < 1000 else "🔴 CRITICAL"
    print(f"💾 Memory: {memory_status} ({memory_mb:.1f} MB)")
    
    # Overall status
    overall_healthy = process_count > 0 and port_open and memory_mb < 1000
    overall_status = "🟢 HEALTHY" if overall_healthy else "🔴 ISSUES"
    print(f"🏥 Overall: {overall_status}")
    
    # Quick recommendations
    if not overall_healthy:
        print("\n🔧 QUICK FIXES:")
        if process_count == 0:
            print("  • Run: python3 bond_dashboard.py")
        if not port_open and process_count > 0:
            print("  • Dashboard starting up, wait 30 seconds")
        if memory_mb > 500:
            print("  • Restart dashboard to free memory")
    
    print("=" * 40)

if __name__ == '__main__':
    main()