#!/usr/bin/env python3
"""
Simple startup script for the Bond Z-Score Dashboard
"""

import subprocess
import sys
import os

def main():
    print("🚀 Starting Bond Z-Score Dashboard...")
    print("=" * 50)
    
    # Check if we're in the correct directory
    if not os.path.exists('historical g spread/bond_z.parquet'):
        print("❌ Error: bond_z.parquet not found!")
        print("Please make sure you're running this from the project root directory.")
        sys.exit(1)
    
    try:
        # Run the dashboard
        print("📊 Loading dashboard...")
        subprocess.run([sys.executable, 'bond_dashboard.py'], check=True)
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except FileNotFoundError:
        print("❌ Error: bond_dashboard.py not found!")
        print("Please make sure bond_dashboard.py is in the current directory.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error running dashboard: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()