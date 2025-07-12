#!/usr/bin/env python3
"""
Launch Script for Bond G-Spread Analytics Dashboard
================================================

This script launches the multi-tab dtale dashboard for bond analytics.
It imports from the organized src/analytics module structure.

Usage:
    poetry run python launch_dtale_dashboard.py
    poetry run python launch_dtale_dashboard.py --sample-size 25000
    poetry run python launch_dtale_dashboard.py --port 8050

Author: Trading Analytics Team
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

if __name__ == "__main__":
    try:
        import sys
        from analytics.dtale_dashboard import main
        
        # Run the main function from the dashboard module
        main()
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure the src/analytics module is properly set up.")
        print("Alternative: Use 'poetry run python src/analytics/dtale_dashboard.py' directly")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error launching dashboard: {e}")
        sys.exit(1) 