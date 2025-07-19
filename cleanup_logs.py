#!/usr/bin/env python3
"""
🧹 LOG CLEANUP SCRIPT
=====================
Standalone script to clean up old log files in the logs directory.
Automatically deletes log files older than a specified number of days.

Usage:
    python cleanup_logs.py                    # Clean logs older than 5 days
    python cleanup_logs.py --retention-days 3 # Clean logs older than 3 days
    python cleanup_logs.py --dry-run         # Show what would be deleted
    python cleanup_logs.py --help            # Show help
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from src.utils.log_cleanup import cleanup_logs_automatically


def main():
    """Main function for log cleanup script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="🧹 Clean up old log files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cleanup_logs.py                    # Clean logs older than 5 days
  python cleanup_logs.py --retention-days 3 # Clean logs older than 3 days
  python cleanup_logs.py --dry-run         # Show what would be deleted
  python cleanup_logs.py --logs-dir logs   # Specify logs directory
        """
    )
    
    parser.add_argument(
        "--logs-dir", 
        default="logs", 
        help="Logs directory path (default: logs)"
    )
    parser.add_argument(
        "--retention-days", 
        type=int, 
        default=5, 
        help="Number of days to keep logs (default: 5)"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Show what would be deleted without actually deleting"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true", 
        help="Show detailed output"
    )
    
    args = parser.parse_args()
    
    print("🧹 Log Cleanup Utility")
    print("=" * 40)
    print(f"Logs directory: {args.logs_dir}")
    print(f"Retention period: {args.retention_days} days")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'ACTUAL CLEANUP'}")
    print("=" * 40)
    
    try:
        # Run cleanup
        stats = cleanup_logs_automatically(
            logs_dir=args.logs_dir,
            retention_days=args.retention_days,
            dry_run=args.dry_run
        )
        
        # Print summary
        print(f"\n📊 Cleanup Summary:")
        print(f"   Total log files found: {stats['total_files']}")
        print(f"   Files to keep: {stats['files_to_keep']}")
        print(f"   Files to delete: {stats['files_to_delete']}")
        print(f"   Space to free: {stats['total_size_mb']:.2f} MB")
        
        if not args.dry_run:
            print(f"   Successfully deleted: {stats['deleted_files']} files")
            print(f"   Failed deletions: {stats['failed_deletions']}")
        
        if args.verbose and stats['files_to_delete'] > 0:
            print(f"\n📋 Files {'that would be' if args.dry_run else 'that were'} deleted:")
            # Note: The actual file list is logged in the cleanup log file
        
        print(f"\n✅ Log cleanup {'simulation' if args.dry_run else 'operation'} completed!")
        
        if not args.dry_run and stats['deleted_files'] > 0:
            print(f"💾 Freed {stats['total_size_mb']:.2f} MB of disk space")
        
        return 0 if stats['failed_deletions'] == 0 else 1
        
    except Exception as e:
        print(f"❌ Error during log cleanup: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 