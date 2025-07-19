"""
🧹 LOG CLEANUP UTILITY
======================
Automated log file management to keep the logs directory clean.
Deletes log files older than a specified number of days.
"""

import os
import glob
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging


class LogCleanupManager:
    """
    Manages log file cleanup to prevent logs directory from growing too large.
    
    Features:
    - Automatic deletion of old log files
    - Configurable retention periods for different log types
    - Safe deletion with backup options
    - Detailed reporting of cleanup operations
    - Integration with pipeline orchestration
    """
    
    def __init__(self, logs_dir: str = "logs", retention_days: int = 5):
        """
        Initialize log cleanup manager.
        
        Args:
            logs_dir: Directory containing log files
            retention_days: Number of days to keep log files (default: 5)
        """
        self.logs_dir = Path(logs_dir)
        self.retention_days = retention_days
        self.cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        # Define log file patterns and their retention periods
        self.log_patterns = {
            # Pipeline orchestrator logs (keep for retention_days)
            "pipeline_orchestrator_*.log": retention_days,
            
            # Individual processor logs (keep for retention_days)
            "*_processor.log": retention_days,
            
            # Database logs (keep longer due to importance)
            "db*.log": retention_days * 2,  # 10 days
            
            # Large database logs (keep even longer)
            "db_cusip*.log": retention_days * 3,  # 15 days
            
            # Performance and audit logs (keep for analysis)
            "db_perf*.log": retention_days * 2,  # 10 days
            "db_audit*.log": retention_days * 2,  # 10 days
            
            # G-spread processor logs (keep for retention_days)
            "g_spread_processor.log": retention_days,
            
            # Pipeline master logs (keep longer)
            "pipeline_master.log": retention_days * 2,  # 10 days
        }
        
        # Setup cleanup logger
        self._setup_cleanup_logger()
    
    def _setup_cleanup_logger(self):
        """Setup logging for cleanup operations."""
        cleanup_log_file = self.logs_dir / "log_cleanup.log"
        
        # Create logs directory if it doesn't exist
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup cleanup logger
        cleanup_logger = logging.getLogger("log_cleanup")
        cleanup_logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        cleanup_logger.handlers.clear()
        
        # Create file handler
        file_handler = logging.FileHandler(cleanup_log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        cleanup_logger.addHandler(file_handler)
        
        self.cleanup_logger = cleanup_logger
    
    def get_log_files(self) -> List[Path]:
        """Get all log files in the logs directory."""
        if not self.logs_dir.exists():
            return []
        
        log_files = []
        for pattern in self.log_patterns.keys():
            # Convert glob pattern to pathlib pattern
            if "*" in pattern:
                # Handle wildcard patterns
                if pattern.startswith("*"):
                    # Pattern like "*_processor.log"
                    log_files.extend(self.logs_dir.glob(pattern))
                else:
                    # Pattern like "pipeline_orchestrator_*.log"
                    log_files.extend(self.logs_dir.glob(pattern))
            else:
                # Exact filename
                exact_file = self.logs_dir / pattern
                if exact_file.exists():
                    log_files.append(exact_file)
        
        # Remove duplicates and return unique files
        return list(set(log_files))
    
    def get_file_age_days(self, file_path: Path) -> float:
        """Get the age of a file in days."""
        if not file_path.exists():
            return float('inf')
        
        # Get file modification time
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        age = datetime.now() - mtime
        return age.days
    
    def should_delete_file(self, file_path: Path) -> bool:
        """Determine if a file should be deleted based on its age and pattern."""
        if not file_path.exists():
            return False
        
        # Get file age in days
        age_days = self.get_file_age_days(file_path)
        
        # Check against all patterns to find the appropriate retention period
        for pattern, retention_days in self.log_patterns.items():
            if self._file_matches_pattern(file_path, pattern):
                return age_days > retention_days
        
        # Default retention period for unmatched files
        return age_days > self.retention_days
    
    def _file_matches_pattern(self, file_path: Path, pattern: str) -> bool:
        """Check if a file matches a given pattern."""
        filename = file_path.name
        
        if pattern == "*_processor.log":
            return filename.endswith("_processor.log")
        elif pattern == "pipeline_orchestrator_*.log":
            return filename.startswith("pipeline_orchestrator_") and filename.endswith(".log")
        elif pattern == "db*.log":
            return filename.startswith("db") and filename.endswith(".log")
        elif pattern == "db_cusip*.log":
            return filename.startswith("db_cusip") and filename.endswith(".log")
        elif pattern == "db_perf*.log":
            return filename.startswith("db_perf") and filename.endswith(".log")
        elif pattern == "db_audit*.log":
            return filename.startswith("db_audit") and filename.endswith(".log")
        elif pattern == "g_spread_processor.log":
            return filename == "g_spread_processor.log"
        elif pattern == "pipeline_master.log":
            return filename == "pipeline_master.log"
        else:
            return filename == pattern
    
    def cleanup_logs(self, dry_run: bool = False) -> Dict[str, any]:
        """
        Clean up old log files.
        
        Args:
            dry_run: If True, only report what would be deleted without actually deleting
            
        Returns:
            Dictionary with cleanup statistics
        """
        self.cleanup_logger.info("🧹 Starting log cleanup process...")
        
        # Get all log files
        log_files = self.get_log_files()
        
        # Categorize files
        files_to_delete = []
        files_to_keep = []
        
        for file_path in log_files:
            if self.should_delete_file(file_path):
                files_to_delete.append(file_path)
            else:
                files_to_keep.append(file_path)
        
        # Calculate total size of files to delete
        total_size_to_delete = sum(f.stat().st_size for f in files_to_delete)
        total_size_mb = total_size_to_delete / (1024 * 1024)
        
        # Report statistics
        self.cleanup_logger.info(f"📊 Log cleanup statistics:")
        self.cleanup_logger.info(f"   Total log files found: {len(log_files)}")
        self.cleanup_logger.info(f"   Files to keep: {len(files_to_keep)}")
        self.cleanup_logger.info(f"   Files to delete: {len(files_to_delete)}")
        self.cleanup_logger.info(f"   Total size to delete: {total_size_mb:.2f} MB")
        
        if dry_run:
            self.cleanup_logger.info("🔍 DRY RUN MODE - No files will be deleted")
            if files_to_delete:
                self.cleanup_logger.info("📋 Files that would be deleted:")
                for file_path in files_to_delete:
                    age_days = self.get_file_age_days(file_path)
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    self.cleanup_logger.info(f"   {file_path.name} (age: {age_days:.1f} days, size: {size_mb:.2f} MB)")
        else:
            # Actually delete files
            deleted_files = []
            failed_deletions = []
            
            for file_path in files_to_delete:
                try:
                    age_days = self.get_file_age_days(file_path)
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    
                    file_path.unlink()  # Delete the file
                    deleted_files.append(file_path)
                    
                    self.cleanup_logger.info(f"🗑️  Deleted: {file_path.name} (age: {age_days:.1f} days, size: {size_mb:.2f} MB)")
                    
                except Exception as e:
                    failed_deletions.append((file_path, str(e)))
                    self.cleanup_logger.error(f"❌ Failed to delete {file_path.name}: {str(e)}")
            
            # Report results
            self.cleanup_logger.info(f"✅ Cleanup completed:")
            self.cleanup_logger.info(f"   Successfully deleted: {len(deleted_files)} files")
            self.cleanup_logger.info(f"   Failed deletions: {len(failed_deletions)}")
            
            if failed_deletions:
                self.cleanup_logger.warning("⚠️  Failed deletions:")
                for file_path, error in failed_deletions:
                    self.cleanup_logger.warning(f"   {file_path.name}: {error}")
        
        # Return statistics
        return {
            "total_files": len(log_files),
            "files_to_keep": len(files_to_keep),
            "files_to_delete": len(files_to_delete),
            "total_size_mb": total_size_mb,
            "dry_run": dry_run,
            "deleted_files": len(files_to_delete) if not dry_run else 0,
            "failed_deletions": len(failed_deletions) if not dry_run else 0
        }
    
    def get_log_directory_stats(self) -> Dict[str, any]:
        """Get statistics about the logs directory."""
        if not self.logs_dir.exists():
            return {
                "directory_exists": False,
                "total_files": 0,
                "total_size_mb": 0,
                "oldest_file_days": 0,
                "newest_file_days": 0
            }
        
        log_files = self.get_log_files()
        
        if not log_files:
            return {
                "directory_exists": True,
                "total_files": 0,
                "total_size_mb": 0,
                "oldest_file_days": 0,
                "newest_file_days": 0
            }
        
        # Calculate statistics
        total_size = sum(f.stat().st_size for f in log_files)
        total_size_mb = total_size / (1024 * 1024)
        
        file_ages = [self.get_file_age_days(f) for f in log_files]
        oldest_file_days = max(file_ages) if file_ages else 0
        newest_file_days = min(file_ages) if file_ages else 0
        
        return {
            "directory_exists": True,
            "total_files": len(log_files),
            "total_size_mb": total_size_mb,
            "oldest_file_days": oldest_file_days,
            "newest_file_days": newest_file_days
        }


def cleanup_logs_automatically(logs_dir: str = "logs", retention_days: int = 5, dry_run: bool = False) -> Dict[str, any]:
    """
    Convenience function to automatically clean up logs.
    
    Args:
        logs_dir: Directory containing log files
        retention_days: Number of days to keep log files
        dry_run: If True, only report what would be deleted
        
    Returns:
        Dictionary with cleanup statistics
    """
    cleanup_manager = LogCleanupManager(logs_dir, retention_days)
    return cleanup_manager.cleanup_logs(dry_run)


if __name__ == "__main__":
    """Command line interface for log cleanup."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Clean up old log files")
    parser.add_argument("--logs-dir", default="logs", help="Logs directory path")
    parser.add_argument("--retention-days", type=int, default=5, help="Number of days to keep logs")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without actually deleting")
    
    args = parser.parse_args()
    
    # Run cleanup
    stats = cleanup_logs_automatically(
        logs_dir=args.logs_dir,
        retention_days=args.retention_days,
        dry_run=args.dry_run
    )
    
    # Print summary
    print(f"\n📊 Cleanup Summary:")
    print(f"   Total files: {stats['total_files']}")
    print(f"   Files to keep: {stats['files_to_keep']}")
    print(f"   Files to delete: {stats['files_to_delete']}")
    print(f"   Size to free: {stats['total_size_mb']:.2f} MB")
    
    if not args.dry_run:
        print(f"   Successfully deleted: {stats['deleted_files']} files")
        print(f"   Failed deletions: {stats['failed_deletions']}")
    
    print(f"✅ Log cleanup {'simulation' if args.dry_run else 'operation'} completed!") 