#!/usr/bin/env python3
"""
MASTER PIPELINE ORCHESTRATOR & SIMPLIFIED CLI
==============================================
Unified entry point for all data processing pipelines.
Provides both advanced CLI and interactive menu.
Follows .cursorrules standards for error handling, logging, and dependency management.
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
import traceback
import subprocess

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from src.orchestrator.pipeline_manager import PipelineManager
from src.orchestrator.pipeline_config import PipelineConfig
from src.utils.logging import LogManager

def get_consolidated_log_path() -> str:
    """Get path for consolidated pipeline log file."""
    return "logs/pipeline_orchestrator.log"

def log_execution_separator(logger: LogManager):
    """Log a separator for each execution run."""
    separator = "=" * 80
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info("")
    logger.info(separator)
    logger.info(f"NEW PIPELINE EXECUTION STARTED - {timestamp}")
    logger.info(separator)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create comprehensive CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Master Pipeline Orchestrator - Trading Analytics System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipe.py --full                    # Run complete pipeline
  python run_pipe.py --universe --portfolio   # Run specific pipelines
  python run_pipe.py --validate-only          # Validate configuration only
  python run_pipe.py --dry-run               # Show execution plan without running
  python run_pipe.py --resume-from=g-spread  # Resume from specific stage
  python run_pipe.py --menu                  # Launch interactive menu
        """
    )
    
    # Pipeline Selection
    pipeline_group = parser.add_argument_group('Pipeline Selection')
    pipeline_group.add_argument('--full', action='store_true', 
                               help='Run complete pipeline (all stages)')
    pipeline_group.add_argument('--universe', action='store_true',
                               help='Run universe processing pipeline')
    pipeline_group.add_argument('--portfolio', action='store_true',
                               help='Run portfolio processing pipeline')
    pipeline_group.add_argument('--historical-gspread', action='store_true',
                               help='Run historical G-spread pipeline')
    pipeline_group.add_argument('--gspread-analytics', action='store_true',
                               help='Run G-spread analytics pipeline')
    pipeline_group.add_argument('--runs', action='store_true',
                               help='Run trading runs processing pipeline')
    
    # Execution Control
    control_group = parser.add_argument_group('Execution Control')
    control_group.add_argument('--dry-run', action='store_true',
                              help='Show execution plan without running')
    control_group.add_argument('--validate-only', action='store_true',
                              help='Validate configuration and dependencies only')
    control_group.add_argument('--resume-from', type=str,
                              help='Resume pipeline from specific stage')
    control_group.add_argument('--force', action='store_true',
                              help='Force execution even if dependencies are missing')
    control_group.add_argument('--parallel', action='store_true',
                              help='Enable parallel execution where possible')
    
    # Configuration
    config_group = parser.add_argument_group('Configuration')
    config_group.add_argument('--config', type=str, default='config/config.yaml',
                             help='Path to configuration file')
    config_group.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                             default='INFO', help='Set logging level')
    config_group.add_argument('--log-file', type=str,
                             help='Override default log file path')
    
    # Monitoring & Reporting
    monitor_group = parser.add_argument_group('Monitoring & Reporting')
    monitor_group.add_argument('--monitor', action='store_true',
                              help='Enable real-time monitoring dashboard')
    monitor_group.add_argument('--report', action='store_true',
                              help='Generate comprehensive execution report')
    monitor_group.add_argument('--notify', type=str,
                              help='Send notifications to specified endpoint')
    
    # Interactive menu
    parser.add_argument('--menu', action='store_true', help='Launch interactive menu')
    
    return parser


def interactive_menu():
    """Interactive CLI for pipeline execution."""
    print("Trading Analytics Pipeline")
    print("=" * 40)
    print()
    print("Available options:")
    print("1. Run complete pipeline")
    print("2. Run universe processing only")
    print("3. Run portfolio processing only")
    print("4. Run historical G-spread processing")
    print("5. Run G-spread analytics")
    print("6. Run trading runs processing")
    print("7. Validate configuration")
    print("8. Show execution plan (dry run)")
    print("9. Resume from specific stage")
    print("10. Exit")
    print()
    
    choice = input("Enter your choice (1-10): ").strip()
    
    commands = {
        "1": ["--full"],
        "2": ["--universe"],
        "3": ["--portfolio"],
        "4": ["--historical-gspread"],
        "5": ["--gspread-analytics"],
        "6": ["--runs"],
        "7": ["--validate-only"],
        "8": ["--full", "--dry-run"],
        "9": None,  # Special handling
        "10": None   # Exit
    }
    
    if choice == "9":
        print("\nAvailable stages:")
        stages = ["universe", "portfolio", "historical-gspread", "gspread-analytics", "runs-excel", "runs-monitor"]
        for i, stage in enumerate(stages, 1):
            print(f"  {i}. {stage}")
        
        stage_choice = input("Enter stage number: ").strip()
        try:
            stage_index = int(stage_choice) - 1
            if 0 <= stage_index < len(stages):
                args = [f"--resume-from={stages[stage_index]}"]
            else:
                print("Invalid stage number")
                return
        except ValueError:
            print("Invalid input")
            return
    elif choice == "10":
        print("Goodbye!")
        return
    else:
        args = commands.get(choice)
    
    if args:
        print(f"\nExecuting: python run_pipe.py {' '.join(args)}")
        print("-" * 40)
        subprocess.run([sys.executable, __file__] + args)
    else:
        print("Invalid choice")


async def main():
    """Main orchestrator entry point with comprehensive error handling."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    if getattr(args, 'menu', False):
        interactive_menu()
        return 0
    
    # Initialize logging
    log_file = args.log_file or get_consolidated_log_path()
    logger = LogManager(log_file=log_file, log_level=args.log_level)
    
    try:
        log_execution_separator(logger)
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info(f"Arguments: {vars(args)}")
        
        # Load and validate configuration
        config = PipelineConfig.load_from_file(args.config)
        if args.validate_only:
            logger.info("Configuration validation complete")
            return 0
        
        # Initialize pipeline manager
        manager = PipelineManager(config, logger)
        
        # Determine execution plan
        execution_plan = manager.create_execution_plan(args)
        
        if args.dry_run:
            manager.display_execution_plan(execution_plan)
            return 0
        
        # Execute pipeline
        results = await manager.execute_pipeline(execution_plan, args)
        
        # Generate reports if requested
        if args.report:
            manager.generate_execution_report(results)
        
        # Send notifications if configured
        if args.notify:
            await manager.send_notifications(args.notify, results)
        
        logger.info("[COMPLETE] PIPELINE ORCHESTRATION COMPLETE")
        return 0 if results.success else 1
        
    except KeyboardInterrupt:
        logger.warning("Pipeline execution interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Pipeline orchestration failed: {str(e)}")
        logger.error(f"Stack trace:\n{traceback.format_exc()}")
        return 1
    finally:
        logger.info("[DONE] Pipeline orchestrator shutdown complete")


if __name__ == "__main__":
    sys.exit(asyncio.run(main())) 