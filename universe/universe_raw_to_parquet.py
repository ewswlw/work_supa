import sys
import argparse
from pathlib import Path
import yaml
import pandas as pd

# This allows the script to import modules from the 'src' directory
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.universe_processor import process_universe_files
from src.utils.logging import LogManager

if __name__ == "__main__":
    """
    This script acts as a simple runner for the universe processing pipeline.
    The core logic is located in the `src/pipeline/universe_processor.py` module.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Universe Data Processor')
    parser.add_argument('--force-full-refresh', action='store_true',
                       help='Process ALL raw data ignoring state tracking')
    args = parser.parse_args()
    
    # Load logging configuration
    config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    log_config = config.get('logging', {})
    log_file = Path(__file__).parent.parent / 'logs' / 'universe_processor.log'
    
    # Setup logger
    logger = LogManager(
        log_file=str(log_file),
        log_level=log_config.get('level', 'INFO'),
        log_format=log_config.get('format')
    )
    
    logger.info("Starting universe processing pipeline...")
    if args.force_full_refresh:
        logger.info("🔄 FORCE FULL REFRESH: Processing ALL raw universe data")
    
    try:
        process_universe_files(logger, force_full_refresh=args.force_full_refresh)
        logger.info("Universe processing pipeline finished successfully.")
        # Export the processed Parquet to CSV
        parquet_path = Path(__file__).parent / 'universe.parquet'
        csv_path = Path(__file__).parent / 'processed data' / 'universe_processed.csv'
        df = pd.read_parquet(parquet_path)
        df.to_csv(csv_path, index=False)
        logger.info(f"Exported processed data to CSV at '{csv_path}'.")
    except Exception as e:
        logger.error("Universe processing pipeline failed.", exc=e) 