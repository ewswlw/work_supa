import sys
import argparse
from pathlib import Path
import yaml
import pandas as pd

# This allows the script to import modules from the 'src' directory
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.portfolio_processor import process_portfolio_files
from src.utils.logging import LogManager

if __name__ == "__main__":
    """
    This script acts as a simple runner for the portfolio processing pipeline.
    The core logic is located in the `src/pipeline/portfolio_processor.py` module.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Portfolio Data Processor')
    parser.add_argument('--force-full-refresh', action='store_true',
                       help='Process ALL raw data ignoring state tracking')
    args = parser.parse_args()
    # Load logging configuration
    config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    log_config = config.get('logging', {})
    log_file = Path(__file__).parent.parent / 'logs' / 'portfolio_processor.log'
    
    # Setup logger
    logger = LogManager(
        log_file=str(log_file),
        log_level=log_config.get('level', 'INFO'),
        log_format=log_config.get('format')
    )
    
    logger.info("Starting portfolio processing pipeline...")
    if args.force_full_refresh:
        logger.info("🔄 FORCE FULL REFRESH: Processing ALL raw portfolio data")
    
    try:
        final_df = process_portfolio_files(logger, force_full_refresh=args.force_full_refresh)
        logger.info("Portfolio processing pipeline finished successfully.")
        
        # Export the processed Parquet to CSV
        if final_df is not None:
            csv_path = Path(__file__).parent / 'processed data' / 'portfolio.csv'
            final_df.to_csv(csv_path, index=False)
            logger.info(f"Exported processed data to CSV at '{csv_path}'.")
            
            # Log blank CUSIP analysis
            blank_cusip = final_df[final_df['CUSIP'].isna() | (final_df['CUSIP'].astype(str).str.strip() == '')]
            logger.info(f"Analysis: Found {len(blank_cusip)} rows with blank CUSIP out of {len(final_df)} total rows")
            if len(blank_cusip) > 0:
                logger.warning(f"Blank CUSIP rows found:\n{blank_cusip[['Date', 'SECURITY', 'CUSIP']].head()}")
        else:
            logger.warning("No data was processed.")
            
    except Exception as e:
        logger.error("Portfolio processing pipeline failed.", exc=e) 