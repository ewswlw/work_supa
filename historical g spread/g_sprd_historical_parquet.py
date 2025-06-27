import sys
from pathlib import Path
import yaml
import pandas as pd

# This allows the script to import modules from the 'src' directory
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.g_spread_processor import process_g_spread_files
from src.utils.logging import LogManager

if __name__ == "__main__":
    """
    This script acts as a simple runner for the G spread processing pipeline.
    The core logic is located in the `src/pipeline/g_spread_processor.py` module.
    """
    # Load logging configuration
    config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    log_config = config.get('logging', {})
    log_file = Path(__file__).parent.parent / 'logs' / 'g_spread_processor.log'
    
    # Setup logger
    logger = LogManager(
        log_file=str(log_file),
        log_level=log_config.get('level', 'INFO'),
        log_format=log_config.get('format')
    )
    
    logger.info("Starting G spread processing pipeline...")
    try:
        final_df = process_g_spread_files(logger)
        logger.info("G spread processing pipeline finished successfully.")
        
        # Additional analysis and reporting
        if final_df is not None:
            logger.info(f"Final processing summary:")
            logger.info(f"- Final DataFrame shape: {final_df.shape}")
            logger.info(f"- Date column coverage: {final_df['DATE'].notna().sum()} records")
            logger.info(f"- Memory usage: {final_df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            # Log basic statistics
            numeric_cols = final_df.select_dtypes(include=['number']).columns
            logger.info(f"- Numeric columns: {len(numeric_cols)}")
            
            missing_analysis = final_df.isnull().sum()
            high_missing = missing_analysis[missing_analysis > len(final_df) * 0.8]
            if len(high_missing) > 0:
                logger.warning(f"- Columns with >80% missing data: {len(high_missing)}")
            
        else:
            logger.warning("No data was processed.")
            
    except Exception as e:
        logger.error("G spread processing pipeline failed.", exc=e)
        raise 