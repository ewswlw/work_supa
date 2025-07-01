# DEPRECATED: All logic for bond_z_enhanced.parquet is now merged into g_z.py and bond_z.parquet. This script is obsolete and should not be used.
# All code below is commented out for archival purposes.

import sys
from pathlib import Path
import yaml
import pandas as pd

# This allows the script to import modules from the 'src' directory
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.g_spread_processor import process_g_spread_files
from src.utils.logging import LogManager

def merge_runs_monitor_with_bond_z(logger):
    """
    Merge runs monitor data with bond_z.parquet for both Security_1 and Security_2.
    
    Args:
        logger: Logger instance for tracking the process
        
    Returns:
        pd.DataFrame: Enhanced bond_z dataframe with runs monitor columns
    """
    logger.info("=== STARTING BOND Z ENHANCEMENT WITH RUNS MONITOR DATA ===")
    
    try:
        # Load bond_z data
        bond_z_path = Path(__file__).parent / 'bond_z.parquet'
        if not bond_z_path.exists():
            logger.error(f"Bond Z file not found: {bond_z_path}")
            return None
            
        bond_z = pd.read_parquet(bond_z_path)
        logger.info(f"Loaded bond_z data: {bond_z.shape}")
        
        # Load runs monitor data (prefer clean version to avoid duplicates)
        runs_monitor_clean_path = Path(__file__).parent.parent / 'runs' / 'run_monitor_clean.parquet'
        runs_monitor_path = Path(__file__).parent.parent / 'runs' / 'run_monitor.parquet'
        
        if runs_monitor_clean_path.exists():
            logger.info(f"Using clean runs monitor data: {runs_monitor_clean_path}")
            runs_monitor = pd.read_parquet(runs_monitor_clean_path)
        elif runs_monitor_path.exists():
            logger.warning("Clean runs monitor not found, using original (may contain duplicates)")
            runs_monitor = pd.read_parquet(runs_monitor_path)
            # Ensure no duplicates
            initial_count = len(runs_monitor)
            runs_monitor = runs_monitor.drop_duplicates(subset=['Security'], keep='first')
            if len(runs_monitor) != initial_count:
                logger.warning(f"Removed {initial_count - len(runs_monitor)} duplicate securities from runs monitor")
        else:
            logger.error(f"No runs monitor file found at: {runs_monitor_path}")
            return None
            
        logger.info(f"Loaded runs monitor data: {runs_monitor.shape}")
        
        # Define columns to merge from runs monitor
        target_columns = [
            'Best Bid', 'Best Offer', 'Bid/Offer', 
            'Dealer @ Best Bid', 'Dealer @ Best Offer',
            'Size @ Best Bid', 'Size @ Best Offer', 
            'G Spread', 'Keyword'
        ]
        
        # Verify all target columns exist in runs monitor
        missing_cols = [col for col in target_columns if col not in runs_monitor.columns]
        if missing_cols:
            logger.error(f"Missing columns in runs monitor: {missing_cols}")
            return None
            
        # Prepare runs monitor data for merging
        runs_data = runs_monitor[['Security'] + target_columns].copy()
        logger.info(f"Prepared runs monitor merge data: {runs_data.shape}")
        
        # Merge for Security_1
        logger.info("Merging runs monitor data for Security_1...")
        bond_z_enhanced = bond_z.merge(
            runs_data.rename(columns={col: f"{col}_1" for col in target_columns}),
            left_on='Security_1',
            right_on='Security',
            how='left',
            suffixes=('', '_runs1')
        )
        # Drop the extra Security column from the merge
        if 'Security' in bond_z_enhanced.columns:
            bond_z_enhanced = bond_z_enhanced.drop('Security', axis=1)
        
        logger.info(f"After Security_1 merge: {bond_z_enhanced.shape}")
        
        # Merge for Security_2
        logger.info("Merging runs monitor data for Security_2...")
        bond_z_enhanced = bond_z_enhanced.merge(
            runs_data.rename(columns={col: f"{col}_2" for col in target_columns}),
            left_on='Security_2',
            right_on='Security',
            how='left',
            suffixes=('', '_runs2')
        )
        # Drop the extra Security column from the merge
        if 'Security' in bond_z_enhanced.columns:
            bond_z_enhanced = bond_z_enhanced.drop('Security', axis=1)
            
        logger.info(f"After Security_2 merge: {bond_z_enhanced.shape}")
        
        # Log merge statistics
        logger.info("=== MERGE STATISTICS ===")
        for i, suffix in enumerate(['_1', '_2'], 1):
            col_name = f'Best Bid{suffix}'
            if col_name in bond_z_enhanced.columns:
                non_null_count = bond_z_enhanced[col_name].notna().sum()
                total_count = len(bond_z_enhanced)
                match_rate = (non_null_count / total_count) * 100
                logger.info(f"Security_{i} match rate: {non_null_count}/{total_count} ({match_rate:.1f}%)")
        
        # Log column summary
        new_columns = [col for col in bond_z_enhanced.columns if col.endswith(('_1', '_2')) and any(target in col for target in target_columns)]
        logger.info(f"Added {len(new_columns)} new columns: {new_columns[:5]}..." if len(new_columns) > 5 else f"Added columns: {new_columns}")
        
        # Save enhanced data
        output_path = Path(__file__).parent / 'bond_z_enhanced.parquet'
        bond_z_enhanced.to_parquet(output_path, index=False)
        logger.info(f"Saved enhanced bond_z to: {output_path}")
        
        # Also save as CSV for inspection
        csv_output_path = Path(__file__).parent / 'processed data' / 'bond_z_enhanced.csv'
        csv_output_path.parent.mkdir(exist_ok=True)
        bond_z_enhanced.to_csv(csv_output_path, index=False)
        logger.info(f"Saved enhanced bond_z CSV to: {csv_output_path}")
        
        # Log final summary
        logger.info("=== ENHANCEMENT COMPLETE ===")
        logger.info(f"Original bond_z shape: {bond_z.shape}")
        logger.info(f"Enhanced bond_z shape: {bond_z_enhanced.shape}")
        logger.info(f"Added columns: {bond_z_enhanced.shape[1] - bond_z.shape[1]}")
        
        return bond_z_enhanced
        
    except Exception as e:
        logger.error(f"Error enhancing bond_z with runs monitor data: {str(e)}", exc_info=True)
        return None

if __name__ == "__main__":
    """
    Enhanced script that:
    1. Runs the original G spread processing pipeline
    2. Merges runs monitor data with bond_z.parquet for both securities in each pair
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
    
    logger.info("Starting enhanced G spread processing pipeline...")
    try:
        # Run original G spread processing
        final_df = process_g_spread_files(logger)
        if final_df is not None:
            logger.info("G spread processing pipeline finished successfully.")
            
            # Additional analysis and reporting
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
            logger.warning("G spread processing failed, but continuing with bond_z enhancement...")
        
        # Enhance bond_z with runs monitor data
        enhanced_bond_z = merge_runs_monitor_with_bond_z(logger)
        
        if enhanced_bond_z is not None:
            logger.info("Bond Z enhancement completed successfully!")
            
            # Display sample of enhanced data
            logger.info("Sample of enhanced bond_z data:")
            sample_cols = ['Security_1', 'Security_2', 'Last_Spread', 'Best Bid_1', 'Best Offer_1', 'Best Bid_2', 'Best Offer_2']
            available_cols = [col for col in sample_cols if col in enhanced_bond_z.columns]
            if available_cols:
                logger.info(f"Sample columns: {available_cols}")
                logger.info(enhanced_bond_z[available_cols].head().to_string())
        else:
            logger.error("Bond Z enhancement failed!")
            
    except Exception as e:
        logger.error("Enhanced G spread processing pipeline failed.", exc_info=True)
        raise 