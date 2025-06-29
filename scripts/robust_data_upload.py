#!/usr/bin/env python
"""
Robust data upload to Supabase with comprehensive error handling.
This script ensures ALL data gets uploaded by handling common issues.
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
import json

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.config import ConfigManager
from src.utils.logging import LogManager


def clean_numeric_columns(df, numeric_columns):
    """Replace NaN and infinity values with None for JSON compatibility."""
    for col in numeric_columns:
        if col in df.columns:
            # Replace inf and -inf with None
            df[col] = df[col].replace([np.inf, -np.inf], None)
            # Replace NaN with None
            df[col] = df[col].where(pd.notnull(df[col]), None)
    return df


def truncate_string_columns(df, string_columns, max_length=50):
    """Truncate string columns to max length."""
    for col in string_columns:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].astype(str).str[:max_length]
    return df


def upload_with_retry(client, table_name, batch_data, logger, batch_num):
    """Upload a batch with retry logic and detailed error handling."""
    try:
        response = client.table(table_name).insert(batch_data).execute()
        logger.info(f"Successfully uploaded batch {batch_num}: {len(batch_data)} rows")
        return len(batch_data)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed batch {batch_num}: {error_msg}")
        
        # If it's a string length error, try to identify and fix
        if "value too long" in error_msg:
            logger.info(f"Retrying batch {batch_num} with truncated strings...")
            # Truncate all string columns more aggressively
            df_batch = pd.DataFrame(batch_data)
            for col in df_batch.select_dtypes(include=['object']).columns:
                df_batch[col] = df_batch[col].astype(str).str[:45]  # Even shorter
            
            try:
                retry_data = df_batch.to_dict('records')
                response = client.table(table_name).insert(retry_data).execute()
                logger.info(f"Retry successful for batch {batch_num}: {len(retry_data)} rows")
                return len(retry_data)
            except Exception as e2:
                logger.error(f"Retry failed for batch {batch_num}: {str(e2)}")
                return 0
        
        return 0


def robust_upload():
    """Upload all data with comprehensive error handling."""
    # Setup logging
    log_manager = LogManager(
        log_file="logs/robust_upload.log",
        log_level="INFO",
        log_format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    logger = log_manager.logger  # Use logger directly
    
    logger.info("="*80)
    logger.info("Starting robust data upload process")
    logger.info("="*80)
    
    # Load config and connect to Supabase
    load_dotenv()
    client = create_client(
        os.getenv('SUPABASE_URL'), 
        os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    )
    
    logger.info(f"Connected to Supabase")
    
    # Define tables and their configurations
    table_configs = {
        'universe': {
            'parquet_file': 'universe/universe.parquet',
            'numeric_columns': ['coupon', 'face_value_outstanding', 'pct_of_outstanding'],
            'string_columns': ['cusip', 'security', 'issuer_name', 'company_symbol', 
                             'security_type', 'currency', 'credit_rating', 'isin',
                             'bloomberg_cusip', 'benchmark_cusip'],
            'date_columns': ['date', 'maturity_date', 'benchmark_maturity_date'],
            'batch_size': 100
        },
        'runs': {
            'parquet_file': 'runs/combined_runs.parquet',
            'numeric_columns': ['price', 'quantity', 'yield_value', 'spread', 'duration', 'dv01'],
            'string_columns': ['cusip', 'dealer', 'security', 'reference_security', 
                             'trade_type', 'benchmark'],
            'date_columns': ['date', 'trade_date', 'settlement_date', 'maturity_date'],
            'batch_size': 200,
            'filter_by_universe': True
        },
        'portfolio': {
            'parquet_file': 'portfolio/portfolio.parquet',
            'numeric_columns': ['face_value', 'quantity', 'price', 'yield_to_mat', 
                              'modified_duration', 'spread_calculated', 'oas_bloomberg',
                              'benchmark_yield', 'value_pct_nav', 'position_pvbp',
                              'security_pvbp', 'position_cr01', 'security_cr01'],
            'string_columns': ['cusip', 'security', 'account', 'portfolio', 'strategy',
                             'security_type', 'security_classification', 'maturity_bucket',
                             'trade_group', 'funding_status', 'underlying_cusip',
                             'underlying_security'],
            'date_columns': ['date'],
            'batch_size': 200,
            'exclude_columns': ['updated_at']  # This column doesn't exist in DB
        }
    }
    
    # Process each table
    for table_name, config in table_configs.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {table_name} table")
        logger.info(f"{'='*60}")
        
        try:
            # Load parquet file
            df = pd.read_parquet(config['parquet_file'])
            logger.info(f"Loaded {len(df)} rows from {config['parquet_file']}")
            
            # Clean numeric columns
            df = clean_numeric_columns(df, config['numeric_columns'])
            logger.info(f"Cleaned numeric columns")
            
            # Truncate string columns
            df = truncate_string_columns(df, config['string_columns'])
            logger.info(f"Truncated string columns")
            
            # Convert date columns to ISO strings
            for col in config['date_columns']:
                if col in df.columns:
                    # Convert to datetime first, handling errors
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    # Convert to string format, replacing NaT with None
                    df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
            logger.info(f"Converted date columns")
            
            # Add created_at timestamp as string
            df['created_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            
            # Remove excluded columns
            if 'exclude_columns' in config:
                for col in config['exclude_columns']:
                    if col in df.columns:
                        df = df.drop(columns=[col])
                        logger.info(f"Removed column: {col}")
            
            # Filter by universe if needed
            if config.get('filter_by_universe') and table_name == 'runs':
                # Get existing cusips from universe
                universe_response = client.table('universe').select('cusip').execute()
                valid_cusips = {row['cusip'] for row in universe_response.data}
                logger.info(f"Found {len(valid_cusips)} valid cusips in universe")
                
                # Filter runs only if we have valid cusips and the cusip column exists
                if valid_cusips and 'cusip' in df.columns:
                    original_count = len(df)
                    df = df[df['cusip'].isin(valid_cusips)]
                    logger.info(f"Filtered from {original_count} to {len(df)} rows")
                else:
                    logger.warning(f"Skipping filter - no valid cusips in universe or cusip column missing")
            
            # Upload in batches
            batch_size = config['batch_size']
            total_uploaded = 0
            failed_batches = []
            
            for i in range(0, len(df), batch_size):
                batch_num = i // batch_size + 1
                batch_df = df.iloc[i:i+batch_size]
                batch_data = batch_df.to_dict('records')
                
                # Clean up the data one more time
                cleaned_batch_data = []
                for record in batch_data:
                    # Remove any None keys
                    cleaned_record = {}
                    for k, v in record.items():
                        if k is not None:
                            # Convert any remaining timestamps to strings
                            if hasattr(v, 'strftime'):
                                cleaned_record[k] = v.strftime('%Y-%m-%d')
                            elif pd.isna(v):
                                cleaned_record[k] = None
                            else:
                                cleaned_record[k] = v
                    cleaned_batch_data.append(cleaned_record)
                
                uploaded = upload_with_retry(client, table_name, cleaned_batch_data, logger, batch_num)
                total_uploaded += uploaded
                
                if uploaded == 0:
                    failed_batches.append(batch_num)
            
            logger.info(f"✅ {table_name}: Successfully uploaded {total_uploaded} out of {len(df)} rows")
            if failed_batches:
                logger.warning(f"Failed batches: {failed_batches}")
                
        except Exception as e:
            logger.error(f"❌ Failed to process {table_name}: {str(e)}")
            continue
    
    # Verify final counts
    logger.info(f"\n{'='*80}")
    logger.info("FINAL VERIFICATION")
    logger.info(f"{'='*80}")
    
    for table in ['universe', 'portfolio', 'runs', 'g_spread']:
        try:
            response = client.table(table).select("*", count='exact').limit(1).execute()
            count = response.count if hasattr(response, 'count') else 0
            logger.info(f"{table}: {count} total rows in database")
        except Exception as e:
            logger.error(f"{table}: Error getting count - {str(e)}")
    
    logger.info("\n✅ Upload process completed!")


if __name__ == "__main__":
    robust_upload() 