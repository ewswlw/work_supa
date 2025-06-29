#!/usr/bin/env python
"""
Final comprehensive upload script that handles all data issues.
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


def create_column_mapping():
    """Create comprehensive mapping from Parquet columns to DB columns."""
    return {
        'universe': {
            'Benchmark Cusip': 'benchmark_cusip',
            'Bloomberg Cusip': 'bloomberg_cusip',
            'Cusip': 'cusip',
            'Notes': 'issuer_name',
            'Issue Date': 'issue_date',
            'Maturity Date': 'maturity_date',
            'Currency': 'currency',
            'Issue Size': 'issue_size',
            'Coupon': 'coupon',
            'Payment Frequency': 'payment_frequency',
            'Security Type': 'security_type',
            'Ticker': 'ticker',
            'Exchange': 'exchange',
            'Country': 'country',
            'Industry': 'industry',
            'Issuer': 'issuer'
        },
        'runs': {
            'Date': 'date',
            'Time': 'time',
            'Dealer': 'dealer',
            'CUSIP': 'cusip',
            'Security': 'security',
            'Bid Price': 'bid_price',
            'Ask Price': 'ask_price',
            'Bid Yield To Convention': 'bid_yield_to_convention',
            'Ask Yield To Convention': 'ask_yield_to_convention',
            'Bid Spread': 'bid_spread',
            'Ask Spread': 'ask_spread',
            'Bid Size': 'bid_size',
            'Ask Size': 'ask_size',
            'Benchmark': 'benchmark',
            'Reference Benchmark': 'reference_benchmark',
            'Reference Security': 'reference_security',
            'Subject': 'subject',
            'Sender Name': 'sender_name',
            'Source': 'source',
            'Sector': 'sector',
            'Ticker': 'ticker',
            'Currency': 'currency',
            'Keyword': 'keyword'
        },
        'portfolio': {
            'Date': 'date',
            'ACCOUNT': 'account',
            'PORTFOLIO': 'portfolio',
            'STRATEGY': 'strategy',
            'TRADE GROUP': 'trade_group',
            'SECURITY CLASSIFICATION': 'security_classification',
            'SECURITY': 'security',
            'SECURITY TYPE': 'security_type',
            'CUSIP': 'cusip',
            'ISIN': 'isin',
            'UNDERLYING SECURITY': 'underlying_security',
            'UNDERLYING CUSIP': 'underlying_cusip',
            'UNDERLYING ISIN': 'underlying_isin',
            'COMPANY SYMBOL': 'company_symbol',
            'CURRENCY': 'currency',
            'QUANTITY': 'quantity',
            'PRICE': 'price',
            'VALUE': 'value',
            'VALUE PCT NAV': 'value_pct_nav',
            'FACE VALUE OUTSTANDING': 'face_value_outstanding',
            'PCT OF OUTSTANDING': 'pct_of_outstanding',
            'COUPON': 'coupon',
            'MATURITY DATE': 'maturity_date',
            'MATURITY BUCKET': 'maturity_bucket',
            'CREDIT RATING': 'credit_rating',
            'YIELD TO MAT': 'yield_to_mat',
            'MODIFIED DURATION': 'modified_duration',
            'POSITION PVBP': 'position_pvbp',
            'SECURITY PVBP': 'security_pvbp',
            'POSITION CR01': 'position_cr01',
            'SECURITY CR01': 'security_cr01'
        }
    }


def fix_data_types(df):
    """Fix problematic data types in DataFrame."""
    # Convert categorical columns to strings
    for col in df.columns:
        if pd.api.types.is_categorical_dtype(df[col]):
            df[col] = df[col].astype(str)
    
    # Convert datetime columns to strings
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        elif 'date' in col.lower():
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            except:
                pass
    
    # Handle numeric columns
    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            # Replace inf and -inf with None
            df[col] = df[col].replace([np.inf, -np.inf], None)
            # Replace NaN with None
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    return df


def truncate_strings(df, max_length=50):
    """Truncate string columns to max length."""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str[:max_length]
    return df


def final_upload():
    """Perform final upload with all fixes."""
    load_dotenv()
    
    # Initialize Supabase client
    client = create_client(
        os.getenv('SUPABASE_URL'), 
        os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    )
    
    # Setup logging with UTF-8 encoding
    log_file = Path("logs/final_comprehensive_upload.log")
    log_file.parent.mkdir(exist_ok=True)
    
    # Configure logging with UTF-8
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # File handler with UTF-8
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
    logger.addHandler(console_handler)
    
    logger.info("="*80)
    logger.info("Starting final comprehensive upload")
    logger.info("="*80)
    
    # Get column mapping
    column_mapping = create_column_mapping()
    
    # Define tables with correct file paths
    tables = [
        {
            'name': 'universe',
            'file': 'universe/universe.parquet',
            'batch_size': 100
        },
        {
            'name': 'runs',
            'file': 'runs/combined_runs.parquet',  # Correct file name
            'batch_size': 100
        },
        {
            'name': 'portfolio',
            'file': 'portfolio/portfolio.parquet',
            'batch_size': 100
        }
    ]
    
    results = {}
    
    for table_info in tables:
        table_name = table_info['name']
        file_path = table_info['file']
        batch_size = table_info['batch_size']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {table_name} table")
        logger.info(f"{'='*60}")
        
        try:
            # Check if file exists
            if not Path(file_path).exists():
                logger.error(f"File not found: {file_path}")
                results[table_name] = {'error': 'File not found'}
                continue
            
            # Load data
            df = pd.read_parquet(file_path)
            original_count = len(df)
            logger.info(f"Loaded {original_count} rows from {file_path}")
            
            # Fix data types FIRST
            df = fix_data_types(df)
            logger.info("Fixed data types")
            
            # Map columns
            if table_name in column_mapping:
                mapping = column_mapping[table_name]
                rename_dict = {old: new for old, new in mapping.items() if old in df.columns}
                if rename_dict:
                    df = df.rename(columns=rename_dict)
                    logger.info(f"Mapped {len(rename_dict)} columns")
            
            # Only keep columns that exist in the database
            # For now, we'll just add created_at
            df['created_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            
            # Truncate strings
            df = truncate_strings(df)
            
            # Filter runs by existing universe cusips if uploading runs
            if table_name == 'runs' and 'cusip' in df.columns:
                try:
                    universe_response = client.table('universe').select('cusip').execute()
                    if universe_response.data:
                        valid_cusips = {row['cusip'] for row in universe_response.data}
                        original_runs = len(df)
                        df = df[df['cusip'].isin(valid_cusips)]
                        logger.info(f"Filtered runs from {original_runs} to {len(df)} based on universe")
                except:
                    logger.warning("Could not filter by universe cusips")
            
            # Upload in batches
            total_uploaded = 0
            failed_batches = []
            
            for i in range(0, len(df), batch_size):
                batch_num = i // batch_size + 1
                batch = df.iloc[i:i+batch_size]
                
                try:
                    # Convert to records and ensure JSON serializable
                    records = json.loads(batch.to_json(orient='records', date_format='iso'))
                    
                    # Upload
                    response = client.table(table_name).insert(records).execute()
                    total_uploaded += len(batch)
                    
                    if batch_num % 10 == 0:
                        logger.info(f"Progress: {total_uploaded}/{len(df)} rows uploaded")
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Check if it's a schema mismatch error
                    if 'Could not find' in error_msg and 'column' in error_msg:
                        # Extract the problematic column name
                        import re
                        match = re.search(r"Could not find the '(.+?)' column", error_msg)
                        if match:
                            bad_column = match.group(1)
                            logger.error(f"Column '{bad_column}' does not exist in {table_name} table schema")
                            # Remove the column and retry
                            if bad_column in batch.columns:
                                batch = batch.drop(columns=[bad_column])
                                try:
                                    records = json.loads(batch.to_json(orient='records', date_format='iso'))
                                    response = client.table(table_name).insert(records).execute()
                                    total_uploaded += len(batch)
                                    logger.info(f"Retry successful after removing '{bad_column}'")
                                    continue
                                except Exception as retry_error:
                                    logger.error(f"Retry failed: {str(retry_error)}")
                    
                    logger.error(f"Failed batch {batch_num}: {error_msg}")
                    failed_batches.append(batch_num)
            
            results[table_name] = {
                'total': original_count,
                'uploaded': total_uploaded,
                'failed_batches': failed_batches
            }
            
            logger.info(f"Completed {table_name}: {total_uploaded}/{original_count} rows uploaded")
            
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            results[table_name] = {'error': str(e)}
    
    # Final summary
    logger.info("\n" + "="*80)
    logger.info("UPLOAD SUMMARY")
    logger.info("="*80)
    
    for table, result in results.items():
        if 'error' in result:
            logger.info(f"{table}: ERROR - {result['error']}")
        else:
            success_rate = (result['uploaded'] / result['total'] * 100) if result['total'] > 0 else 0
            logger.info(f"{table}: {result['uploaded']}/{result['total']} rows ({success_rate:.1f}% success)")
            if result.get('failed_batches'):
                logger.info(f"  Failed batches: {len(result['failed_batches'])}")
    
    # Verify final counts
    logger.info("\n" + "="*80)
    logger.info("VERIFYING FINAL DATABASE COUNTS")
    logger.info("="*80)
    
    for table_name in ['universe', 'portfolio', 'runs', 'g_spread']:
        try:
            response = client.table(table_name).select('*', count='exact').limit(1).execute()
            count = response.count if hasattr(response, 'count') else len(response.data)
            logger.info(f"{table_name}: {count} rows in database")
        except Exception as e:
            logger.error(f"{table_name}: Could not get count - {str(e)}")
    
    logger.info("\nUpload process completed!")


if __name__ == "__main__":
    final_upload() 