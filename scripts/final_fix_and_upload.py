#!/usr/bin/env python
"""
Final comprehensive data upload to Supabase.
This script uploads data in the correct order and handles all data quality issues.
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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('logs/final_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fix_universe_data(df):
    """Fix all issues in universe data."""
    logger.info(f"Fixing universe data: {len(df)} rows")
    
    # Map columns
    column_mapping = {
        'Date': 'date',
        'CUSIP': 'cusip',
        'Cusip': 'cusip',
        'Security': 'security',
        'Security Name': 'security',
        'Notes': 'issuer_name',
        'Issuer Name': 'issuer_name',
        'Rating': 'credit_rating',
        'Maturity Date': 'maturity_date',
        'Maturity': 'maturity_date',
        'Coupon': 'coupon',
        'Issue Date': 'issue_date',
        'Benchmark Cusip': 'benchmark_cusip',
        'Bloomberg Cusip': 'bloomberg_cusip'
    }
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    # Keep only columns that exist in database
    db_columns = ['date', 'cusip', 'security', 'issuer_name', 'credit_rating', 
                  'maturity_date', 'coupon', 'issue_date', 'benchmark_cusip', 
                  'bloomberg_cusip', 'created_at']
    
    existing_columns = [col for col in db_columns if col in df.columns]
    df = df[existing_columns]
    
    # Add date if missing
    if 'date' not in df.columns:
        df['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
    
    # Fix data types and handle NaN/infinity
    # Handle numeric columns
    if 'coupon' in df.columns:
        # Replace infinity and NaN with None
        df['coupon'] = df['coupon'].replace([np.inf, -np.inf], np.nan)
        # Convert to float, handling errors
        df['coupon'] = pd.to_numeric(df['coupon'], errors='coerce')
        # Replace NaN with None for JSON compatibility
        df['coupon'] = df['coupon'].where(pd.notna(df['coupon']), None)
    
    # Handle date columns
    date_columns = ['date', 'maturity_date', 'issue_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d').where(pd.notna(df[col]), None)
    
    # Clean string columns - truncate to fit database limits
    string_columns = {
        'cusip': 20,
        'security': 100,
        'issuer_name': 100,
        'credit_rating': 10,
        'benchmark_cusip': 20,
        'bloomberg_cusip': 20
    }
    
    for col, max_len in string_columns.items():
        if col in df.columns:
            # Convert to string and truncate
            df[col] = df[col].astype(str).str[:max_len]
            # Replace 'nan' string with None
            df[col] = df[col].replace(['nan', 'None', ''], None)
    
    # Remove duplicates based on composite primary key
    df = df.drop_duplicates(subset=['date', 'cusip'])
    
    # Remove rows with invalid cusip
    df = df[df['cusip'].notna()]
    df = df[df['cusip'] != 'None']
    
    # Add created_at
    df['created_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    logger.info(f"Fixed universe data: {len(df)} rows remaining")
    return df

def fix_runs_data(df, valid_cusips):
    """Fix all issues in runs data."""
    logger.info(f"Fixing runs data: {len(df)} rows")
    
    # Map columns
    column_mapping = {
        'Date': 'date',
        'CUSIP': 'cusip',
        'Cusip': 'cusip',
        'Dealer': 'dealer',
        'Security': 'security',
        'Reference Security': 'reference_security',
        'Bid Price': 'price',
        'Bid Spread': 'spread',
        'Benchmark': 'benchmark'
    }
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    # Keep only columns that exist in database
    db_columns = ['date', 'cusip', 'dealer', 'security', 'reference_security',
                  'price', 'spread', 'benchmark', 'created_at']
    
    existing_columns = [col for col in db_columns if col in df.columns]
    df = df[existing_columns]
    
    # Add date if missing
    if 'date' not in df.columns:
        df['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
    else:
        # Fix date column
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d').where(pd.notna(df['date']), 
                                                               pd.to_datetime('today').strftime('%Y-%m-%d'))
    
    # Filter by valid cusips (foreign key constraint)
    df = df[df['cusip'].isin(valid_cusips)]
    logger.info(f"After filtering by valid cusips: {len(df)} rows")
    
    # Fix numeric columns
    numeric_columns = ['price', 'spread']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # Clean string columns
    string_columns = {
        'cusip': 20,
        'dealer': 100,
        'security': 100,
        'reference_security': 100,
        'benchmark': 100
    }
    
    for col, max_len in string_columns.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str[:max_len]
            df[col] = df[col].replace(['nan', 'None', ''], None)
    
    # Remove duplicates based on composite primary key
    df = df.drop_duplicates(subset=['date', 'cusip', 'dealer'])
    
    # Add created_at
    df['created_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    logger.info(f"Fixed runs data: {len(df)} rows remaining")
    return df

def fix_portfolio_data(df, valid_cusips):
    """Fix all issues in portfolio data."""
    logger.info(f"Fixing portfolio data: {len(df)} rows")
    
    # Map columns
    column_mapping = {
        'CUSIP': 'cusip',  # Use actual CUSIP column
        'ACCOUNT': 'account',
        'QUANTITY': 'quantity',
        'VALUE': 'face_value',  # VALUE column exists
        'VALUE PCT NAV': 'value_pct_nav'
    }
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    # Add required columns
    df['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
    df['portfolio'] = 'DEFAULT'
    
    # Filter by valid cusips
    df = df[df['cusip'].isin(valid_cusips)]
    logger.info(f"After filtering by valid cusips: {len(df)} rows")
    
    # Fix numeric columns
    numeric_columns = ['quantity', 'face_value', 'value_pct_nav']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Special handling for quantity - must be non-negative
            if col == 'quantity':
                # Replace negative values with absolute value
                df[col] = df[col].abs()
                # Replace NaN with 0
                df[col] = df[col].fillna(0)
            elif col == 'face_value':
                # Convert face_value to cents (integer)
                df[col] = (df[col] * 100).round().fillna(0).astype('int64')
            else:
                df[col] = df[col].where(pd.notna(df[col]), None)
    
    # Clean string columns
    string_columns = {
        'cusip': 20,
        'account': 100,
        'portfolio': 100
    }
    
    for col, max_len in string_columns.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str[:max_len]
            df[col] = df[col].replace(['nan', 'None', ''], None)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['date', 'cusip', 'account', 'portfolio'])
    
    # Add created_at
    df['created_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    # Select only columns that exist in database
    db_columns = ['date', 'cusip', 'account', 'portfolio', 'quantity', 
                  'face_value', 'value_pct_nav', 'created_at']
    existing_columns = [col for col in db_columns if col in df.columns]
    df = df[existing_columns]
    
    logger.info(f"Fixed portfolio data: {len(df)} rows remaining")
    return df

def upload_dataframe(client, table_name, df, batch_size=500):
    """Upload dataframe to Supabase in batches."""
    total_rows = len(df)
    uploaded_rows = 0
    failed_rows = 0
    
    logger.info(f"Uploading {total_rows} rows to {table_name}")
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx].copy()
        
        try:
            # Convert to records and clean
            batch_data = []
            for _, row in batch_df.iterrows():
                record = {}
                for col, val in row.items():
                    if pd.isna(val):
                        record[col] = None
                    elif isinstance(val, (np.integer, np.int64)):
                        record[col] = int(val)
                    elif isinstance(val, (np.floating, float)):
                        record[col] = float(val)
                    else:
                        record[col] = val
                batch_data.append(record)
            
            # Upload to Supabase
            response = client.table(table_name).insert(batch_data).execute()
            uploaded_rows += len(batch_df)
            logger.info(f"Uploaded batch {start_idx//batch_size + 1}: {len(batch_df)} rows")
            
        except Exception as e:
            failed_rows += len(batch_df)
            logger.error(f"Failed batch {start_idx//batch_size + 1}: {str(e)}")
            if batch_data:
                logger.debug(f"First record: {json.dumps(batch_data[0], indent=2)}")
    
    if total_rows > 0:
        logger.info(f"Completed {table_name}: {uploaded_rows}/{total_rows} uploaded ({uploaded_rows/total_rows*100:.1f}%)")
    else:
        logger.info(f"Completed {table_name}: No rows to upload")
    return uploaded_rows, failed_rows

def main():
    """Main function to fix and upload all data."""
    load_dotenv()
    
    # Initialize Supabase client
    client = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    )
    
    logger.info("="*80)
    logger.info("Starting final comprehensive data upload")
    logger.info("="*80)
    
    # 1. Fix and upload universe data first (DON'T CLEAR - APPEND ONLY)
    logger.info("\n1. Processing Universe data")
    universe_df = pd.read_parquet('universe/universe.parquet')
    universe_df = fix_universe_data(universe_df)
    
    # Get existing cusips to avoid duplicates
    try:
        existing_response = client.table('universe').select('date,cusip').execute()
        existing_keys = {(row['date'], row['cusip']) for row in existing_response.data}
        logger.info(f"Found {len(existing_keys)} existing universe records")
        
        # Filter out existing records
        universe_df['key'] = list(zip(universe_df['date'], universe_df['cusip']))
        universe_df = universe_df[~universe_df['key'].isin(existing_keys)]
        universe_df = universe_df.drop('key', axis=1)
        logger.info(f"After filtering existing: {len(universe_df)} new records to upload")
    except Exception as e:
        logger.warning(f"Could not check existing universe data: {e}")
    
    # Upload universe
    universe_uploaded, universe_failed = upload_dataframe(client, 'universe', universe_df)
    
    # Get ALL valid cusips (including previously uploaded)
    try:
        all_universe = client.table('universe').select('cusip').execute()
        valid_cusips = {row['cusip'] for row in all_universe.data if row['cusip']}
        logger.info(f"Total valid cusips in universe: {len(valid_cusips)}")
    except Exception as e:
        logger.error(f"Could not get valid cusips: {e}")
        valid_cusips = set(universe_df['cusip'].dropna().unique())
    
    # 2. Fix and upload runs data
    logger.info("\n2. Processing Runs data")
    runs_df = pd.read_parquet('runs/combined_runs.parquet')
    runs_df = fix_runs_data(runs_df, valid_cusips)
    
    # Get existing runs to avoid duplicates
    try:
        existing_response = client.table('runs').select('date,cusip,dealer').execute()
        existing_keys = {(row['date'], row['cusip'], row['dealer']) for row in existing_response.data}
        logger.info(f"Found {len(existing_keys)} existing runs records")
        
        # Filter out existing records
        runs_df['key'] = list(zip(runs_df['date'], runs_df['cusip'], runs_df['dealer']))
        runs_df = runs_df[~runs_df['key'].isin(existing_keys)]
        runs_df = runs_df.drop('key', axis=1)
        logger.info(f"After filtering existing: {len(runs_df)} new records to upload")
    except Exception as e:
        logger.warning(f"Could not check existing runs data: {e}")
    
    # Upload runs
    runs_uploaded, runs_failed = upload_dataframe(client, 'runs', runs_df)
    
    # 3. Fix and upload portfolio data
    logger.info("\n3. Processing Portfolio data")
    portfolio_df = pd.read_parquet('portfolio/portfolio.parquet')
    portfolio_df = fix_portfolio_data(portfolio_df, valid_cusips)
    
    # Get existing portfolio records to avoid duplicates
    try:
        existing_response = client.table('portfolio').select('date,cusip,account,portfolio').execute()
        existing_keys = {(row['date'], row['cusip'], row['account'], row['portfolio']) 
                        for row in existing_response.data}
        logger.info(f"Found {len(existing_keys)} existing portfolio records")
        
        # Filter out existing records
        portfolio_df['key'] = list(zip(portfolio_df['date'], portfolio_df['cusip'], 
                                      portfolio_df['account'], portfolio_df['portfolio']))
        portfolio_df = portfolio_df[~portfolio_df['key'].isin(existing_keys)]
        portfolio_df = portfolio_df.drop('key', axis=1)
        logger.info(f"After filtering existing: {len(portfolio_df)} new records to upload")
    except Exception as e:
        logger.warning(f"Could not check existing portfolio data: {e}")
    
    # Upload portfolio
    portfolio_uploaded, portfolio_failed = upload_dataframe(client, 'portfolio', portfolio_df)
    
    # 4. Get final counts
    try:
        universe_count = client.table('universe').select('cusip', count='exact').execute()
        runs_count = client.table('runs').select('cusip', count='exact').execute()
        portfolio_count = client.table('portfolio').select('cusip', count='exact').execute()
        g_spread_count = client.table('g_spread').select('security', count='exact').execute()
        
        total_universe = universe_count.count if hasattr(universe_count, 'count') else len(universe_count.data)
        total_runs = runs_count.count if hasattr(runs_count, 'count') else len(runs_count.data)
        total_portfolio = portfolio_count.count if hasattr(portfolio_count, 'count') else len(portfolio_count.data)
        total_g_spread = g_spread_count.count if hasattr(g_spread_count, 'count') else len(g_spread_count.data)
    except Exception as e:
        logger.error(f"Could not get final counts: {e}")
        total_universe = universe_uploaded
        total_runs = runs_uploaded
        total_portfolio = portfolio_uploaded
        total_g_spread = 2130523  # From previous upload
    
    # 5. Summary
    logger.info("\n" + "="*80)
    logger.info("FINAL UPLOAD SUMMARY")
    logger.info("="*80)
    logger.info(f"Universe: {universe_uploaded:,} new rows uploaded (Total in DB: {total_universe:,})")
    logger.info(f"Runs: {runs_uploaded:,} new rows uploaded (Total in DB: {total_runs:,})")
    logger.info(f"Portfolio: {portfolio_uploaded:,} new rows uploaded (Total in DB: {total_portfolio:,})")
    logger.info(f"G-Spread: Already has {total_g_spread:,} rows")
    logger.info(f"Grand Total: {total_universe + total_runs + total_portfolio + total_g_spread:,} rows in database")

if __name__ == "__main__":
    main() 