#!/usr/bin/env python
"""
Upload data with correct schema mapping based on database design specification.
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
        logging.FileHandler('logs/correct_schema_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_schema():
    """Define the database schema based on database_design_specification.md"""
    return {
        'universe': {
            'columns': ['cusip', 'security_name', 'issuer_name', 'rating', 'maturity_date', 
                       'coupon', 'issue_date', 'benchmark_cusip', 'bloomberg_cusip', 
                       'created_at'],
            'primary_key': 'cusip',
            'date_columns': ['maturity_date', 'issue_date'],
            'numeric_columns': ['coupon']
        },
        'runs': {
            'columns': ['run_id', 'cusip', 'trade_date', 'dealer_name', 'price', 
                       'bid_price', 'ask_price', 'spread', 'g_spread', 'z_spread',
                       'ask_discount_margin', 'bid_discount_margin', 'created_at'],
            'primary_key': 'run_id',
            'date_columns': ['trade_date'],
            'numeric_columns': ['price', 'bid_price', 'ask_price', 'spread', 'g_spread', 
                               'z_spread', 'ask_discount_margin', 'bid_discount_margin']
        },
        'portfolio': {
            'columns': ['portfolio_id', 'cusip', 'account_name', 'position_date',
                       'quantity', 'market_value', 'percent_of_portfolio', 'created_at'],
            'primary_key': 'portfolio_id',
            'date_columns': ['position_date'],
            'numeric_columns': ['quantity', 'market_value', 'percent_of_portfolio']
        }
    }

def create_parquet_to_db_mapping():
    """Map Parquet column names to database column names."""
    return {
        'universe': {
            # Direct mappings
            'CUSIP': 'cusip',
            'Cusip': 'cusip',
            'Security': 'security_name',
            'Security Name': 'security_name',
            'Notes': 'issuer_name',
            'Issuer Name': 'issuer_name',
            'Rating': 'rating',
            'Maturity Date': 'maturity_date',
            'Maturity': 'maturity_date',
            'Coupon': 'coupon',
            'Issue Date': 'issue_date',
            'Benchmark Cusip': 'benchmark_cusip',
            'Bloomberg Cusip': 'bloomberg_cusip'
        },
        'runs': {
            # Direct mappings  
            'CUSIP': 'cusip',
            'Cusip': 'cusip',
            'Date': 'trade_date',
            'Trade Date': 'trade_date',
            'Dealer': 'dealer_name',
            'Dealer Name': 'dealer_name',
            'Price': 'price',
            'Bid Price': 'bid_price',
            'Ask Price': 'ask_price',
            'Spread': 'spread',
            'G Spread': 'g_spread',
            'G-Spread': 'g_spread',
            'Z Spread': 'z_spread',
            'Z-Spread': 'z_spread',
            'Ask Discount Margin': 'ask_discount_margin',
            'Bid Discount Margin': 'bid_discount_margin'
        },
        'portfolio': {
            # Direct mappings
            'CUSIP': 'cusip',
            'Cusip': 'cusip',
            'SECURITY': 'cusip',
            'ACCOUNT': 'account_name',
            'Account': 'account_name',
            'Account Name': 'account_name',
            'Date': 'position_date',
            'Position Date': 'position_date',
            'QUANTITY': 'quantity',
            'Quantity': 'quantity',
            'MARKET_VALUE': 'market_value',
            'Market Value': 'market_value',
            'PERCENT_OF_PORTFOLIO': 'percent_of_portfolio',
            'Percent of Portfolio': 'percent_of_portfolio'
        }
    }

def clean_dataframe(df, schema_info):
    """Clean dataframe according to schema requirements."""
    # Handle numeric columns - replace inf and -inf with None
    for col in schema_info.get('numeric_columns', []):
        if col in df.columns:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # Handle date columns
    for col in schema_info.get('date_columns', []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d').where(pd.notna(df[col]), None)
    
    # Add created_at
    df['created_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    # Convert any remaining timestamps to strings
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if any values are timestamps
            sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
            if sample and hasattr(sample, 'strftime'):
                df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
    
    return df

def upload_table_data(client, table_name, df, schema_info, batch_size=500):
    """Upload data to a specific table."""
    total_rows = len(df)
    uploaded_rows = 0
    failed_rows = 0
    
    logger.info(f"Starting upload of {total_rows} rows to {table_name}")
    
    # Upload in batches
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx].copy()
        
        try:
            # Convert to records
            batch_data = batch_df.to_dict('records')
            
            # Final cleanup of each record
            cleaned_batch = []
            for record in batch_data:
                cleaned_record = {}
                for k, v in record.items():
                    if pd.isna(v):
                        cleaned_record[k] = None
                    elif isinstance(v, (np.integer, np.floating)):
                        cleaned_record[k] = float(v)
                    else:
                        cleaned_record[k] = v
                cleaned_batch.append(cleaned_record)
            
            # Upload to Supabase
            response = client.table(table_name).insert(cleaned_batch).execute()
            uploaded_rows += len(batch_df)
            logger.info(f"Uploaded batch {start_idx//batch_size + 1}: {len(batch_df)} rows")
            
        except Exception as e:
            failed_rows += len(batch_df)
            logger.error(f"Failed to upload batch {start_idx//batch_size + 1}: {str(e)}")
            # Log first record for debugging
            if cleaned_batch:
                logger.debug(f"First record in failed batch: {json.dumps(cleaned_batch[0], indent=2)}")
    
    logger.info(f"Completed {table_name}: {uploaded_rows} uploaded, {failed_rows} failed")
    return uploaded_rows, failed_rows

def main():
    """Main upload function."""
    load_dotenv()
    
    # Initialize Supabase client
    client = create_client(
        os.getenv('SUPABASE_URL'),
        os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    )
    
    # Get schemas
    db_schema = get_database_schema()
    column_mapping = create_parquet_to_db_mapping()
    
    # Define file locations
    files = {
        'universe': 'universe/universe.parquet',
        'runs': 'runs/combined_runs.parquet',
        'portfolio': 'portfolio/portfolio.parquet'
    }
    
    results = {}
    
    for table_name, file_path in files.items():
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            continue
            
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {table_name} from {file_path}")
        logger.info(f"{'='*60}")
        
        try:
            # Read parquet file
            df = pd.read_parquet(file_path)
            logger.info(f"Loaded {len(df)} rows from {file_path}")
            
            # Map columns
            mapping = column_mapping.get(table_name, {})
            schema_info = db_schema.get(table_name, {})
            
            # Rename columns based on mapping
            df = df.rename(columns=mapping)
            
            # Keep only columns that exist in the database schema
            db_columns = schema_info.get('columns', [])
            existing_columns = [col for col in db_columns if col in df.columns]
            missing_columns = [col for col in db_columns if col not in df.columns and col != 'created_at']
            
            logger.info(f"Found {len(existing_columns)} matching columns: {existing_columns}")
            if missing_columns:
                logger.warning(f"Missing columns in source data: {missing_columns}")
            
            # Select only existing columns
            df = df[existing_columns]
            
            # Add primary key if missing
            primary_key = schema_info.get('primary_key')
            if primary_key and primary_key not in df.columns:
                if table_name == 'runs':
                    # Generate run_id
                    df['run_id'] = range(1, len(df) + 1)
                elif table_name == 'portfolio':
                    # Generate portfolio_id
                    df['portfolio_id'] = range(1, len(df) + 1)
            
            # Clean the dataframe
            df = clean_dataframe(df, schema_info)
            
            # Upload the data
            uploaded, failed = upload_table_data(client, table_name, df, schema_info)
            results[table_name] = {'uploaded': uploaded, 'failed': failed, 'total': len(df)}
            
        except Exception as e:
            logger.error(f"Error processing {table_name}: {str(e)}")
            results[table_name] = {'error': str(e)}
    
    # Print summary
    logger.info(f"\n{'='*60}")
    logger.info("UPLOAD SUMMARY")
    logger.info(f"{'='*60}")
    for table, result in results.items():
        if 'error' in result:
            logger.info(f"{table}: ERROR - {result['error']}")
        else:
            logger.info(f"{table}: {result['uploaded']}/{result['total']} rows uploaded ({result['failed']} failed)")

if __name__ == "__main__":
    main() 