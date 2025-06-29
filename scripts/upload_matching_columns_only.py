#!/usr/bin/env python
"""
Upload data to Supabase using only columns that match the database schema.
This script maps Parquet columns to the actual database schema.
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
        logging.FileHandler('logs/matching_columns_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_schema():
    """Define the actual database schema based on database_design_specification.md"""
    return {
        'universe': {
            'columns': {
                'date': 'DATE',
                'cusip': 'VARCHAR(20)',
                'security': 'VARCHAR(100)',
                'benchmark_cusip': 'VARCHAR(20)',
                'bloomberg_cusip': 'VARCHAR(20)',
                'isin': 'VARCHAR(20)',
                'security_type': 'VARCHAR(50)',
                'currency': 'VARCHAR(3)',
                'maturity_date': 'DATE',
                'coupon': 'DECIMAL(10,6)',
                'credit_rating': 'VARCHAR(10)',
                'company_symbol': 'VARCHAR(20)',
                'issuer_name': 'VARCHAR(100)',
                'benchmark_maturity_date': 'DATE',
                'face_value_outstanding': 'BIGINT',
                'pct_of_outstanding': 'DECIMAL(10,8)',
                'created_at': 'TIMESTAMP',
                'updated_at': 'TIMESTAMP'
            },
            'primary_key': ['date', 'cusip'],
            'date_columns': ['date', 'maturity_date', 'benchmark_maturity_date'],
            'numeric_columns': ['coupon', 'face_value_outstanding', 'pct_of_outstanding']
        },
        'runs': {
            'columns': {
                'date': 'DATE',
                'cusip': 'VARCHAR(20)',
                'dealer': 'VARCHAR(100)',
                'security': 'VARCHAR(100)',
                'reference_security': 'VARCHAR(100)',
                'trade_type': 'VARCHAR(50)',
                'quantity': 'DECIMAL(18,4)',
                'price': 'DECIMAL(12,6)',
                'yield_value': 'DECIMAL(10,6)',
                'spread': 'DECIMAL(10,6)',
                'duration': 'DECIMAL(10,6)',
                'dv01': 'DECIMAL(12,6)',
                'settlement_date': 'DATE',
                'trade_date': 'DATE',
                'benchmark': 'VARCHAR(100)',
                'maturity_date': 'DATE',
                'created_at': 'TIMESTAMP'
            },
            'primary_key': ['date', 'cusip', 'dealer'],
            'date_columns': ['date', 'settlement_date', 'trade_date', 'maturity_date'],
            'numeric_columns': ['quantity', 'price', 'yield_value', 'spread', 'duration', 'dv01']
        },
        'portfolio': {
            'columns': {
                'date': 'DATE',
                'cusip': 'VARCHAR(20)',
                'account': 'VARCHAR(100)',
                'portfolio': 'VARCHAR(100)',
                'security': 'VARCHAR(100)',
                'security_type': 'VARCHAR(50)',
                'underlying_security': 'VARCHAR(100)',
                'underlying_cusip': 'VARCHAR(20)',
                'quantity': 'DECIMAL(18,4)',
                'face_value': 'BIGINT',
                'price': 'DECIMAL(12,6)',
                'value_pct_nav': 'DECIMAL(10,8)',
                'modified_duration': 'DECIMAL(10,6)',
                'position_cr01': 'DECIMAL(12,6)',
                'position_pvbp': 'DECIMAL(12,6)',
                'security_pvbp': 'DECIMAL(12,6)',
                'security_cr01': 'DECIMAL(12,6)',
                'yield_to_mat': 'DECIMAL(10,6)',
                'yield_calc': 'DECIMAL(10,6)',
                'benchmark_yield': 'DECIMAL(10,6)',
                'oas_bloomberg': 'DECIMAL(10,6)',
                'spread_calculated': 'DECIMAL(10,6)',
                'maturity_bucket': 'VARCHAR(50)',
                'trade_group': 'VARCHAR(50)',
                'strategy': 'VARCHAR(50)',
                'security_classification': 'VARCHAR(50)',
                'funding_status': 'VARCHAR(50)',
                'created_at': 'TIMESTAMP'
            },
            'primary_key': ['date', 'cusip', 'account', 'portfolio'],
            'date_columns': ['date'],
            'numeric_columns': ['quantity', 'face_value', 'price', 'value_pct_nav', 
                              'modified_duration', 'position_cr01', 'position_pvbp',
                              'security_pvbp', 'security_cr01', 'yield_to_mat',
                              'yield_calc', 'benchmark_yield', 'oas_bloomberg',
                              'spread_calculated']
        }
    }

def create_parquet_to_db_mapping():
    """Map Parquet column names to database column names - only columns that exist in DB."""
    return {
        'universe': {
            # Direct mappings
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
            'Issue Date': 'maturity_date',  # Using as proxy
            'Benchmark Cusip': 'benchmark_cusip',
            'Bloomberg Cusip': 'bloomberg_cusip'
        },
        'runs': {
            # Map only columns that exist in the DB schema
            'Date': 'date',
            'CUSIP': 'cusip',
            'Cusip': 'cusip',
            'Dealer': 'dealer',
            'Security': 'security',
            'Reference Security': 'reference_security',
            'Bid Price': 'price',  # Using bid price as the price
            'Ask Price': None,  # Not in DB schema
            'Bid Spread': 'spread',  # Using bid spread as the spread
            'Ask Spread': None,  # Not in DB schema
            'Benchmark': 'benchmark',
            # The following columns from Parquet don't exist in DB schema:
            # 'Bid Discount Margin', 'Ask Discount Margin', 'Bid Yield To Convention', etc.
        },
        'portfolio': {
            # Direct mappings
            'Date': 'date',
            'CUSIP': 'cusip',
            'Cusip': 'cusip',
            'SECURITY': 'cusip',  # In portfolio.parquet, SECURITY contains CUSIP
            'ACCOUNT': 'account',
            'Account': 'account',
            'PORTFOLIO': 'portfolio',
            'Portfolio': 'portfolio',
            'QUANTITY': 'quantity',
            'Quantity': 'quantity',
            'MARKET_VALUE': 'face_value',  # Using market value as face value
            'Market Value': 'face_value',
            'PERCENT_OF_PORTFOLIO': 'value_pct_nav',  # Using as proxy
            'Percent of Portfolio': 'value_pct_nav'
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
            
            # Rename columns based on mapping (only keep mapped columns)
            mapped_columns = {}
            for parquet_col, db_col in mapping.items():
                if db_col and parquet_col in df.columns:
                    mapped_columns[parquet_col] = db_col
            
            df = df.rename(columns=mapped_columns)
            
            # Keep only columns that exist in the database schema
            db_columns = list(schema_info['columns'].keys())
            existing_columns = [col for col in db_columns if col in df.columns]
            missing_columns = [col for col in db_columns if col not in df.columns and col not in ['created_at', 'updated_at']]
            
            logger.info(f"Found {len(existing_columns)} matching columns: {existing_columns}")
            if missing_columns:
                logger.warning(f"Missing columns in source data: {missing_columns}")
            
            # Select only existing columns
            df = df[existing_columns]
            
            # Handle composite primary keys
            primary_keys = schema_info.get('primary_key', [])
            
            # Add missing primary key columns with default values if needed
            if table_name == 'universe' and 'date' not in df.columns:
                # Use the Date from filename or current date
                df['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
            
            if table_name == 'runs' and 'date' not in df.columns:
                # Use current date as default
                df['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
            
            if table_name == 'portfolio':
                # Add missing columns with defaults
                if 'date' not in df.columns:
                    df['date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                if 'portfolio' not in df.columns:
                    df['portfolio'] = 'DEFAULT'
                if 'account' not in df.columns and 'ACCOUNT' in df.columns:
                    df['account'] = df['ACCOUNT']
            
            # Remove duplicates based on primary key
            if primary_keys:
                df = df.drop_duplicates(subset=[pk for pk in primary_keys if pk in df.columns])
                logger.info(f"After removing duplicates: {len(df)} rows")
            
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
            success_rate = (result['uploaded'] / result['total'] * 100) if result['total'] > 0 else 0
            logger.info(f"{table}: {result['uploaded']}/{result['total']} rows uploaded ({success_rate:.1f}% success rate)")

if __name__ == "__main__":
    main() 