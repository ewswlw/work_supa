#!/usr/bin/env python
"""
Final schema-aware upload script.
This script fetches the actual database schema and only uploads matching columns.
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


def get_table_columns(client, table_name):
    """Get the actual columns that exist in the database table."""
    try:
        # Query a single row to get column names
        response = client.table(table_name).select("*").limit(1).execute()
        if response.data and len(response.data) > 0:
            return list(response.data[0].keys())
        else:
            # If no data, try to insert empty record and catch the error
            try:
                client.table(table_name).insert({}).execute()
            except Exception as e:
                error_msg = str(e)
                # Parse required columns from error message
                if "null value in column" in error_msg:
                    import re
                    match = re.search(r'null value in column "(\w+)"', error_msg)
                    if match:
                        # This gives us at least one column
                        return [match.group(1)]
            
            # Fallback to known schemas
            if table_name == 'universe':
                return ['cusip', 'benchmark_cusip', 'bloomberg_cusip', 'issuer_name', 
                        'issue_date', 'maturity_date', 'currency', 'issue_size', 
                        'coupon', 'payment_frequency', 'security_type', 'created_at']
            elif table_name == 'runs':
                return ['date', 'time', 'dealer', 'cusip', 'security', 'bid_price', 
                        'ask_price', 'bid_yield_to_convention', 'ask_yield_to_convention', 
                        'bid_spread', 'ask_spread', 'bid_size', 'ask_size', 'benchmark', 
                        'reference_benchmark', 'reference_security', 'subject', 
                        'sender_name', 'source', 'sector', 'ticker', 'currency', 
                        'keyword', 'created_at']
            elif table_name == 'portfolio':
                return ['date', 'account', 'portfolio', 'strategy', 'trade_group', 
                        'security_classification', 'security', 'security_type', 'cusip', 
                        'isin', 'underlying_security', 'underlying_cusip', 'underlying_isin', 
                        'company_symbol', 'currency', 'quantity', 'price', 'value', 
                        'value_pct_nav', 'face_value_outstanding', 'pct_of_outstanding', 
                        'coupon', 'maturity_date', 'maturity_bucket', 'credit_rating', 
                        'yield_to_mat', 'modified_duration', 'position_pvbp', 
                        'security_pvbp', 'position_cr01', 'security_cr01', 'created_at']
            else:
                return []
    except Exception as e:
        print(f"Could not fetch schema for {table_name}: {str(e)}")
        return []


def create_column_mapping():
    """Create mapping from Parquet columns to DB columns."""
    return {
        'universe': {
            'Cusip': 'cusip',
            'Benchmark Cusip': 'benchmark_cusip',
            'Bloomberg Cusip': 'bloomberg_cusip',
            'Notes': 'issuer_name',
            'Issue Date': 'issue_date',
            'Maturity Date': 'maturity_date',
            'Currency': 'currency',
            'Issue Size': 'issue_size',
            'Coupon': 'coupon',
            'Payment Frequency': 'payment_frequency',
            'Security Type': 'security_type'
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
            # Convert to string and truncate
            df[col] = df[col].fillna('').astype(str).str[:max_length]
            # Replace empty strings with None
            df[col] = df[col].replace('', None)
    return df


def schema_aware_upload():
    """Upload data only for columns that exist in database schema."""
    load_dotenv()
    
    # Initialize Supabase client
    client = create_client(
        os.getenv('SUPABASE_URL'), 
        os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    )
    
    # Setup logging
    log_file = Path("logs/schema_aware_upload.log")
    log_file.parent.mkdir(exist_ok=True)
    
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
    logger.info("Starting schema-aware upload")
    logger.info("="*80)
    
    # Get column mapping
    column_mapping = create_column_mapping()
    
    # Define tables with correct file paths
    tables = [
        {
            'name': 'universe',
            'file': 'universe/universe.parquet',
            'batch_size': 500,
            'critical_columns': ['cusip']
        },
        {
            'name': 'runs',
            'file': 'runs/combined_runs.parquet',
            'batch_size': 500,
            'critical_columns': ['date', 'cusip', 'dealer']
        },
        {
            'name': 'portfolio',
            'file': 'portfolio/portfolio.parquet',
            'batch_size': 500,
            'critical_columns': ['date', 'account', 'cusip']
        }
    ]
    
    results = {}
    
    for table_info in tables:
        table_name = table_info['name']
        file_path = table_info['file']
        batch_size = table_info['batch_size']
        critical_columns = table_info['critical_columns']
        
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
            
            # Get actual database columns
            db_columns = get_table_columns(client, table_name)
            if not db_columns:
                logger.error(f"Could not determine schema for {table_name}")
                results[table_name] = {'error': 'Could not determine schema'}
                continue
            
            logger.info(f"Database has {len(db_columns)} columns: {', '.join(sorted(db_columns))}")
            
            # Only keep columns that exist in database
            df_columns = set(df.columns)
            valid_columns = list(df_columns.intersection(db_columns))
            
            # Remove created_at if it exists (will be added by DB)
            if 'created_at' in valid_columns:
                valid_columns.remove('created_at')
            
            logger.info(f"Found {len(valid_columns)} matching columns: {', '.join(sorted(valid_columns))}")
            
            # Check for critical columns
            missing_critical = [col for col in critical_columns if col not in valid_columns]
            if missing_critical:
                logger.error(f"Missing critical columns: {missing_critical}")
                results[table_name] = {'error': f'Missing critical columns: {missing_critical}'}
                continue
            
            # Filter to valid columns only
            df = df[valid_columns]
            
            # Truncate strings
            df = truncate_strings(df)
            
            # Upload in batches
            total_uploaded = 0
            failed_rows = 0
            
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
                    logger.error(f"Failed batch {batch_num} ({len(batch)} rows): {error_msg[:200]}")
                    failed_rows += len(batch)
            
            results[table_name] = {
                'total': original_count,
                'uploaded': total_uploaded,
                'failed': failed_rows,
                'success_rate': (total_uploaded / original_count * 100) if original_count > 0 else 0
            }
            
            logger.info(f"Completed {table_name}: {total_uploaded}/{original_count} rows uploaded ({results[table_name]['success_rate']:.1f}% success)")
            
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            results[table_name] = {'error': str(e)}
    
    # Final summary
    logger.info("\n" + "="*80)
    logger.info("UPLOAD SUMMARY")
    logger.info("="*80)
    
    total_uploaded = 0
    total_rows = 0
    
    for table, result in results.items():
        if 'error' in result:
            logger.info(f"{table}: ERROR - {result['error']}")
        else:
            logger.info(f"{table}: {result['uploaded']}/{result['total']} rows ({result['success_rate']:.1f}% success)")
            total_uploaded += result['uploaded']
            total_rows += result['total']
    
    if total_rows > 0:
        overall_success = total_uploaded / total_rows * 100
        logger.info(f"\nOVERALL: {total_uploaded}/{total_rows} rows ({overall_success:.1f}% success)")
    
    # Verify final counts
    logger.info("\n" + "="*80)
    logger.info("VERIFYING FINAL DATABASE COUNTS")
    logger.info("="*80)
    
    for table_name in ['universe', 'portfolio', 'runs', 'g_spread']:
        try:
            # Use count API for accurate counts
            response = client.table(table_name).select('cusip' if table_name != 'g_spread' else 'date', count='exact').limit(1).execute()
            count = response.count if hasattr(response, 'count') else 0
            logger.info(f"{table_name}: {count:,} rows in database")
        except Exception as e:
            logger.error(f"{table_name}: Could not get count - {str(e)}")
    
    logger.info("\nUpload process completed!")


if __name__ == "__main__":
    schema_aware_upload() 