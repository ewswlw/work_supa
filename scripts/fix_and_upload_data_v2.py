#!/usr/bin/env python
"""
Fix data issues and upload to Supabase in the correct order (v2).
This script handles foreign key constraints, data type conversions, and schema mismatches.
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

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.config import ConfigManager
from src.utils.logging import LogManager


def fix_and_upload_data():
    """Fix data issues and upload to Supabase in the correct order."""
    # Setup logging
    log_manager = LogManager(
        log_file="logs/fix_and_upload_v2.log",
        log_level="INFO",
        log_format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    logger = log_manager.logger
    
    logger.info("=" * 80)
    logger.info("Starting data fix and upload process (v2)")
    logger.info("=" * 80)
    
    # Load environment variables
    load_dotenv()
    
    # Create Supabase client directly
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment")
        return False
    
    client = create_client(supabase_url, supabase_key)
    logger.info(f"Connected to Supabase at {supabase_url}")
    
    # Track results
    results = {}
    
    # Step 1: Upload universe table first (no foreign key dependencies)
    logger.info("\n" + "="*60)
    logger.info("Step 1: Uploading universe table")
    logger.info("="*60)
    
    try:
        df = pd.read_parquet("universe/universe.parquet")
        logger.info(f"Loaded {len(df)} rows from universe parquet")
        
        # Fix data types and column names
        df_fixed = pd.DataFrame()
        
        # Map columns based on the actual parquet structure
        df_fixed['date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        df_fixed['cusip'] = df['CUSIP'].astype(str)
        df_fixed['security'] = df.get('Security', pd.Series([''] * len(df))).astype(str)
        df_fixed['benchmark_cusip'] = df.get('Benchmark Cusip', pd.Series([''] * len(df))).astype(str)
        df_fixed['bloomberg_cusip'] = df.get('Bloomberg Cusip', pd.Series([''] * len(df))).astype(str)
        df_fixed['isin'] = None  # Not in parquet
        df_fixed['security_type'] = None  # Not in parquet
        df_fixed['currency'] = df.get('Currency', pd.Series(['USD'] * len(df))).astype(str)
        
        # Handle maturity_date with proper null handling
        if 'Worst Date' in df.columns:
            df_fixed['maturity_date'] = pd.to_datetime(df['Worst Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        else:
            df_fixed['maturity_date'] = None
            
        # Handle numeric columns properly
        if 'CPN' in df.columns:
            df_fixed['coupon'] = pd.to_numeric(df['CPN'], errors='coerce').fillna(0)
        else:
            df_fixed['coupon'] = 0
            
        df_fixed['credit_rating'] = df.get('Rating', pd.Series([''] * len(df))).astype(str)
        df_fixed['company_symbol'] = None  # Not in parquet
        df_fixed['issuer_name'] = df.get('Notes', pd.Series([''] * len(df))).astype(str)
        df_fixed['benchmark_maturity_date'] = None  # Not in parquet
        df_fixed['face_value_outstanding'] = 0  # Default to 0
        df_fixed['pct_of_outstanding'] = 0  # Default to 0
        df_fixed['created_at'] = datetime.now().isoformat()
        df_fixed['updated_at'] = datetime.now().isoformat()
        
        # Remove any rows with null cusip or date
        df_fixed = df_fixed.dropna(subset=['date', 'cusip'])
        
        # Upload in batches
        batch_size = 500
        total_uploaded = 0
        
        for i in range(0, len(df_fixed), batch_size):
            batch = df_fixed.iloc[i:i+batch_size]
            try:
                response = client.table('universe').insert(batch.to_dict('records')).execute()
                total_uploaded += len(batch)
                logger.info(f"Uploaded batch {i//batch_size + 1}: {len(batch)} rows")
            except Exception as e:
                logger.error(f"Failed to upload batch {i//batch_size + 1}: {str(e)}")
                # Continue with next batch
        
        logger.info(f"✅ Universe: Uploaded {total_uploaded} rows")
        results['universe'] = {"status": "success", "rows": total_uploaded}
        
    except Exception as e:
        logger.error(f"❌ Universe upload failed: {str(e)}")
        results['universe'] = {"status": "failed", "error": str(e)}
    
    # Step 2: Upload portfolio table
    logger.info("\n" + "="*60)
    logger.info("Step 2: Uploading portfolio table")
    logger.info("="*60)
    
    try:
        df = pd.read_parquet("portfolio/portfolio.parquet")
        logger.info(f"Loaded {len(df)} rows from portfolio parquet")
        
        # Fix data types and column names
        df_fixed = pd.DataFrame()
        
        # Map columns based on the actual parquet structure
        df_fixed['date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        df_fixed['cusip'] = df.get('CUSIP', pd.Series([''] * len(df))).astype(str)
        df_fixed['account'] = df.get('ACCOUNT', pd.Series([''] * len(df))).astype(str)
        df_fixed['portfolio'] = df.get('PORTFOLIO', pd.Series([''] * len(df))).astype(str)
        df_fixed['security'] = df.get('SECURITY', pd.Series([''] * len(df))).astype(str)
        df_fixed['security_type'] = df.get('SECURITY TYPE', pd.Series([''] * len(df))).astype(str)
        df_fixed['underlying_security'] = df.get('UNDERLYING SECURITY', pd.Series([''] * len(df))).astype(str)
        df_fixed['underlying_cusip'] = df.get('UNDERLYING CUSIP', pd.Series([''] * len(df))).astype(str)
        
        # Handle numeric columns properly
        numeric_columns = {
            'quantity': 'QUANTITY',
            'face_value': 'FACE VALUE OUTSTANDING',
            'price': 'PRICE',
            'value_pct_nav': 'VALUE PCT NAV',
            'modified_duration': 'MODIFIED DURATION',
            'position_cr01': 'POSITION CR01',
            'position_pvbp': 'POSITION PVBP',
            'security_pvbp': 'SECURITY PVBP',
            'security_cr01': 'SECURITY CR01',
            'yield_to_mat': 'YIELD TO MAT',
            'yield_calc': 'Yield',
            'benchmark_yield': 'Benchmark Yield',
            'oas_bloomberg': 'OAS (Bloomberg)',
            'spread_calculated': 'Sprd Calculated'
        }
        
        for target_col, source_col in numeric_columns.items():
            if source_col in df.columns:
                df_fixed[target_col] = pd.to_numeric(df[source_col], errors='coerce').fillna(0)
            else:
                df_fixed[target_col] = 0
        
        df_fixed['maturity_bucket'] = df.get('MATURITY BUCKET', pd.Series([''] * len(df))).astype(str)
        df_fixed['trade_group'] = df.get('TradeGroup Fixed', pd.Series([''] * len(df))).astype(str)
        df_fixed['strategy'] = df.get('STRATEGY', pd.Series([''] * len(df))).astype(str)
        df_fixed['security_classification'] = df.get('SECURITY CLASSIFICATION', pd.Series([''] * len(df))).astype(str)
        df_fixed['funding_status'] = df.get('Fuding Status', pd.Series([''] * len(df))).astype(str)
        df_fixed['created_at'] = datetime.now().isoformat()
        df_fixed['updated_at'] = datetime.now().isoformat()
        
        # Remove any rows with null cusip, date, or account
        df_fixed = df_fixed.dropna(subset=['date', 'cusip', 'account'])
        
        # Upload in batches
        batch_size = 500
        total_uploaded = 0
        
        for i in range(0, len(df_fixed), batch_size):
            batch = df_fixed.iloc[i:i+batch_size]
            try:
                response = client.table('portfolio').insert(batch.to_dict('records')).execute()
                total_uploaded += len(batch)
                logger.info(f"Uploaded batch {i//batch_size + 1}: {len(batch)} rows")
            except Exception as e:
                logger.error(f"Failed to upload batch {i//batch_size + 1}: {str(e)}")
                # Continue with next batch
        
        logger.info(f"✅ Portfolio: Uploaded {total_uploaded} rows")
        results['portfolio'] = {"status": "success", "rows": total_uploaded}
        
    except Exception as e:
        logger.error(f"❌ Portfolio upload failed: {str(e)}")
        results['portfolio'] = {"status": "failed", "error": str(e)}
    
    # Step 3: Upload runs table (depends on universe)
    logger.info("\n" + "="*60)
    logger.info("Step 3: Uploading runs table")
    logger.info("="*60)
    
    try:
        df = pd.read_parquet("runs/combined_runs.parquet")
        logger.info(f"Loaded {len(df)} rows from runs parquet")
        
        # Fix data types and column names
        df_fixed = pd.DataFrame()
        
        # Map columns based on the actual parquet structure
        df_fixed['date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        df_fixed['cusip'] = df.get('CUSIP', pd.Series([''] * len(df))).astype(str)
        df_fixed['dealer'] = df.get('Dealer', pd.Series([''] * len(df))).astype(str)
        df_fixed['security'] = df.get('Security', pd.Series([''] * len(df))).astype(str)
        df_fixed['reference_security'] = df.get('Reference Security', pd.Series([''] * len(df))).astype(str)
        df_fixed['trade_type'] = df.get('Subject', pd.Series([''] * len(df))).astype(str)
        
        # Handle numeric columns properly
        numeric_columns = {
            'quantity': 'Ask Size',
            'price': 'Ask Price',
            'yield_value': 'Ask Yield to Convention',
            'spread': 'Ask Interpolated Spread to Government',
            'duration': 'Bid Workout Risk',
            'dv01': 'Ask Discount Margin'
        }
        
        for target_col, source_col in numeric_columns.items():
            if source_col in df.columns:
                df_fixed[target_col] = pd.to_numeric(df[source_col], errors='coerce').fillna(0)
            else:
                df_fixed[target_col] = 0
                
        df_fixed['settlement_date'] = None  # Not in parquet
        df_fixed['trade_date'] = df_fixed['date']  # Use date as trade_date
        df_fixed['benchmark'] = df.get('Reference Benchmark', pd.Series([''] * len(df))).astype(str)
        df_fixed['maturity_date'] = None  # Not in parquet
        df_fixed['created_at'] = datetime.now().isoformat()
        
        # Remove any rows with null cusip, date, or dealer
        df_fixed = df_fixed.dropna(subset=['date', 'cusip', 'dealer'])
        
        # Only keep cusips that exist in universe table
        logger.info("Filtering runs to only include cusips that exist in universe table...")
        universe_cusips = client.table('universe').select('cusip').execute().data
        valid_cusips = set(row['cusip'] for row in universe_cusips)
        df_fixed = df_fixed[df_fixed['cusip'].isin(valid_cusips)]
        logger.info(f"Filtered to {len(df_fixed)} rows with valid cusips")
        
        # Upload in batches
        batch_size = 500
        total_uploaded = 0
        
        for i in range(0, len(df_fixed), batch_size):
            batch = df_fixed.iloc[i:i+batch_size]
            try:
                response = client.table('runs').insert(batch.to_dict('records')).execute()
                total_uploaded += len(batch)
                logger.info(f"Uploaded batch {i//batch_size + 1}: {len(batch)} rows")
            except Exception as e:
                logger.error(f"Failed to upload batch {i//batch_size + 1}: {str(e)}")
                # Continue with next batch
        
        logger.info(f"✅ Runs: Uploaded {total_uploaded} rows")
        results['runs'] = {"status": "success", "rows": total_uploaded}
        
    except Exception as e:
        logger.error(f"❌ Runs upload failed: {str(e)}")
        results['runs'] = {"status": "failed", "error": str(e)}
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info("UPLOAD SUMMARY")
    logger.info("="*80)
    
    for table, result in results.items():
        if result["status"] == "success":
            logger.info(f"✅ {table}: {result['rows']} rows uploaded")
        else:
            logger.info(f"❌ {table}: {result['error']}")
    
    # Check if all uploads were successful
    all_success = all(r["status"] == "success" for r in results.values())
    
    if all_success:
        logger.info("\n✅ All tables uploaded successfully!")
    else:
        logger.info("\n⚠️  Some tables failed to upload. Check the logs for details.")
    
    return all_success


if __name__ == "__main__":
    success = fix_and_upload_data()
    sys.exit(0 if success else 1) 