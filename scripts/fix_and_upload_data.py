#!/usr/bin/env python
"""
Fix data issues and upload to Supabase in the correct order.
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
        log_file="logs/fix_and_upload.log",
        log_level="INFO",
        log_format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    logger = log_manager.logger
    
    logger.info("=" * 80)
    logger.info("Starting data fix and upload process")
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
        df_fixed['security'] = df.get('Security', '').astype(str)
        df_fixed['benchmark_cusip'] = df.get('Benchmark Cusip', '').astype(str)
        df_fixed['bloomberg_cusip'] = df.get('Bloomberg Cusip', '').astype(str)
        df_fixed['isin'] = None  # Not in parquet
        df_fixed['security_type'] = None  # Not in parquet
        df_fixed['currency'] = df.get('Currency', 'USD').astype(str)
        df_fixed['maturity_date'] = pd.to_datetime(df.get('Worst Date'), errors='coerce').dt.strftime('%Y-%m-%d')
        df_fixed['coupon'] = pd.to_numeric(df.get('CPN', 0), errors='coerce').fillna(0)  # Convert to numeric
        df_fixed['credit_rating'] = df.get('Rating', '').astype(str)
        df_fixed['company_symbol'] = None  # Not in parquet
        df_fixed['issuer_name'] = df.get('Notes', '').astype(str)
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
    
    # Step 2: Upload security_mapping table (if needed)
    # Skip for now as we don't have data for it
    
    # Step 3: Upload portfolio table
    logger.info("\n" + "="*60)
    logger.info("Step 3: Uploading portfolio table")
    logger.info("="*60)
    
    try:
        df = pd.read_parquet("portfolio/portfolio.parquet")
        logger.info(f"Loaded {len(df)} rows from portfolio parquet")
        
        # Fix data types and column names
        df_fixed = pd.DataFrame()
        
        # Map columns based on the actual parquet structure
        df_fixed['date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        df_fixed['cusip'] = df.get('CUSIP', '').astype(str)
        df_fixed['account'] = df.get('ACCOUNT', '').astype(str)
        df_fixed['portfolio'] = df.get('PORTFOLIO', '').astype(str)
        df_fixed['security'] = df.get('SECURITY', '').astype(str)
        df_fixed['security_type'] = df.get('SECURITY TYPE', '').astype(str)
        df_fixed['underlying_security'] = df.get('UNDERLYING SECURITY', '').astype(str)
        df_fixed['underlying_cusip'] = df.get('UNDERLYING CUSIP', '').astype(str)
        df_fixed['quantity'] = pd.to_numeric(df.get('QUANTITY', 0), errors='coerce').fillna(0)
        df_fixed['face_value'] = pd.to_numeric(df.get('FACE VALUE OUTSTANDING', 0), errors='coerce').fillna(0)
        df_fixed['price'] = pd.to_numeric(df.get('PRICE', 0), errors='coerce').fillna(0)
        df_fixed['value_pct_nav'] = pd.to_numeric(df.get('VALUE PCT NAV', 0), errors='coerce').fillna(0)
        df_fixed['modified_duration'] = pd.to_numeric(df.get('MODIFIED DURATION', 0), errors='coerce').fillna(0)
        df_fixed['position_cr01'] = pd.to_numeric(df.get('POSITION CR01', 0), errors='coerce').fillna(0)
        df_fixed['position_pvbp'] = pd.to_numeric(df.get('POSITION PVBP', 0), errors='coerce').fillna(0)
        df_fixed['security_pvbp'] = pd.to_numeric(df.get('SECURITY PVBP', 0), errors='coerce').fillna(0)
        df_fixed['security_cr01'] = pd.to_numeric(df.get('SECURITY CR01', 0), errors='coerce').fillna(0)
        df_fixed['yield_to_mat'] = pd.to_numeric(df.get('YIELD TO MAT', 0), errors='coerce').fillna(0)
        df_fixed['yield_calc'] = pd.to_numeric(df.get('Yield', 0), errors='coerce').fillna(0)
        df_fixed['benchmark_yield'] = pd.to_numeric(df.get('Benchmark Yield', 0), errors='coerce').fillna(0)
        df_fixed['oas_bloomberg'] = pd.to_numeric(df.get('OAS (Bloomberg)', 0), errors='coerce').fillna(0)
        df_fixed['spread_calculated'] = pd.to_numeric(df.get('Sprd Calculated', 0), errors='coerce').fillna(0)
        df_fixed['maturity_bucket'] = df.get('MATURITY BUCKET', '').astype(str)
        df_fixed['trade_group'] = df.get('TradeGroup Fixed', '').astype(str)
        df_fixed['strategy'] = df.get('STRATEGY', '').astype(str)
        df_fixed['security_classification'] = df.get('SECURITY CLASSIFICATION', '').astype(str)
        df_fixed['funding_status'] = df.get('Fuding Status', '').astype(str)
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
    
    # Step 4: Upload runs table (depends on universe)
    logger.info("\n" + "="*60)
    logger.info("Step 4: Uploading runs table")
    logger.info("="*60)
    
    try:
        df = pd.read_parquet("runs/combined_runs.parquet")
        logger.info(f"Loaded {len(df)} rows from runs parquet")
        
        # Fix data types and column names
        df_fixed = pd.DataFrame()
        
        # Map columns based on the actual parquet structure
        df_fixed['date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        df_fixed['cusip'] = df.get('CUSIP', '').astype(str)
        df_fixed['dealer'] = df.get('Dealer', '').astype(str)
        df_fixed['security'] = df.get('Security', '').astype(str)
        df_fixed['reference_security'] = df.get('Reference Security', '').astype(str)
        df_fixed['trade_type'] = df.get('Subject', '').astype(str)
        df_fixed['quantity'] = pd.to_numeric(df.get('Ask Size', 0), errors='coerce').fillna(0)
        df_fixed['price'] = pd.to_numeric(df.get('Ask Price', 0), errors='coerce').fillna(0)
        df_fixed['yield_value'] = pd.to_numeric(df.get('Ask Yield to Convention', 0), errors='coerce').fillna(0)
        df_fixed['spread'] = pd.to_numeric(df.get('Ask Interpolated Spread to Government', 0), errors='coerce').fillna(0)
        df_fixed['duration'] = pd.to_numeric(df.get('Bid Workout Risk', 0), errors='coerce').fillna(0)
        df_fixed['dv01'] = pd.to_numeric(df.get('Ask Discount Margin', 0), errors='coerce').fillna(0)
        df_fixed['settlement_date'] = None  # Not in parquet
        df_fixed['trade_date'] = df_fixed['date']  # Use date as trade_date
        df_fixed['benchmark'] = df.get('Reference Benchmark', '').astype(str)
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
    
    # Step 5: Transform and upload g_spread table
    logger.info("\n" + "="*60)
    logger.info("Step 5: Transforming and uploading g_spread table")
    logger.info("="*60)
    
    try:
        df = pd.read_parquet("historical g spread/bond_g_sprd_time_series.parquet")
        logger.info(f"Loaded {len(df)} rows from g_spread parquet")
        logger.info(f"Columns: {list(df.columns)[:5]}...")
        
        # The g_spread data is in wide format - need to transform to long format
        # Each column (except DATE) is a security, and the values are the g_spread values
        
        # Melt the dataframe to convert from wide to long format
        id_vars = ['DATE']
        value_vars = [col for col in df.columns if col != 'DATE']
        
        df_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='security', value_name='g_spread')
        
        # Create the fixed dataframe
        df_fixed = pd.DataFrame()
        df_fixed['date'] = pd.to_datetime(df_long['DATE']).dt.strftime('%Y-%m-%d')
        df_fixed['security'] = df_long['security'].astype(str)
        df_fixed['g_spread'] = pd.to_numeric(df_long['g_spread'], errors='coerce')
        df_fixed['spread_change'] = 0  # Calculate if needed
        df_fixed['volume'] = 0  # Not available
        df_fixed['price'] = 0  # Not available
        df_fixed['yield_value'] = 0  # Not available
        df_fixed['duration'] = 0  # Not available
        df_fixed['credit_rating'] = ''  # Not available
        df_fixed['maturity_date'] = None  # Not available
        df_fixed['created_at'] = datetime.now().isoformat()
        
        # Remove rows with null g_spread values
        df_fixed = df_fixed.dropna(subset=['g_spread'])
        
        logger.info(f"Transformed to {len(df_fixed)} rows in long format")
        
        # Upload in batches
        batch_size = 500
        total_uploaded = 0
        
        for i in range(0, len(df_fixed), batch_size):
            batch = df_fixed.iloc[i:i+batch_size]
            try:
                response = client.table('g_spread').insert(batch.to_dict('records')).execute()
                total_uploaded += len(batch)
                logger.info(f"Uploaded batch {i//batch_size + 1}: {len(batch)} rows")
            except Exception as e:
                logger.error(f"Failed to upload batch {i//batch_size + 1}: {str(e)}")
                # Continue with next batch
        
        logger.info(f"✅ G_spread: Uploaded {total_uploaded} rows")
        results['g_spread'] = {"status": "success", "rows": total_uploaded}
        
    except Exception as e:
        logger.error(f"❌ G_spread upload failed: {str(e)}")
        results['g_spread'] = {"status": "failed", "error": str(e)}
    
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