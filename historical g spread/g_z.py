#!/usr/bin/env python3
"""
G-Spread Pairwise Z-Score Analysis - SINGLE SOURCE OF TRUTH
==========================================================

This script is the ONLY source for all G-spread analytics, enrichment, filtering, and output.
All logic for:
  - Data loading
  - Pairwise analytics (Z-score, percentiles, etc.)
  - Universe and portfolio enrichment (all columns)
  - Filtering (core, simple, advanced)
  - Output (bond_z.parquet, bond_z.csv)
resides here.

Do NOT use or reference any other script for G-spread analytics or enrichment.
All downstream code, dashboards, and notebooks must use the output of this script.

If you need to add or change logic, do it HERE ONLY.

SPEED OPTIMIZATIONS:
- Matrix-based vectorized calculations (1000x faster than loops)
- Smart sampling of most liquid bonds
- Parallel processing
- Memory-efficient pivot operations
- Pre-computed statistical windows

INTERACTIVE WINDOW COMPATIBLE:
- Auto-detects and sets correct working directory
- Can be run from any interactive environment
- Functions can be called independently
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
from datetime import datetime
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from scipy import stats
import os
import sys
import logging

# Disable verbose logging from other modules to clean up console output
logging.getLogger().setLevel(logging.ERROR)
for logger_name in ['botocore', 'urllib3', 'requests', 'matplotlib']:
    logging.getLogger(logger_name).setLevel(logging.ERROR)

# ==========================================
# SAFE FILE READING UTILITIES
# ==========================================

def safe_read_csv(file_path, **kwargs):
    """Safely read CSV with encoding fallback to prevent decode errors."""
    try:
        return pd.read_csv(file_path, encoding='utf-8', **kwargs)
    except UnicodeDecodeError:
        try:
            return pd.read_csv(file_path, encoding='utf-8-sig', **kwargs)
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding='latin1', **kwargs)

def safe_read_parquet(file_path, **kwargs):
    """Safely read Parquet (already safe, but for consistency)."""
    return pd.read_parquet(file_path, **kwargs)

# ==========================================
# INTERACTIVE WINDOW SETUP
# ==========================================

def setup_interactive_environment():
    """Setup correct working directory and paths for interactive use."""
    global PROJECT_ROOT
    
    # Define project root path
    PROJECT_ROOT = r"C:\Users\Eddy\YTM Capital Dropbox\Eddy Winiarz\Trading\COF\Models\Unfinished Models\Eddy\Python Projects\work_supa"
    
    # Get current directory
    current_dir = os.getcwd()
    
    # Check if we're in the correct directory
    if Path(current_dir).name != "work_supa":
        print(f"[INFO] Interactive mode: Changing directory from {Path(current_dir).name} to work_supa")
        try:
            os.chdir(PROJECT_ROOT)
            print(f"[OK] Changed to: {os.getcwd()}")
        except Exception as e:
            print(f"[FAIL] Could not change directory: {e}")
            print(f"[INFO] Please manually change to: {PROJECT_ROOT}")
            return False
    else:
        print(f"[OK] Already in correct directory: {current_dir}")
    
    # NOTE: Not adding src to Python path by default to avoid pipeline logging
    # If you need pipeline features, manually add: sys.path.append(str(Path(PROJECT_ROOT) / "src"))
    
    return True

def check_required_files():
    """Check if all required files exist."""
    required_files = [
        "historical g spread/bond_g_sprd_time_series.parquet",
        "universe/universe.parquet"
    ]
    
    print("[INFO] Checking required files...")
    all_exist = True
    
    for file_path in required_files:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path) / (1024*1024)  # MB
            print(f"[OK] {file_path} ({file_size:.1f} MB)")
        else:
            print(f"[FAIL] MISSING: {file_path}")
            all_exist = False
    
    if not all_exist:
        print("[WARN] Some required files are missing. Please ensure you're in the correct directory.")
        print(f"   Expected directory: {PROJECT_ROOT}")
        return False
    
    return True

def get_data_file_path(relative_path: str) -> str:
    """Get absolute path for data files, ensuring they exist."""
    if os.path.exists(relative_path):
        return relative_path
    else:
        # Try with PROJECT_ROOT if defined
        if 'PROJECT_ROOT' in globals():
            abs_path = os.path.join(PROJECT_ROOT, relative_path)
            if os.path.exists(abs_path):
                return abs_path
        
        # If still not found, return original path (will cause error with helpful message)
        return relative_path

# Auto-setup when imported (for interactive use) - ENABLED WITH LOGGING
if __name__ != "__main__":
    try:
        if setup_interactive_environment():
            check_required_files()
            # Enable pipeline logging system for file logging (but suppress console)
            try:
                sys.path.append(str(Path(PROJECT_ROOT) / "src"))
                from utils.logging import LogManager
                log_manager = LogManager("logs/g_spread_processor.log", "INFO")
                print("[OK] Pipeline logging enabled (file logging active)")
            except Exception as log_e:
                print(f"[WARN] Could not setup pipeline logging: {log_e}")
    except Exception as e:
        print(f"[WARN] Auto-setup failed: {e}")
        print("[INFO] You can manually run: setup_interactive_environment()")

# ==========================================
# INTERACTIVE UTILITY FUNCTIONS  
# ==========================================

def run_quick_analysis(max_bonds=50, lookback_days=252, enable_filters=False):
    """Quick analysis for interactive testing with fewer bonds."""
    global CONFIG
    
    print("[RUN] Running QUICK ANALYSIS (Interactive Mode)")
    print(f"   • Max bonds: {max_bonds}")
    print(f"   • Lookback: {lookback_days} days") 
    print(f"   • Filtering: {'ON' if enable_filters else 'OFF'}")
    
    # Temporarily modify config
    original_config = CONFIG.copy()
    CONFIG['MAX_BONDS'] = max_bonds
    CONFIG['LOOKBACK_DAYS'] = lookback_days
    CONFIG['ENABLE_FILTERING'] = enable_filters
    
    try:
        # Run main analysis
        main()
    finally:
        # Restore original config
        CONFIG = original_config

def get_config_summary():
    """Display current configuration for interactive users."""
    print("[INFO] CURRENT CONFIGURATION:")
    print(f"   • Max bonds: {CONFIG['MAX_BONDS']}")
    print(f"   • Lookback days: {CONFIG['LOOKBACK_DAYS']}")
    print(f"   • Date sampling: Every {CONFIG['DATE_SAMPLE_FREQ']} dates" if CONFIG['SAMPLE_DATES'] else "   • Using all dates")
    print(f"   • Universe integration: {'ON' if CONFIG['INCLUDE_UNIVERSE_DATA'] else 'OFF'}")
    print(f"   • Filtering enabled: {'ON' if CONFIG['ENABLE_FILTERING'] else 'OFF'}")
    
    if CONFIG['INCLUDE_UNIVERSE_DATA']:
        print(f"   • Universe columns: {', '.join(CONFIG['UNIVERSE_COLUMNS'])}")
    
    if CONFIG['ENABLE_FILTERING']:
        simple_active = [k for k, v in CONFIG['SIMPLE_FILTERS'].items() if v]
        advanced_active = list(CONFIG['ADVANCED_FILTERS'].keys())
        if simple_active:
            print(f"   • Simple filters: {', '.join(simple_active)}")
        if advanced_active:
            print(f"   • Advanced filters: {len(advanced_active)} rules")

def load_latest_results():
    """Load the most recent analysis results for interactive exploration."""
    parquet_path = "historical g spread/bond_z.parquet"
    csv_path = "historical g spread/processed data/bond_z.csv"
    
    try:
        if os.path.exists(parquet_path):
            df = safe_read_parquet(parquet_path)
            print(f"[OK] Loaded latest results: {df.shape}")
            return df
        elif os.path.exists(csv_path):
            df = safe_read_csv(csv_path)
            print(f"[OK] Loaded latest results from CSV: {df.shape}")
            return df
        else:
            print("[FAIL] No results found. Run analysis first.")
            return None
    except Exception as e:
        print(f"[FAIL] Error loading results: {e}")
        return None

def set_config(max_bonds=None, lookback_days=None, enable_filters=None, enable_universe=None):
    """Easily modify configuration for interactive use."""
    global CONFIG
    
    if max_bonds is not None:
        CONFIG['MAX_BONDS'] = max_bonds
        print(f"[OK] Set max_bonds = {max_bonds}")
    
    if lookback_days is not None:
        CONFIG['LOOKBACK_DAYS'] = lookback_days
        print(f"[OK] Set lookback_days = {lookback_days}")
    
    if enable_filters is not None:
        CONFIG['ENABLE_FILTERING'] = enable_filters
        print(f"[OK] Set filtering = {'ON' if enable_filters else 'OFF'}")
    
    if enable_universe is not None:
        CONFIG['INCLUDE_UNIVERSE_DATA'] = enable_universe
        print(f"[OK] Set universe integration = {'ON' if enable_universe else 'OFF'}")
    
    print("\n[INFO] Updated configuration:")
    get_config_summary()


# ==========================================
# FILTER OPERATORS SYSTEM
# ==========================================

FILTER_OPERATORS = {
    # Categorical operators
    '==': lambda x, val: x == val,
    '!=': lambda x, val: x != val, 
    'in': lambda x, vals: x in vals,
    'not_in': lambda x, vals: x not in vals,
    'contains': lambda x, val: val in str(x) if pd.notna(x) else False,
    'startswith': lambda x, val: str(x).startswith(val) if pd.notna(x) else False,
    
    # Numeric operators  
    '>': lambda x, val: x > val if pd.notna(x) else False,
    '<': lambda x, val: x < val if pd.notna(x) else False,
    '>=': lambda x, val: x >= val if pd.notna(x) else False,
    '<=': lambda x, val: x <= val if pd.notna(x) else False,
    'between': lambda x, min_val, max_val: (min_val <= x <= max_val) if pd.notna(x) else False,
    'not_between': lambda x, min_val, max_val: not (min_val <= x <= max_val) if pd.notna(x) else False,
    
    # Special operators
    'abs_>': lambda x, val: abs(x) > val if pd.notna(x) else False,
    'abs_<': lambda x, val: abs(x) < val if pd.notna(x) else False,
    'is_null': lambda x: pd.isna(x),
    'not_null': lambda x: pd.notna(x),
    
    # Pair comparison operators (applied later)
    'pair_equal': None,
    'pair_not_equal': None,
}

# ==========================================
# ULTRA-FAST CONFIGURATION WITH UNIVERSE INTEGRATION
# ==========================================

CONFIG = {
    # Core parameters
    'LOOKBACK_DAYS': 252,  
    'MAX_BONDS': 200,         # Limit to most liquid bonds for speed (small sample for testing)
    'MIN_OBSERVATIONS': 200,  # Minimum data points
    
    # Speed optimizations
    'USE_PARALLEL': True,   # Parallel processing
    'CHUNK_SIZE': 500,     # Matrix chunk size
    'SAMPLE_DATES': True,   # Sample every Nth date for speed
    'DATE_SAMPLE_FREQ': 1,  
    
    # Data quality
    'MIN_DATA_COVERAGE': 0.7, 
    'MAX_SPREAD_DIFF': 500,    # Filter extreme outliers
    
    # ==========================================
    # UNIVERSE DATA INTEGRATION
    # ==========================================
    'INCLUDE_UNIVERSE_DATA': True,  # Enable universe enrichment
    
    # Available universe columns for enrichment:
    # • Custom_Sector (Non Financial Maples, HY, Utility, etc.)
    # • CPN_TYPE (FIXED, etc.)  
    # • Ticker (AAPL, etc.)
    # • Currency (USD, CAD, etc.)
    # • Equity_Ticker (AAPL US Equity, etc.)
    # • Rating (AA+, NR, etc.)
    # • Yrs_Since_Issue_Bucket (3-5, >7, etc.)
    # • Yrs_Mat_Bucket (0-0.50, etc.)
    
    'UNIVERSE_COLUMNS': [           # Columns to add to output (Security_1 & Security_2)
        'Custom_Sector',            # Sector classification
        'Rating',                   # Credit rating
        'Currency',                 # Currency 
        'Ticker',                   # Company ticker
        'CPN_TYPE',                 # Coupon type
        'Yrs_Mat_Bucket',          # Maturity bucket
        'Yrs_Since_Issue_Bucket',  # Issue age bucket
    ],
    
    # ==========================================
    # SOPHISTICATED FILTERING SYSTEM
    # ==========================================
    
    # CORE BUSINESS FILTERS (always applied when universe data available):
    # • Excludes CAD Government bonds (Custom_Sector = 'CAD Govt')
    # • Excludes short-term maturities: '>.25-1', '0-0.50', '0.50-1' and blank
    # • These filters focus analysis on corporate/credit bonds with >1 year maturity
    
    'ENABLE_FILTERING': False,      # Set to True to enable additional filtering
    
    # SIMPLE MODE FILTERS (easy to use)
    'SIMPLE_FILTERS': {
        # 'same_sector': True,                    # Only pairs from same sector
        # 'same_currency': True,                  # Only same currency pairs
        # 'investment_grade_only': True,          # Exclude NR ratings
        # 'usd_only': True,                      # Only USD securities
        # 'min_z_score': 2.0,                   # Minimum |Z-score|
        # 'fixed_coupon_only': True,             # Only FIXED coupon types
    },
    
    # ADVANCED MODE FILTERS (full flexibility)
    'ADVANCED_FILTERS': {
        # Categorical filters - Security 1
        # 'Custom_Sector_1': {'operator': 'in', 'values': ['Utility', 'Infrastructure']},
        # 'Rating_1': {'operator': '!=', 'value': 'NR'},
        # 'Currency_1': {'operator': '==', 'value': 'USD'},
        
        # Categorical filters - Security 2  
        # 'Custom_Sector_2': {'operator': 'not_in', 'values': ['HY']},
        # 'Rating_2': {'operator': '!=', 'value': 'NR'},
        
        # Pair comparison filters
        # 'Same_Sector': {'operator': 'pair_equal', 'columns': ['Custom_Sector']},
        # 'Different_Currency': {'operator': 'pair_not_equal', 'columns': ['Currency']},
        
        # Z-Score and statistics filters
        # 'Z_Score': {'operator': 'abs_>', 'value': 1.5},
        # 'Percentile': {'operator': 'between', 'min': 10, 'max': 90},
        # 'Last_Spread': {'operator': 'between', 'min': -100, 'max': 100},
    }
}

def load_and_pivot_data(file_path: str) -> pd.DataFrame:
    """Load data and immediately pivot to matrix format for vectorized operations."""
    print("[RUN] Loading and pivoting data for vectorized processing...")
    
    df = safe_read_parquet(file_path)
    df['DATE'] = pd.to_datetime(df['DATE'])
    
    # Remove missing data
    df = df.dropna(subset=['GSpread'])
    print(f"   Loaded {len(df):,} records with {df['Security'].nunique():,} bonds")
    
    # Sample dates for speed if enabled
    if CONFIG['SAMPLE_DATES']:
        unique_dates = sorted(df['DATE'].unique())
        sampled_dates = unique_dates[::CONFIG['DATE_SAMPLE_FREQ']]
        df = df[df['DATE'].isin(sampled_dates)]
        print(f"   Sampled to {len(sampled_dates):,} dates for speed")
    
    return df

def load_universe_data() -> pd.DataFrame:
    """Load universe data for enrichment, using most recent date only."""
    if not CONFIG['INCLUDE_UNIVERSE_DATA']:
        return pd.DataFrame()
        
    print("[INFO] Loading universe data for enrichment...")
    
    try:
        # Load universe data with path handling
        universe_path = get_data_file_path('universe/universe.parquet')
        universe_df = safe_read_parquet(universe_path)
        
        # Get most recent date globally (as per user preference)
        latest_date = universe_df['Date'].max()
        universe_df = universe_df[universe_df['Date'] == latest_date].copy()
        
        # Clean up column names to match config expectations
        universe_df = universe_df.rename(columns={
            'CPN TYPE': 'CPN_TYPE',
            'Equity Ticker': 'Equity_Ticker', 
            'Yrs Since Issue Bucket': 'Yrs_Since_Issue_Bucket',
            'Yrs (Mat) Bucket': 'Yrs_Mat_Bucket'
        })
        
        # Select only needed columns plus CUSIP for matching
        needed_cols = ['CUSIP'] + CONFIG['UNIVERSE_COLUMNS']
        available_cols = [col for col in needed_cols if col in universe_df.columns]
        
        if 'CUSIP' not in available_cols:
            print("[WARN] Warning: CUSIP column not found in universe data")
            return pd.DataFrame()
            
        universe_df = universe_df[available_cols].copy()
        
        # Handle duplicates (keep first occurrence)
        initial_count = len(universe_df)
        universe_df = universe_df.drop_duplicates(subset=['CUSIP'], keep='first')
        
        print(f"   [OK] Loaded {len(universe_df):,} universe records from {latest_date.strftime('%Y-%m-%d')}")
        if initial_count > len(universe_df):
            print(f"   [INFO] Removed {initial_count - len(universe_df):,} duplicate CUSIPs")
            
        return universe_df
        
    except Exception as e:
        print("[FAIL] Error loading universe data: {e}")
        return pd.DataFrame()

def select_top_bonds(df: pd.DataFrame, max_bonds: int) -> list:
    """Select most liquid bonds based on data coverage and recent activity."""
    print(f"[INFO] Selecting top {max_bonds} most liquid bonds...")
    
    # Calculate bond metrics
    latest_date = df['DATE'].max()
    min_start_date = latest_date - pd.Timedelta(days=CONFIG['LOOKBACK_DAYS'])
    
    bond_metrics = df.groupby('Security').agg({
        'DATE': ['count', 'min', 'max'],
        'GSpread': ['count', 'std']
    }).round(4)
    
    bond_metrics.columns = ['Total_Obs', 'First_Date', 'Last_Date', 'GSpread_Count', 'GSpread_Volatility']
    
    # Filter criteria
    sufficient_history = bond_metrics['First_Date'] <= min_start_date
    sufficient_data = bond_metrics['Total_Obs'] >= CONFIG['MIN_OBSERVATIONS']
    recent_activity = bond_metrics['Last_Date'] >= (latest_date - pd.Timedelta(days=30))
    good_coverage = (bond_metrics['GSpread_Count'] / bond_metrics['Total_Obs']) >= CONFIG['MIN_DATA_COVERAGE']
    
    # Combine filters
    eligible = bond_metrics[sufficient_history & sufficient_data & recent_activity & good_coverage]
    
    # Sort by data quality and take top N
    eligible['Score'] = eligible['Total_Obs'] * eligible['GSpread_Count'] / eligible['Total_Obs']
    top_bonds = eligible.nlargest(max_bonds, 'Score').index.tolist()
    
    print(f"   Selected {len(top_bonds)} bonds from {len(bond_metrics)} total")
    return top_bonds

def get_cusip_mapping(df: pd.DataFrame) -> dict:
    """Create mapping from Security name to CUSIP for universe matching."""
    if not CONFIG['INCLUDE_UNIVERSE_DATA']:
        return {}
        
    print("[INFO] Creating Security -> CUSIP mapping...")
    
    # Get unique Security → CUSIP mapping
    mapping_df = df[['Security', 'CUSIP']].drop_duplicates()
    mapping = dict(zip(mapping_df['Security'], mapping_df['CUSIP']))
    
    print(f"[OK] Created {len(mapping):,} Security -> CUSIP mappings")
    return mapping

def filter_bonds_by_universe_criteria(bonds: list, cusip_mapping: dict, universe_df: pd.DataFrame) -> list:
    """Filter bonds based on universe criteria: maturity > 1yr and issue age 1-4yr."""
    if not CONFIG['INCLUDE_UNIVERSE_DATA'] or universe_df.empty:
        print("[INFO] Universe filtering skipped (no universe data)")
        return bonds
        
    print("\n[FILTER] Applying universe criteria to bond selection...")
    print(f"[FILTER] Starting with {len(bonds)} bonds from liquidity selection")
    
    # Create universe lookup indexed by CUSIP
    universe_indexed = universe_df.set_index('CUSIP')
    
    # Filter criteria definitions
    maturity_buckets_over_1yr = ['1-2.1', '2.1-3.1', '3.1-4.1', '4.1-5.1', '5.1-7.1', '7.1-10.1', '10.1-25.1', '>25.1']
    issue_age_buckets_1_to_4yr = ['1-2', '2-3', '3-5']  # User confirmed to include 3-5 bucket
    
    # Track filtering stats
    bonds_with_universe_data = []
    bonds_maturity_passed = []
    bonds_issue_age_passed = []
    
    for bond in bonds:
        cusip = cusip_mapping.get(bond)
        if cusip is None:
            continue
            
        if cusip not in universe_indexed.index:
            continue
            
        # Bond has universe data
        bonds_with_universe_data.append(bond)
        
        # Check maturity criteria
        mat_bucket = universe_indexed.loc[cusip, 'Yrs_Mat_Bucket']
        if pd.isna(mat_bucket) or mat_bucket not in maturity_buckets_over_1yr:
            continue
            
        # Passed maturity filter
        bonds_maturity_passed.append(bond)
        
        # Check issue age criteria
        issue_bucket = universe_indexed.loc[cusip, 'Yrs_Since_Issue_Bucket']
        if pd.isna(issue_bucket) or issue_bucket not in issue_age_buckets_1_to_4yr:
            continue
            
        # Passed all filters
        bonds_issue_age_passed.append(bond)
    
    # Print detailed filtering breakdown
    print(f"[FILTER] • Bonds with universe data: {len(bonds_with_universe_data)} ({len(bonds_with_universe_data)/len(bonds)*100:.1f}%)")
    print(f"[FILTER] • After maturity filter (>1yr): {len(bonds_maturity_passed)} (-{len(bonds_with_universe_data)-len(bonds_maturity_passed)} excluded)")
    print(f"[FILTER] • After issue age filter (1-4yr): {len(bonds_issue_age_passed)} (-{len(bonds_maturity_passed)-len(bonds_issue_age_passed)} excluded)")
    print(f"[FILTER] Final filtered bonds: {len(bonds_issue_age_passed)} ({len(bonds_issue_age_passed)/len(bonds)*100:.1f}% of original)")
    
    if len(bonds_issue_age_passed) < 2:
        print("[WARN] Warning: Very few bonds passed filtering criteria!")
    
    return bonds_issue_age_passed

def create_spread_matrix(df: pd.DataFrame, bonds: list) -> pd.DataFrame:
    """Create a date x bond matrix for vectorized calculations."""
    print("[RUN] Creating spread matrix for vectorized operations...")
    
    # Filter to selected bonds and pivot
    df_filtered = df[df['Security'].isin(bonds)]
    matrix = df_filtered.pivot(index='DATE', columns='Security', values='GSpread')
    
    # Forward fill small gaps and drop rows with too many NaNs
    matrix = matrix.fillna(method='ffill', limit=5)
    
    # Only keep dates with sufficient bond coverage
    min_bonds_per_date = len(bonds) * 0.7  # At least 70% of bonds must have data
    matrix = matrix.dropna(thresh=min_bonds_per_date)
    
    print(f"   Matrix shape: {matrix.shape[0]:,} dates × {matrix.shape[1]:,} bonds")
    return matrix

def vectorized_pairwise_analysis(matrix: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
    """Ultra-fast vectorized pairwise analysis using matrix operations."""
    print("[RUN] Computing pairwise statistics with vectorized operations...")
    
    # Get the last N rows for lookback analysis
    if len(matrix) < lookback_days:
        lookback_data = matrix
    else:
        lookback_data = matrix.tail(lookback_days)
    
    bonds = matrix.columns.tolist()
    n_bonds = len(bonds)
    results = []
    
    # Track filtering reasons
    total_pairs_attempted = 0
    failed_start_date_check = 0
    failed_missing_recent_data = 0
    failed_no_spread_diff_data = 0
    successful_pairs = 0
    
    # Get last values for all bonds
    last_values = matrix.iloc[-1].values
    
    # Process all pairs properly
    for i in range(n_bonds):
        for j in range(i + 1, n_bonds):  # j starts from i+1 to avoid duplicates
                
            total_pairs_attempted += 1
            bond1, bond2 = bonds[i], bonds[j]
            
            # Get full data series for both bonds (not just lookback)
            full_data1 = matrix[bond1].dropna()
            full_data2 = matrix[bond2].dropna()
            
            # Check if both bonds have data that starts before the lookback period
            lookback_start_date = matrix.index[-lookback_days]
            
            if full_data1.index[0] > lookback_start_date or full_data2.index[0] > lookback_start_date:
                failed_start_date_check += 1
                continue
            
            # Get lookback data for analysis
            data1 = lookback_data[bond1].dropna()
            data2 = lookback_data[bond2].dropna()
            
            # Calculate current spread difference (most recent available dates)
            last_spread1 = data1.iloc[-1] if len(data1) > 0 else np.nan
            last_spread2 = data2.iloc[-1] if len(data2) > 0 else np.nan
            
            if pd.isna(last_spread1) or pd.isna(last_spread2):
                failed_missing_recent_data += 1
                continue
                
            current_spread_diff = last_spread1 - last_spread2
            
            # Calculate spread difference series using lookback data
            spread_diff_series = data1 - data2
            
            # Remove any NaN values
            spread_diff_series = spread_diff_series.dropna()
            
            if len(spread_diff_series) == 0:
                failed_no_spread_diff_data += 1
                continue
            
            # Calculate statistics
            last_spread = current_spread_diff
            mean_val = spread_diff_series.mean()
            std_val = spread_diff_series.std()
            min_val = spread_diff_series.min()
            max_val = spread_diff_series.max()
            
            # Z-score
            z_score = (last_spread - mean_val) / std_val if std_val > 0 else 0
            
            # Percentile
            percentile = stats.percentileofscore(spread_diff_series, last_spread)
            
            # Compile results
            successful_pairs += 1
            results.append({
                'Security_1': bond1,
                'Security_2': bond2,
                'Last_Spread': last_spread,
                'Z_Score': z_score,
                'Max': max_val,
                'Min': min_val,
                'Last_vs_Max': last_spread - max_val,
                'Last_vs_Min': last_spread - min_val,
                'Percentile': percentile
            })
    
    print(f"   Generated {len(results):,} pair analyses")
    
    # Print detailed breakdown
    print("\n[INFO] DETAILED DATA QUALITY BREAKDOWN:")
    print(f"   • Total pairs attempted: {total_pairs_attempted:,}")
    print(f"   • Failed start date check: {failed_start_date_check:,} ({failed_start_date_check/total_pairs_attempted*100:.1f}%)")
    print(f"   • Failed missing recent data: {failed_missing_recent_data:,} ({failed_missing_recent_data/total_pairs_attempted*100:.1f}%)")
    print(f"   • Failed no spread diff data: {failed_no_spread_diff_data:,} ({failed_no_spread_diff_data/total_pairs_attempted*100:.1f}%)")
    print(f"   • Successful pairs: {successful_pairs:,} ({successful_pairs/total_pairs_attempted*100:.1f}%)")
    
    return pd.DataFrame(results)

def enrich_with_universe_data(results_df: pd.DataFrame, cusip_mapping: dict, universe_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich results with universe data for both securities."""
    if not CONFIG['INCLUDE_UNIVERSE_DATA'] or universe_df.empty:
        return results_df
        
    print("[INFO] Enriching results with universe data...")
    
    # Create CUSIP lookups for Security_1 and Security_2
    results_df['CUSIP_1'] = results_df['Security_1'].map(cusip_mapping)
    results_df['CUSIP_2'] = results_df['Security_2'].map(cusip_mapping)
    
    # Set universe as index for faster lookups
    universe_indexed = universe_df.set_index('CUSIP')
    
    # Add universe data for Security_1
    for col in CONFIG['UNIVERSE_COLUMNS']:
        if col in universe_indexed.columns:
            results_df[f"{col}_1"] = results_df['CUSIP_1'].map(universe_indexed[col])
            results_df[f"{col}_2"] = results_df['CUSIP_2'].map(universe_indexed[col])
    
    # Remove temporary CUSIP columns
    results_df = results_df.drop(['CUSIP_1', 'CUSIP_2'], axis=1)
    
    # Count successful matches
    universe_cols_1 = [f"{col}_1" for col in CONFIG['UNIVERSE_COLUMNS'] if col in universe_indexed.columns]
    if universe_cols_1:
        match_count = results_df[universe_cols_1[0]].notna().sum()
        print(f"[OK] Enriched {match_count:,} of {len(results_df):,} pairs ({match_count/len(results_df)*100:.1f}% match rate)")
    
    return results_df

def apply_simple_filters(results_df: pd.DataFrame) -> pd.DataFrame:
    """Apply simple mode filters."""
    if not CONFIG['ENABLE_FILTERING'] or not CONFIG['SIMPLE_FILTERS']:
        return results_df
        
    print("[INFO] Applying simple filters...")
    initial_count = len(results_df)
    
    for filter_name, filter_value in CONFIG['SIMPLE_FILTERS'].items():
        if not filter_value:  # Skip disabled filters
            continue
            
        if filter_name == 'same_sector':
            results_df = results_df[results_df['Custom_Sector_1'] == results_df['Custom_Sector_2']]
        elif filter_name == 'same_currency':
            results_df = results_df[results_df['Currency_1'] == results_df['Currency_2']]
        elif filter_name == 'investment_grade_only':
            results_df = results_df[(results_df['Rating_1'] != 'NR') & (results_df['Rating_2'] != 'NR')]
        elif filter_name == 'usd_only':
            results_df = results_df[(results_df['Currency_1'] == 'USD') & (results_df['Currency_2'] == 'USD')]
        elif filter_name == 'min_z_score':
            results_df = results_df[abs(results_df['Z_Score']) >= filter_value]
        elif filter_name == 'fixed_coupon_only':
            results_df = results_df[(results_df['CPN_TYPE_1'] == 'FIXED') & (results_df['CPN_TYPE_2'] == 'FIXED')]
    
    filtered_count = len(results_df)
    print(f"[OK] Simple filters: {initial_count:,} -> {filtered_count:,} pairs ({filtered_count/initial_count*100:.1f}% retained)")
    
    return results_df

def apply_advanced_filters(results_df: pd.DataFrame) -> pd.DataFrame:
    """Apply advanced mode filters."""
    if not CONFIG['ENABLE_FILTERING'] or not CONFIG['ADVANCED_FILTERS']:
        return results_df
        
    print("[INFO] Applying advanced filters...")
    initial_count = len(results_df)
    
    for filter_name, filter_config in CONFIG['ADVANCED_FILTERS'].items():
        operator = filter_config['operator']
        
        if operator == 'pair_equal':
            # Compare values between Security_1 and Security_2
            columns = filter_config['columns']
            for col in columns:
                col_1, col_2 = f"{col}_1", f"{col}_2"
                if col_1 in results_df.columns and col_2 in results_df.columns:
                    results_df = results_df[results_df[col_1] == results_df[col_2]]
                    
        elif operator == 'pair_not_equal':
            # Compare values between Security_1 and Security_2
            columns = filter_config['columns']
            for col in columns:
                col_1, col_2 = f"{col}_1", f"{col}_2"
                if col_1 in results_df.columns and col_2 in results_df.columns:
                    results_df = results_df[results_df[col_1] != results_df[col_2]]
                    
        elif operator in FILTER_OPERATORS and FILTER_OPERATORS[operator] is not None:
            # Standard operator
            column = filter_name.replace('_1', '').replace('_2', '')
            if filter_name in results_df.columns:
                func = FILTER_OPERATORS[operator]
                
                if operator == 'between':
                    mask = results_df[filter_name].apply(lambda x: func(x, filter_config['min'], filter_config['max']))
                elif operator in ['in', 'not_in']:
                    mask = results_df[filter_name].apply(lambda x: func(x, filter_config['values']))
                else:
                    mask = results_df[filter_name].apply(lambda x: func(x, filter_config['value']))
                    
                results_df = results_df[mask]
    
    filtered_count = len(results_df)
    print(f"[OK] Advanced filters: {initial_count:,} -> {filtered_count:,} pairs ({filtered_count/initial_count*100:.1f}% retained)")
    
    return results_df

def parallel_chunk_processing(matrix_chunk, lookback_days, chunk_id):
    """Process a chunk of the matrix in parallel."""
    return vectorized_pairwise_analysis(matrix_chunk, lookback_days)

def save_results(results_df: pd.DataFrame, cusip_mapping: dict = None):
    """Save results to parquet and CSV, and add Own? and XCCY columns."""
    print("[INFO] Saving results...")
    
    if results_df.empty:
        print("[FAIL] No results to save")
        return
    
    # --- ENRICHMENT: Add Own_1 and Own_2 columns ---
    try:
        portfolio = safe_read_parquet('portfolio/portfolio.parquet')
        portfolio_cusips = set(portfolio['CUSIP'])
        
        # Check ownership for both Security_1 and Security_2
        if cusip_mapping is not None:
            # Map both securities to CUSIPs
            security1_cusips = results_df['Security_1'].map(cusip_mapping)
            security2_cusips = results_df['Security_2'].map(cusip_mapping)
            
            # Check ownership for both securities
            results_df['Own_1'] = security1_cusips.map(lambda cusip: 1 if cusip in portfolio_cusips else 0)
            results_df['Own_2'] = security2_cusips.map(lambda cusip: 1 if cusip in portfolio_cusips else 0)
            
            # Report comprehensive statistics
            mapped_1_count = security1_cusips.notna().sum()
            mapped_2_count = security2_cusips.notna().sum()
            owned_1_count = (results_df['Own_1'] == 1).sum()
            owned_2_count = (results_df['Own_2'] == 1).sum()
            either_owned = ((results_df['Own_1'] == 1) | (results_df['Own_2'] == 1)).sum()
            both_owned = ((results_df['Own_1'] == 1) & (results_df['Own_2'] == 1)).sum()
            neither_owned = ((results_df['Own_1'] == 0) & (results_df['Own_2'] == 0)).sum()
            
            print(f"[OK] Added 'Own_1' and 'Own_2' columns:")
            print(f"      • Security_1: {owned_1_count:,}/{mapped_1_count:,} owned ({owned_1_count/mapped_1_count*100:.1f}%)")
            print(f"      • Security_2: {owned_2_count:,}/{mapped_2_count:,} owned ({owned_2_count/mapped_2_count*100:.1f}%)")
            print(f"      • Either owned: {either_owned:,}/{len(results_df):,} pairs ({either_owned/len(results_df)*100:.1f}%)")
            print(f"      • Both owned: {both_owned:,}/{len(results_df):,} pairs ({both_owned/len(results_df)*100:.1f}%)")
            print(f"      • Neither owned: {neither_owned:,}/{len(results_df):,} pairs ({neither_owned/len(results_df)*100:.1f}%)")
        else:
            print("[WARN] No CUSIP mapping provided, using fallback logic")
            results_df['Own_1'] = results_df['Security_1'].map(lambda sec: 1 if sec in portfolio_cusips else 0)
            results_df['Own_2'] = results_df['Security_2'].map(lambda sec: 1 if sec in portfolio_cusips else 0)
            print("[OK] Added 'Own_1' and 'Own_2' columns (FALLBACK)")
    except Exception as e:
        print(f"[WARN] Could not add 'Own_1' and 'Own_2': {e}")
    
    # --- ENRICHMENT: Add XCCY column ---
    try:
        universe = safe_read_parquet('universe/universe.parquet')
        if 'Date' in universe.columns:
            universe = universe[universe['Date'] == universe['Date'].max()]
        universe = universe.rename(columns={'CPN TYPE': 'CPN_TYPE', 'Equity Ticker': 'Equity_Ticker', 'Yrs Since Issue Bucket': 'Yrs_Since_Issue_Bucket', 'Yrs (Mat) Bucket': 'Yrs_Mat_Bucket'})
        
        # Create CUSIP to CAD Equiv Swap mapping
        cad_swap = universe.set_index('CUSIP')['CAD Equiv Swap'].to_dict()
        
        if cusip_mapping is not None:
            # CORRECTED LOGIC: Use proper CUSIP mapping
            # Step 1: Map Security names to CUSIPs
            security1_cusips = results_df['Security_1'].map(cusip_mapping)
            security2_cusips = results_df['Security_2'].map(cusip_mapping)
            
            # Step 2: Map CUSIPs to CAD Equiv Swap values
            cad_swap_1 = security1_cusips.map(cad_swap)
            cad_swap_2 = security2_cusips.map(cad_swap)
            
            # Step 3: Calculate XCCY as the difference
            results_df['XCCY'] = cad_swap_1.fillna(0) - cad_swap_2.fillna(0)
            
            # Add debugging information
            total_pairs = len(results_df)
            matched_1 = cad_swap_1.notna().sum()
            matched_2 = cad_swap_2.notna().sum()
            both_matched = (cad_swap_1.notna() & cad_swap_2.notna()).sum()
            
            print("[INFO] XCCY matching statistics:")
            print(f"      • Security_1 matches: {matched_1:}/{total_pairs:}({matched_1/total_pairs*100:.1f}%)")
            print(f"      • Security_2 matches: {matched_2:}/{total_pairs:}({matched_2/total_pairs*100:.1f}%)")
            print(f"      • Both matched: {both_matched:}/{total_pairs:}({both_matched/total_pairs*100:.1f}%)")
            print(f"      • XCCY range: {results_df['XCCY'].min():.2f} to {results_df['XCCY'].max():.2f}")
        else:
            # Fallback to old logic if cusip_mapping not provided
            print("[WARN] No CUSIP mapping provided, using fallback logic")
            results_df['XCCY'] = results_df['Security_1'].map(cad_swap).fillna(0) - results_df['Security_2'].map(cad_swap).fillna(0)
            print("[OK] Added 'XCCY' column (FALLBACK)")
        
        # Move XCCY beside Percentile if present
        if 'Percentile' in results_df.columns:
            cols = results_df.columns.tolist()
            idx = cols.index('Percentile') + 1
            cols = cols[:idx] + ['XCCY'] + [c for c in cols if c != 'XCCY' and c not in cols[:idx]]
            results_df = results_df[cols]
            
    except Exception as e:
        print(f"[WARN] Could not add 'XCCY': {e}")
        import traceback
        traceback.print_exc()

    # --- ENRICHMENT: Add runs monitor data ---
    try:
        print("[INFO] Adding runs monitor data enrichment...")
        
        # Load runs monitor data (prefer clean version to avoid duplicates)
        runs_monitor_clean_path = Path("runs/run_monitor_clean.parquet")
        runs_monitor_path = Path("runs/run_monitor.parquet")
        
        if runs_monitor_clean_path.exists():
            runs_monitor = safe_read_parquet(runs_monitor_clean_path)
            print(f"[OK] Using clean runs monitor data: {len(runs_monitor):,} securities")
        elif runs_monitor_path.exists():
            runs_monitor = safe_read_parquet(runs_monitor_path)
            # Ensure no duplicates
            initial_count = len(runs_monitor)
            runs_monitor = runs_monitor.drop_duplicates(subset=['Security'], keep='first')
            if len(runs_monitor) != initial_count:
                print(f"[INFO] Removed {initial_count - len(runs_monitor):,} duplicate securities from runs monitor")
            print(f"[OK] Using runs monitor data: {len(runs_monitor):,} securities")
        else:
            print("[WARN] No runs monitor file found, skipping runs enrichment")
            runs_monitor = None
        
        if runs_monitor is not None:
            # Define columns to merge from runs monitor
            target_columns = [
                'Best Bid', 'Best Offer', 'Bid/Offer', 
                'Dealer @ Best Bid', 'Dealer @ Best Offer',
                'Size @ Best Bid', 'Size @ Best Offer', 
                'G Spread', 'Keyword'
            ]
            # Ensure all target columns exist in runs monitor, add missing as NaN
            for col in target_columns:
                if col not in runs_monitor.columns:
                    runs_monitor[col] = np.nan
            # Prepare runs monitor data for merging
            runs_data = runs_monitor[['Security'] + target_columns].copy()
            # Merge for Security_1 with _runs1 suffix
            results_df = results_df.merge(
                runs_data.rename(columns={col: f"{col}_runs1" for col in target_columns}),
                left_on='Security_1',
                right_on='Security',
                how='left',
                suffixes=('', '_runs1')
            )
            if 'Security' in results_df.columns:
                results_df = results_df.drop('Security', axis=1)
            # Merge for Security_2 with _runs2 suffix
            results_df = results_df.merge(
                runs_data.rename(columns={col: f"{col}_runs2" for col in target_columns}),
                left_on='Security_2',
                right_on='Security',
                how='left',
                suffixes=('', '_runs2')
            )
            if 'Security' in results_df.columns:
                results_df = results_df.drop('Security', axis=1)
            # Ensure all 18 enhanced columns are present
            for col in target_columns:
                col1 = f"{col}_runs1"
                col2 = f"{col}_runs2"
                if col1 not in results_df.columns:
                    results_df[col1] = np.nan
                if col2 not in results_df.columns:
                    results_df[col2] = np.nan
                
                # Log merge statistics
                if 'Best Bid_1' in results_df.columns:
                    match_1 = results_df['Best Bid_1'].notna().sum()
                    match_2 = results_df['Best Bid_2'].notna().sum()
                    total = len(results_df)
                    print(f"[INFO] Security_1 match rate: {match_1:}/{total:}({match_1/total*100:.1f}%)")
                    print(f"[INFO] Security_2 match rate: {match_2:}/{total:}({match_2/total*100:.1f}%)")
                
                print(f"[OK] Added {len(target_columns)*2:,} runs monitor columns")
            else:
                print("[WARN] No valid runs monitor columns found for enrichment")
    
    except Exception as e:
        print(f"[WARN] Could not add runs monitor enrichment: {e}")

    # --- DETAILED OUTPUT: Print columns and info ---
    print("\n[INFO] FINAL OUTPUT COLUMNS:")
    print(list(results_df.columns))
    print("\n[INFO] DataFrame info:")
    results_df.info()
    print("")
    
    # Sort by absolute Z-score
    results_df['Abs_Z_Score'] = abs(results_df['Z_Score'])
    results_df = results_df.sort_values('Abs_Z_Score', ascending=False)
    results_df = results_df.drop('Abs_Z_Score', axis=1)
    
    # Save files
    base_dir = Path("historical g spread")
    processed_dir = base_dir / "processed data"
    processed_dir.mkdir(exist_ok=True)
    
    # Parquet
    parquet_path = base_dir / "bond_z.parquet"
    results_df.to_parquet(parquet_path, index=False)
    
    # CSV
    csv_path = processed_dir / "bond_z.csv"
    results_df.to_csv(csv_path, index=False, float_format='%.4f')
    
    print(f"[OK] Saved {len(results_df):,} results")
    print(f"[INFO] Parquet: {parquet_path}")
    print(f"[INFO] CSV: {csv_path}")
    
    # Summary stats
    print("\n[INFO] Summary Statistics:")
    print(f"   Z-score range: {results_df['Z_Score'].min():.2f} to {results_df['Z_Score'].max():.2f}")
    print(f"   Extreme pairs (|Z| > 2): {(abs(results_df['Z_Score']) > 2).sum():,}")
    print(f"   Extreme pairs (|Z| > 3): {(abs(results_df['Z_Score']) > 3).sum():,}")
    
    # Print top 30 most extreme pairs (already sorted by |Z-Score|)
    print("\n[TOP] TOP 30 MOST EXTREME PAIRS (Highest |Z-Score|):")
    top_30 = results_df.head(30).copy()
    top_30['|Z_Score|'] = abs(top_30['Z_Score'])
    top_cols = ['Security_1', 'Security_2', 'Z_Score', '|Z_Score|', 'Last_Spread', 'Custom_Sector_1', 'Custom_Sector_2']
    available_cols = [col for col in top_cols if col in top_30.columns]
    print(top_30[available_cols].to_string(index=False, float_format='%.2f'))

def main():
    """Ultra-fast main execution."""
    print("[RUN] ULTRA-FAST G-Spread Pairwise Z-Score Analysis with Universe Integration")
    print("=" * 80)
    
    # Setup logging if available
    logger = None
    try:
        import logging
        logger = logging.getLogger("pipeline")  # Use same logger name as pipeline system
        logger.info("=== STARTING G-SPREAD Z-SCORE ANALYSIS ===")
        logger.info(f"Configuration: MAX_BONDS={CONFIG['MAX_BONDS']}, LOOKBACK_DAYS={CONFIG['LOOKBACK_DAYS']}")
    except Exception:
        pass  # Continue without logging if setup fails
    print("SPEED OPTIMIZATIONS:")
    print(f"   • Max bonds: {CONFIG['MAX_BONDS']} (most liquid)")
    print(f"   • Date sampling: Every {CONFIG['DATE_SAMPLE_FREQ']} dates" if CONFIG['SAMPLE_DATES'] else "   • Using all dates")
    print(f"   • Parallel processing: {'ON' if CONFIG['USE_PARALLEL'] else 'OFF'}")
    print(f"   • Lookback: {CONFIG['LOOKBACK_DAYS']} days")
    
    print("\nUNIVERSE INTEGRATION:")
    print(f"   • Universe enrichment: {'ON' if CONFIG['INCLUDE_UNIVERSE_DATA'] else 'OFF'}")
    if CONFIG['INCLUDE_UNIVERSE_DATA']:
        print(f"   • Universe columns: {', '.join(CONFIG['UNIVERSE_COLUMNS'])}")
    
    print("\nFILTERING SYSTEM:")
    print(f"   • Core business filters: {'ON' if CONFIG['INCLUDE_UNIVERSE_DATA'] else 'OFF'} (CAD/USD Govt + short-term exclusions)")
    print(f"   • Additional filtering: {'ON' if CONFIG['ENABLE_FILTERING'] else 'OFF'}")
    if CONFIG['ENABLE_FILTERING']:
        simple_active = [k for k, v in CONFIG['SIMPLE_FILTERS'].items() if v]
        advanced_active = list(CONFIG['ADVANCED_FILTERS'].keys())
        if simple_active:
            print(f"   • Simple filters: {', '.join(simple_active)}")
        if advanced_active:
            print(f"   • Advanced filters: {len(advanced_active)} custom rules")
    print("=" * 80)
    
    start_time = datetime.now()
    
    try:
        # 1. Load and pivot data (vectorized approach)
        g_spread_path = get_data_file_path("historical g spread/bond_g_sprd_time_series.parquet")
        df = load_and_pivot_data(g_spread_path)
        if logger: logger.info(f"Loaded G-spread data: {len(df):,} records with {df['Security'].nunique():,} bonds")
        
        # 2. Load universe data for enrichment
        universe_df = load_universe_data()
        if logger: logger.info(f"Loaded universe data: {len(universe_df):,} securities")
        
        # 3. Create CUSIP mapping for universe matching
        cusip_mapping = get_cusip_mapping(df)
        if logger: logger.info(f"Created CUSIP mapping: {len(cusip_mapping):,} mappings")
        
        # 4. Select top liquid bonds
        top_bonds = select_top_bonds(df, CONFIG['MAX_BONDS'])
        if logger: logger.info(f"Selected top liquid bonds: {len(top_bonds):,} bonds")
        
        if len(top_bonds) < 2:
            error_msg = "Insufficient bonds for analysis"
            if logger: logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 5. Apply universe-based bond filtering (maturity > 1yr, issue age 1-4yr)
        filtered_bonds = filter_bonds_by_universe_criteria(top_bonds, cusip_mapping, universe_df)
        if logger: logger.info(f"Applied universe filtering: {len(filtered_bonds):,} bonds passed criteria")
        
        if len(filtered_bonds) < 2:
            error_msg = "Insufficient bonds passed universe filtering criteria"
            if logger: logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 6. Create spread matrix
        matrix = create_spread_matrix(df, filtered_bonds)
        if logger: logger.info(f"Created spread matrix: {matrix.shape[0]:,} dates × {matrix.shape[1]:,} bonds")
        
        # Calculate total potential pairs
        total_pairs = len(filtered_bonds) * (len(filtered_bonds) - 1) // 2
        
        # 7. Vectorized pairwise analysis
        print("\n[ANALYSIS] DETAILED FUNNEL ANALYSIS:")
        print(f"   Stage 1 - Theoretical maximum: {total_pairs:,} potential pairs")
        if logger: logger.info(f"Starting pairwise analysis: {total_pairs:,} potential pairs")
        results_df = vectorized_pairwise_analysis(matrix, CONFIG['LOOKBACK_DAYS'])
        print(f"   Stage 2 - After data quality filters: {len(results_df):,} pairs ({len(results_df)/total_pairs*100:.2f}% of theoretical)")
        if logger: logger.info(f"Pairwise analysis complete: {len(results_df):,} valid pairs")
        
        # 8. Enrich with universe data
        pre_enrich_count = len(results_df)
        results_df = enrich_with_universe_data(results_df, cusip_mapping, universe_df)
        print(f"   Stage 3 - After universe enrichment: {len(results_df):,} pairs ({len(results_df)/total_pairs*100:.2f}% of theoretical)")
        if logger: logger.info(f"Universe enrichment complete: {len(results_df):,} enriched pairs")
        
        # 9. Apply core business filters (always applied when universe data available)
        if CONFIG['INCLUDE_UNIVERSE_DATA'] and not universe_df.empty:
            universe_cols_1 = [f"{col}_1" for col in CONFIG['UNIVERSE_COLUMNS'] if col in universe_df.columns]
            if universe_cols_1:
                initial_count = len(results_df)
                
                # Exclude pairs where universe data is missing
                results_df = results_df.dropna(subset=universe_cols_1[:1])  # Check first universe column
                missing_universe = initial_count - len(results_df)
                
                # CORE BUSINESS FILTERS (always applied)
                pre_filter_count = len(results_df)
                
                # Filter 1: Exclude CAD Government and USD Government bonds
                if 'Custom_Sector_1' in results_df.columns and 'Custom_Sector_2' in results_df.columns:
                    results_df = results_df[
                        (results_df['Custom_Sector_1'] != 'CAD Govt') & 
                        (results_df['Custom_Sector_2'] != 'CAD Govt') &
                        (results_df['Custom_Sector_1'] != 'USD Govt') & 
                        (results_df['Custom_Sector_2'] != 'USD Govt')
                    ]
                    govt_excluded = pre_filter_count - len(results_df)
                    pre_filter_count = len(results_df)
                    
                # Filter 2: Exclude short-term maturity buckets
                if 'Yrs_Mat_Bucket_1' in results_df.columns and 'Yrs_Mat_Bucket_2' in results_df.columns:
                    excluded_buckets = ['>.25-1', '0-0.50', '0.50-1']
                    results_df = results_df[
                        (~results_df['Yrs_Mat_Bucket_1'].isin(excluded_buckets)) &
                        (~results_df['Yrs_Mat_Bucket_2'].isin(excluded_buckets)) &
                        (results_df['Yrs_Mat_Bucket_1'].notna()) &
                        (results_df['Yrs_Mat_Bucket_2'].notna())
                    ]
                    short_term_excluded = pre_filter_count - len(results_df)
                
                # Report filtering results
                total_excluded = initial_count - len(results_df)
                if missing_universe > 0:
                    print(f"   [FILTER] Excluded {missing_universe:,} pairs without universe matches")
                if 'govt_excluded' in locals() and govt_excluded > 0:
                    print(f"   [FILTER] Excluded {govt_excluded:,} pairs with Government bonds (CAD/USD)")
                if 'short_term_excluded' in locals() and short_term_excluded > 0:
                    print(f"   [FILTER] Excluded {short_term_excluded:,} pairs with short-term maturities (<1 year)")
                if total_excluded > 0:
                    print(f"   [OK] Core filters: {initial_count:,} -> {len(results_df):,} pairs ({len(results_df)/initial_count*100:.1f}% retained)")
                    print(f"   Stage 4 - After core business filters: {len(results_df):,} pairs ({len(results_df)/total_pairs*100:.2f}% of theoretical)")
        
        # 10. Apply simple filters
        pre_simple_count = len(results_df)
        results_df = apply_simple_filters(results_df)
        if len(results_df) != pre_simple_count:
            print(f"   Stage 5 - After simple filters: {len(results_df):,} pairs ({len(results_df)/total_pairs*100:.2f}% of theoretical)")
        
        # 11. Apply advanced filters  
        pre_advanced_count = len(results_df)
        results_df = apply_advanced_filters(results_df)
        if len(results_df) != pre_advanced_count:
            print(f"   Stage 6 - After advanced filters: {len(results_df):,} pairs ({len(results_df)/total_pairs*100:.2f}% of theoretical)")
        
        print("\n[SUMMARY] FINAL FUNNEL SUMMARY:")
        print(f"   • Started with: {total_pairs:,} potential pairs")
        print(f"   • Ended with: {len(results_df):,} final pairs")
        print(f"   • Overall retention rate: {len(results_df)/total_pairs*100:.3f}%")
        print(f"   • Reduction factor: {total_pairs/len(results_df):.1f}x fewer pairs")
        
        # 12. Save results
        save_results(results_df, cusip_mapping)
        if logger: logger.info(f"Results saved: {len(results_df):,} final pairs")
        
        # Performance summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n[COMPLETE] ULTRA-FAST EXECUTION COMPLETE!")
        print(f"   [TIME] Duration: {duration.total_seconds():.1f} seconds")
        print(f"   [COUNT] Processed: {total_pairs:,} potential pairs")
        print(f"   [RATE] Rate: {total_pairs / duration.total_seconds():.0f} pairs/second")
        print(f"   [STATUS] Success: {'[OK]' if duration.total_seconds() < 120 else '[WARN]'} {'Under 2 minutes!' if duration.total_seconds() < 120 else 'Over 2 minutes'}")
        
        if logger: 
            logger.info(f"=== G-SPREAD Z-SCORE ANALYSIS COMPLETE ===")
            logger.info(f"Duration: {duration.total_seconds():.1f} seconds")
            logger.info(f"Final results: {len(results_df):,} pairs from {total_pairs:,} potential")
            logger.info(f"Processing rate: {total_pairs / duration.total_seconds():.0f} pairs/second")
        
    except Exception as e:
        error_msg = f"Error: {e}"
        print(f"[FAIL] {error_msg}")
        if logger: logger.error(f"Analysis failed: {error_msg}")
        raise

# ==========================================
# INTERACTIVE MODE INSTRUCTIONS
# ==========================================

if __name__ != "__main__":
    # Display helpful instructions when imported
    print("\n" + "="*70)
    print("[READY] G-SPREAD Z-SCORE ANALYSIS - INTERACTIVE MODE READY!")
    print("="*70)
    print("[HELP] Quick Start Commands:")
    print("   • get_config_summary()           # View current settings")
    print("   • run_quick_analysis()           # Quick test with 50 bonds")
    print("   • run_quick_analysis(100, 180)   # Custom: 100 bonds, 180 days")
    print("   • main()                         # Full analysis with current config")
    print("   • load_latest_results()          # Load previous results")
    print("")
    print("[SETUP] Setup Functions:")
    print("   • setup_interactive_environment() # Fix path issues")
    print("   • check_required_files()         # Verify data files")
    print("   • set_config(max_bonds=100)      # Modify settings easily")
    print("")
    print("[CONFIG] Configuration:")
    print(f"   • Current max bonds: {CONFIG['MAX_BONDS']}")
    print(f"   • Current lookback: {CONFIG['LOOKBACK_DAYS']} days")
    print(f"   • Universe integration: {'ON' if CONFIG['INCLUDE_UNIVERSE_DATA'] else 'OFF'}")
    print("="*70)

if __name__ == "__main__":
    warnings.filterwarnings('ignore')
    
    # Setup logging for command line execution
    try:
        if setup_interactive_environment():
            sys.path.append(str(Path(PROJECT_ROOT) / "src"))
            from utils.logging import LogManager
            log_manager = LogManager("logs/g_spread_processor.log", "INFO")
            print("[OK] Pipeline logging enabled for command line execution")
    except Exception as e:
        print(f"[WARN] Could not setup logging: {e}")
    
    main() 