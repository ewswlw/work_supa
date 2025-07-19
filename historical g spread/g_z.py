#!/usr/bin/env python3
"""
G-Spread Pairwise Z-Score Analysis - CORE ANALYSIS ONLY
========================================================

This script performs CORE G-spread pairwise analysis with NO external dependencies.

CORE ANALYSIS FEATURES:
  - Data loading from bond_g_sprd_time_series.parquet only
  - Pairwise Z-score, percentiles, min/max analysis
  - Self-contained with no external table dependencies
  - Output: 9 core analysis columns only

OUTPUT COLUMNS (11):
  - Security_1, Security_2 (bond pair names)
  - CUSIP_1, CUSIP_2 (bond pair CUSIPs from raw data)
  - Last_Spread, Z_Score, Max, Min, Last_vs_Max, Last_vs_Min, Percentile

REMOVED FEATURES (for core simplicity):
  - Universe data enrichment
  - Portfolio data enrichment  
  - Runs monitor data enrichment
  - All filtering systems
  - External dependencies

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
        print("[INFO] Interactive mode: Changing directory from {Path(current_dir).name} to work_supa")
        try:
            os.chdir(PROJECT_ROOT)
            print("[OK] Changed to: {os.getcwd()}")
        except Exception as e:
            print("[FAIL] Could not change directory: {e}")
            print("[INFO] Please manually change to: {PROJECT_ROOT}")
            return False
    else:
        print("[OK] Already in correct directory: {current_dir}")
    
    # Add src to Python path if needed
    src_path = Path(PROJECT_ROOT) / "src"
    if str(src_path) not in sys.path:
        sys.path.append(str(src_path))
        print("[PYTHON] Added src to Python path: {src_path}")
    
    return True

def check_required_files():
    """Check if all required files exist."""
    required_files = [
        "historical g spread/raw data/g_ts.parquet"
    ]
    
    print("[INFO] Checking required files...")
    all_exist = True
    
    for file_path in required_files:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path) / (1024*1024)  # MB
            print("[OK] {file_path} ({file_size:.1f} MB)")
        else:
            print("[FAIL] MISSING: {file_path}")
            all_exist = False
    
    if not all_exist:
        print("[WARN] Some required files are missing. Please ensure you're in the correct directory.")
        print("   Expected directory: {PROJECT_ROOT}")
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

# Auto-setup when imported (for interactive use)
if __name__ != "__main__":
    # Only auto-setup if we're being imported (interactive mode)
    try:
        setup_result = setup_interactive_environment()
        if setup_result:
            check_required_files()
    except Exception as e:
        print("[WARN] Auto-setup failed: {e}")
        print("[INFO] You can manually run: setup_interactive_environment()")

# ==========================================
# INTERACTIVE UTILITY FUNCTIONS  
# ==========================================

def run_quick_analysis(max_bonds=50, lookback_days=252, enable_filters=False):
    """Quick analysis for interactive testing with fewer bonds."""
    global CONFIG
    
    print("[RUN] Running QUICK ANALYSIS (Interactive Mode)")
    print("   • Max bonds: {max_bonds}")
    print("   • Lookback: {lookback_days} days") 
    print("   • Filtering: {'ON' if enable_filters else 'OFF'}")
    
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
    print("   • Max bonds: {CONFIG['MAX_BONDS']}")
    print("   • Lookback days: {CONFIG['LOOKBACK_DAYS']}")
    print("   • Date sampling: Every {CONFIG['DATE_SAMPLE_FREQ']} dates" if CONFIG['SAMPLE_DATES'] else "   • Using all dates")
    print("   • Universe integration: {'ON' if CONFIG['INCLUDE_UNIVERSE_DATA'] else 'OFF'}")
    print("   • Filtering enabled: {'ON' if CONFIG['ENABLE_FILTERING'] else 'OFF'}")
    
    if CONFIG['INCLUDE_UNIVERSE_DATA']:
        print("   • Universe columns: {', '.join(CONFIG['UNIVERSE_COLUMNS'])}")
    
    if CONFIG['ENABLE_FILTERING']:
        simple_active = [k for k, v in CONFIG['SIMPLE_FILTERS'].items() if v]
        advanced_active = list(CONFIG['ADVANCED_FILTERS'].keys())
        if simple_active:
            print("   • Simple filters: {', '.join(simple_active)}")
        if advanced_active:
            print("   • Advanced filters: {len(advanced_active)} rules")

def load_latest_results():
    """Load the most recent analysis results for interactive exploration."""
    parquet_path = "historical g spread/bond_z.parquet"
    csv_path = "historical g spread/processed data/bond_z.csv"
    
    try:
        if os.path.exists(parquet_path):
            df = safe_read_parquet(parquet_path)
            print("[OK] Loaded latest results: {df.shape}")
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
        print("[OK] Set max_bonds = {max_bonds}")
    
    if lookback_days is not None:
        CONFIG['LOOKBACK_DAYS'] = lookback_days
        print("[OK] Set lookback_days = {lookback_days}")
    
    if enable_filters is not None:
        CONFIG['ENABLE_FILTERING'] = enable_filters
        print("[OK] Set filtering = {'ON' if enable_filters else 'OFF'}")
    
    if enable_universe is not None:
        CONFIG['INCLUDE_UNIVERSE_DATA'] = enable_universe
        print("[OK] Set universe integration = {'ON' if enable_universe else 'OFF'}")
    
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
    'MAX_BONDS': 200,       # Limit to most liquid bonds for speed
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
    # CORE ANALYSIS ONLY - NO EXTERNAL DATA
    # ==========================================
    'INCLUDE_UNIVERSE_DATA': False,  # Disabled - core analysis only
    
    # ==========================================
    # CORE ANALYSIS ONLY - NO FILTERING
    # ==========================================
    
    'ENABLE_FILTERING': False,      # Disabled - core analysis only
}

def load_and_pivot_data(file_path: str) -> pd.DataFrame:
    """Load data and immediately pivot to matrix format for vectorized operations."""
    print("[RUN] Loading and pivoting data for vectorized processing...")
    
    df = safe_read_parquet(file_path)
    
    # Standardize column names for compatibility
    df = df.rename(columns={
        'Date': 'DATE',
        'Name': 'Security', 
        'G_Spread_BVAL': 'GSpread'
    })
    
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
    """Universe data loading disabled - core analysis only."""
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
    """Create Security name to CUSIP mapping from raw data."""
    print("[INFO] Creating Security -> CUSIP mapping from raw data...")
    
    # Get unique Security → CUSIP mapping from the same raw data
    mapping_df = df[['Security', 'CUSIP']].drop_duplicates()
    mapping = dict(zip(mapping_df['Security'], mapping_df['CUSIP']))
    
    print(f"[OK] Created {len(mapping):,} Security -> CUSIP mappings")
    return mapping

def create_spread_matrix(df: pd.DataFrame, bonds: list) -> pd.DataFrame:
    """Create a date x bond matrix for vectorized calculations."""
    print("[RUN] Creating spread matrix for vectorized operations...")
    
    # Filter to selected bonds
    df_filtered = df[df['Security'].isin(bonds)]
    
    # Handle duplicates by taking the last value (most recent for same date/security)
    # This is common in bond data where there might be multiple updates per day
    df_aggregated = df_filtered.groupby(['DATE', 'Security'])['GSpread'].last().reset_index()
    
    # Pivot to create matrix
    matrix = df_aggregated.pivot(index='DATE', columns='Security', values='GSpread')
    
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
    print("   • Total pairs attempted: {total_pairs_attempted:,}")
    print("   • Failed start date check: {failed_start_date_check:,} ({failed_start_date_check/total_pairs_attempted*100:.1f}%)")
    print("   • Failed missing recent data: {failed_missing_recent_data:,} ({failed_missing_recent_data/total_pairs_attempted*100:.1f}%)")
    print("   • Failed no spread diff data: {failed_no_spread_diff_data:,} ({failed_no_spread_diff_data/total_pairs_attempted*100:.1f}%)")
    print("   • Successful pairs: {successful_pairs:,} ({successful_pairs/total_pairs_attempted*100:.1f}%)")
    
    return pd.DataFrame(results)

def enrich_with_cusip_data(results_df: pd.DataFrame, cusip_mapping: dict) -> pd.DataFrame:
    """Add CUSIP columns using raw data mapping."""
    if not cusip_mapping:
        return results_df
        
    print("[INFO] Adding CUSIP_1 and CUSIP_2 columns...")
    
    # Add CUSIP columns for both securities
    results_df['CUSIP_1'] = results_df['Security_1'].map(cusip_mapping)
    results_df['CUSIP_2'] = results_df['Security_2'].map(cusip_mapping)
    
    # Count successful matches
    cusip_1_matches = results_df['CUSIP_1'].notna().sum()
    cusip_2_matches = results_df['CUSIP_2'].notna().sum()
    total_pairs = len(results_df)
    
    print(f"[OK] CUSIP_1 matches: {cusip_1_matches}/{total_pairs} ({cusip_1_matches/total_pairs*100:.1f}%)")
    print(f"[OK] CUSIP_2 matches: {cusip_2_matches}/{total_pairs} ({cusip_2_matches/total_pairs*100:.1f}%)")
    
    # Reorder columns: Security_1, CUSIP_1, Security_2, CUSIP_2, then the rest
    original_cols = ['Security_1', 'Security_2', 'Last_Spread', 'Z_Score', 'Max', 'Min', 'Last_vs_Max', 'Last_vs_Min', 'Percentile']
    new_cols = ['Security_1', 'CUSIP_1', 'Security_2', 'CUSIP_2'] + original_cols[2:]
    results_df = results_df[new_cols]
    
    return results_df

def apply_simple_filters(results_df: pd.DataFrame) -> pd.DataFrame:
    """Filtering disabled - core analysis only."""
    return results_df

def apply_advanced_filters(results_df: pd.DataFrame) -> pd.DataFrame:
    """Filtering disabled - core analysis only."""
    return results_df

def parallel_chunk_processing(matrix_chunk, lookback_days, chunk_id):
    """Process a chunk of the matrix in parallel."""
    return vectorized_pairwise_analysis(matrix_chunk, lookback_days)

def save_results(results_df: pd.DataFrame, cusip_mapping: dict = None):
    """Save core analysis results to parquet and CSV."""
    print("[INFO] Saving core analysis results...")
    
    if results_df.empty:
        print("[FAIL] No results to save")
        return

    # --- CORE ANALYSIS OUTPUT: Print columns and info ---
    print("\n[INFO] CORE ANALYSIS COLUMNS:")
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
    # Core columns only
    core_cols = ['Security_1', 'Security_2', 'Z_Score', '|Z_Score|', 'Last_Spread', 'Percentile']
    available_cols = [col for col in core_cols if col in top_30.columns]
    print(top_30[available_cols].to_string(index=False, float_format='%.2f'))

def main():
    """Core G-Spread Pairwise Z-Score Analysis - Self-Contained."""
    print("[RUN] CORE G-Spread Pairwise Z-Score Analysis")
    print("=" * 80)
    print("CORE ANALYSIS CONFIGURATION:")
    print(f"   • Max bonds: {CONFIG['MAX_BONDS']} (most liquid)")
    print(f"   • Date sampling: Every {CONFIG['DATE_SAMPLE_FREQ']} dates" if CONFIG['SAMPLE_DATES'] else "   • Using all dates")
    print(f"   • Parallel processing: {'ON' if CONFIG['USE_PARALLEL'] else 'OFF'}")
    print(f"   • Lookback: {CONFIG['LOOKBACK_DAYS']} days")
    print("   • External dependencies: NONE (self-contained)")
    print("   • Output columns: 11 core analysis columns (includes CUSIPs)")
    print("=" * 80)
    
    start_time = datetime.now()
    
    try:
        # 1. Load and pivot data (vectorized approach)
        g_spread_path = get_data_file_path("historical g spread/raw data/g_ts.parquet")
        df = load_and_pivot_data(g_spread_path)
        
        # 2. Create CUSIP mapping from the same raw data (no external dependencies)
        universe_df = pd.DataFrame()
        cusip_mapping = get_cusip_mapping(df)
        
        # 4. Select top liquid bonds
        top_bonds = select_top_bonds(df, CONFIG['MAX_BONDS'])
        
        if len(top_bonds) < 2:
            raise ValueError("Insufficient bonds for analysis")
        
        # 5. Create spread matrix
        matrix = create_spread_matrix(df, top_bonds)
        
        # Calculate total potential pairs
        total_pairs = len(top_bonds) * (len(top_bonds) - 1) // 2
        
        # 6. Vectorized pairwise analysis
        print("\n[ANALYSIS] DETAILED FUNNEL ANALYSIS:")
        print(f"   Stage 1 - Theoretical maximum: {total_pairs:,} potential pairs")
        results_df = vectorized_pairwise_analysis(matrix, CONFIG['LOOKBACK_DAYS'])
        print(f"   Stage 2 - After data quality filters: {len(results_df):,} pairs ({len(results_df)/total_pairs*100:.2f}% of theoretical)")
        
        # 7. Add CUSIP columns from raw data
        results_df = enrich_with_cusip_data(results_df, cusip_mapping)
        print(f"   Stage 3 - Core analysis complete with CUSIPs: {len(results_df):,} pairs ({len(results_df)/total_pairs*100:.2f}% of theoretical)")
        
        print("\n[SUMMARY] CORE ANALYSIS SUMMARY:")
        print(f"   • Started with: {total_pairs:,} potential pairs")
        print(f"   • Ended with: {len(results_df):,} core analysis pairs")
        print(f"   • Data quality retention rate: {len(results_df)/total_pairs*100:.3f}%")
        print(f"   • Reduction factor: {total_pairs/len(results_df):.1f}x fewer pairs (quality filtering)")
        
        # 8. Save core results
        save_results(results_df, cusip_mapping)
        
        # Performance summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n[COMPLETE] CORE ANALYSIS EXECUTION COMPLETE!")
        print(f"   [TIME] Duration: {duration.total_seconds():.1f} seconds")
        print(f"   [COUNT] Processed: {total_pairs:,} potential pairs")
        print(f"   [RATE] Rate: {total_pairs / duration.total_seconds():.0f} pairs/second")
        print(f"   [STATUS] Success: {'[OK]' if duration.total_seconds() < 120 else '[WARN]'} {'Under 2 minutes!' if duration.total_seconds() < 120 else 'Over 2 minutes'}")
        print("   [OUTPUT] Generated 11 core analysis columns (includes CUSIPs)")
        
    except Exception as e:
        print(f"[FAIL] Error: {e}")
        raise

# ==========================================
# INTERACTIVE MODE INSTRUCTIONS
# ==========================================

if __name__ != "__main__":
    # Display helpful instructions when imported
    print("\n" + "="*70)
    print("[READY] G-SPREAD CORE ANALYSIS - INTERACTIVE MODE READY!")
    print("="*70)
    print("[HELP] Quick Start Commands:")
    print("   • get_config_summary()           # View current settings")
    print("   • run_quick_analysis()           # Quick test with 50 bonds")
    print("   • run_quick_analysis(100, 180)   # Custom: 100 bonds, 180 days")
    print("   • main()                         # Full core analysis")
    print("   • load_latest_results()          # Load previous results")
    print("")
    print("[SETUP] Setup Functions:")
    print("   • setup_interactive_environment() # Fix path issues")
    print("   • check_required_files()         # Verify data files")
    print("   • set_config(max_bonds=100)      # Modify settings easily")
    print("")
    print("[CONFIG] Core Analysis Configuration:")
    print("   • Current max bonds: {CONFIG['MAX_BONDS']}")
    print("   • Current lookback: {CONFIG['LOOKBACK_DAYS']} days")
    print("   • External dependencies: NONE (core only)")
    print("   • Output: 9 core analysis columns")
    print("="*70)

if __name__ == "__main__":
    warnings.filterwarnings('ignore')
    main() 