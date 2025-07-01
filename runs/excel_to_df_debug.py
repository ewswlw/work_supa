"""
excel_to_df_debug.py

Highly configurable script to scan a directory for Excel files, load them into a single DataFrame (with parallel loading),
parse Date and Time columns as datetime/time objects (kept separate), print extensive debugging and integrity info, and output to Parquet.

Features:
- Incremental processing: Only processes new or modified files
- Parallel loading of Excel files
- Comprehensive data validation and logging
- Deduplication and cleaning
- Detailed statistics and debugging output
- Command line options for force processing

Dependencies: pandas, openpyxl, pyarrow (for Parquet)

Usage:
    python excel_to_df_debug.py                    # Normal incremental processing
    python excel_to_df_debug.py --force-all        # Force process all files
    python excel_to_df_debug.py --reset-date       # Reset last processed date to None
"""
import os
import sys
import json
import pandas as pd
from glob import glob
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import argparse

# ===================== CONFIG SECTION =====================
CONFIG = {
    # Directory containing Excel files
    'INPUT_DIR': os.path.join(os.path.dirname(__file__), 'older files'),
    # Glob pattern for Excel files
    'FILE_PATTERN': '*.xls*',
    # Output Parquet file
    'OUTPUT_PARQUET': os.path.join(os.path.dirname(__file__), 'combined_runs.parquet'),
    # Last processed date file
    'LAST_PROCESSED_FILE': os.path.join(os.path.dirname(__file__), 'last_processed.json'),
    # Date and Time formats (for parsing)
    'DATE_FORMAT': '%m/%d/%y',  # Excel date format in your files
    'TIME_FORMAT': '%H:%M',     # Excel time format in your files
    # Enable parallel loading
    'PARALLEL_LOAD': True,
    # Number of parallel workers (set to os.cpu_count() or override)
    'N_WORKERS': os.cpu_count() or 4,
    # How many rows to show in head/tail
    'SHOW_ROWS': 5,
    # Log file
    'LOG_FILE': os.path.join(os.path.dirname(__file__), 'data_pipeline.log'),
}
# =================== END CONFIG SECTION ===================

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='[LOG] %(message)s',
    handlers=[
        logging.FileHandler(CONFIG['LOG_FILE']),
        logging.StreamHandler()
    ]
)

# Add import for LogManager
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.utils.logging import LogManager

# Set up new LogManager logger for runs_processor.log
RUNS_LOG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs', 'runs_processor.log'))
runs_logger = LogManager(
    log_file=RUNS_LOG_PATH,
    log_level='INFO',
    log_format='[%(asctime)s] %(levelname)s: %(message)s'
)

def log(msg):
    """Log message to both file and console."""
    logging.info(msg)

def log_runs(msg, level="info"):
    """Log message to runs_processor.log using LogManager-style logger."""
    if level == "info":
        runs_logger.info(msg)
    elif level == "debug":
        runs_logger.debug(msg)
    elif level == "warning":
        runs_logger.warning(msg)
    elif level == "error":
        runs_logger.error(msg)
    else:
        runs_logger.info(msg)

def load_last_processed():
    """Load the last processed date from JSON file. Always return a dict with 'last_processed' key."""
    try:
        if os.path.exists(CONFIG['LAST_PROCESSED_FILE']):
            with open(CONFIG['LAST_PROCESSED_FILE'], "r") as f:
                data = json.load(f)
                last_processed = data.get("last_processed")
                if last_processed:
                    # Check if the date is in the future
                    last_date = datetime.strptime(last_processed, "%Y-%m-%d")
                    if last_date > datetime.now():
                        log(f"Warning: Last processed date ({last_processed}) is in the future. Resetting.")
                        return {"last_processed": None}
                return {"last_processed": last_processed}
    except Exception as e:
        log(f"Warning: Could not read or parse last_processed.json: {e}")
    return {"last_processed": None}

def save_last_processed(date):
    """Save the last processed date to JSON file."""
    with open(CONFIG['LAST_PROCESSED_FILE'], "w") as f:
        json.dump({"last_processed": date}, f)

def validate_dataframe(df, stage=""):
    """Validate DataFrame and log statistics."""
    log(f"\n===== DataFrame Validation {stage} =====")
    log_runs(f"\n===== DataFrame Validation {stage} =====")
    log(f"Shape: {df.shape}")
    log_runs(f"Shape: {df.shape}")
    log(f"Columns: {df.columns.tolist()}")
    log_runs(f"Columns: {df.columns.tolist()}")
    log(f"Data types:\n{df.dtypes}")
    log_runs(f"Data types:\n{df.dtypes}")
    
    # DataFrame info
    from io import StringIO
    buf = StringIO()
    df.info(buf=buf)
    info_str = buf.getvalue()
    log(f"DataFrame info:\n{info_str}")
    log_runs(f"DataFrame info:\n{info_str}")
    
    # Check for nulls
    nulls = df.isnull().sum()
    log("\nNull values per column:")
    log_runs("\nNull values per column:")
    for col in df.columns:
        null_count = nulls[col]
        null_pct = (null_count / len(df)) * 100
        log(f"{col}: {null_count} ({null_pct:.2f}%)")
        log_runs(f"{col}: {null_count} ({null_pct:.2f}%)")
    
    # Check for duplicates
    dupes = df.duplicated().sum()
    log(f"\nDuplicate rows: {dupes}")
    log_runs(f"\nDuplicate rows: {dupes}")
    
    # Basic statistics for numeric columns
    log("\nNumeric columns statistics:")
    log_runs("\nNumeric columns statistics:")
    stats = str(df.describe())
    log(stats)
    log_runs(stats)
    
    return df

def parse_date_time_columns(df, file_name):
    """
    Parse 'Date' and 'Time' columns to datetime/time types with enhanced error handling.
    Maintains consistency with the enhanced pipeline datetime approach.
    """
    log(f"Parsing date/time columns for {os.path.basename(file_name)}")
    
    if 'Date' in df.columns:
        try:
            log(f"  Original Date column type: {df['Date'].dtype}")
            
            # Clean and convert Date column
            date_series = df['Date'].astype(str).str.strip()
            
            # Try different date formats
            date_formats = [CONFIG['DATE_FORMAT'], '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']
            
            parsed_dates = None
            for fmt in date_formats:
                try:
                    parsed_dates = pd.to_datetime(date_series, format=fmt, errors='coerce')
                    valid_count = parsed_dates.notna().sum()
                    if valid_count > 0:
                        log(f"  Successfully parsed {valid_count} dates using format '{fmt}'")
                        break
                except:
                    continue
            
            if parsed_dates is None:
                # Fallback to pandas auto-parsing
                parsed_dates = pd.to_datetime(date_series, errors='coerce')
                log(f"  Used pandas auto-parsing for Date column")
            
            df['Date'] = parsed_dates
            nat_count = df['Date'].isna().sum()
            log(f"  Final Date column type: {df['Date'].dtype}")
            log(f"  NaT (invalid dates) count: {nat_count}")
            
            if nat_count > 0:
                log(f"  Warning: {nat_count} dates could not be parsed in {os.path.basename(file_name)}")
                
        except Exception as e:
            log(f"ERROR parsing 'Date' in {file_name}: {e}")
            
    if 'Time' in df.columns:
        try:
            log(f"  Original Time column type: {df['Time'].dtype}")
            
            # Clean and convert Time column
            time_series = df['Time'].astype(str).str.strip()
            
            # Parse as datetime first, then extract time
            time_formats = [CONFIG['TIME_FORMAT'], '%H:%M:%S', '%H:%M']
            
            parsed_times = None
            for fmt in time_formats:
                try:
                    temp_datetime = pd.to_datetime(time_series, format=fmt, errors='coerce')
                    if temp_datetime.notna().sum() > 0:
                        parsed_times = temp_datetime.dt.time
                        log(f"  Successfully parsed times using format '{fmt}'")
                        break
                except:
                    continue
            
            if parsed_times is None:
                # Fallback to pandas auto-parsing
                temp_datetime = pd.to_datetime(time_series, errors='coerce')
                parsed_times = temp_datetime.dt.time
                log(f"  Used pandas auto-parsing for Time column")
            
            df['Time'] = parsed_times
            nan_count = df['Time'].isna().sum()
            log(f"  Final Time column type: {df['Time'].dtype}")
            log(f"  NaN (invalid times) count: {nan_count}")
            
            if nan_count > 0:
                log(f"  Warning: {nan_count} times could not be parsed in {os.path.basename(file_name)}")
                
        except Exception as e:
            log(f"ERROR parsing 'Time' in {file_name}: {e}")
    
    return df

def load_excel(file):
    """
    Load a single Excel file, parse Date and Time columns, and return (filename, DataFrame or None)
    """
    try:
        df = pd.read_excel(file, engine='openpyxl')
        log(f"Loaded {os.path.basename(file)}: shape={df.shape}")
        df = parse_date_time_columns(df, file)
        return (file, df)
    except Exception as e:
        log(f"ERROR loading {file}: {e}")
        return (file, None)

def clean_and_deduplicate(df):
    """Clean and deduplicate the DataFrame with proper datetime handling."""
    log(f"Starting clean_and_deduplicate with {len(df)} rows")
    
    # Remove negative values only from columns that should never be negative
    non_negative_cols = ['Bid Size', 'Ask Size', 'Bid Price', 'Ask Price']
    for col in non_negative_cols:
        if col in df.columns:
            before_count = len(df)
            df = df[df[col] >= 0]
            after_count = len(df)
            log(f"After filtering negative {col}: {after_count} rows (removed {before_count - after_count})")
    
    # Ensure Date column is datetime type (no string conversion needed)
    if 'Date' in df.columns:
        log(f"Date column type before sorting: {df['Date'].dtype}")
        if not pd.api.types.is_datetime64_any_dtype(df['Date']):
            log("Converting Date column to datetime...")
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        log(f"Date column type after conversion: {df['Date'].dtype}")
    
    # Sort by Date and Time (keeping datetime objects)
    log(f"Sorting by Date and Time...")
    try:
        # Create sort columns list dynamically
        sort_cols = []
        if 'Date' in df.columns:
            sort_cols.append('Date')
        if 'Time' in df.columns:
            sort_cols.append('Time')
        
        if sort_cols:
            df = df.sort_values(sort_cols)
            log(f"After sorting by {sort_cols}: {len(df)} rows")
        else:
            log("No Date or Time columns found for sorting")
            
    except Exception as e:
        log(f"ERROR during sorting: {e}")
        log(f"Date column type: {df['Date'].dtype if 'Date' in df.columns else 'N/A'}")
        log(f"Time column type: {df['Time'].dtype if 'Time' in df.columns else 'N/A'}")
        if 'Date' in df.columns:
            log(f"Sample Date values: {df['Date'].head()}")
        if 'Time' in df.columns:
            log(f"Sample Time values: {df['Time'].head()}")
        raise e
    
    # Remove duplicates based on key columns
    key_columns = [
        'Date', 'Time', 'Dealer', 'Security', 'Bid Price', 'Ask Price',
        'Bid Size', 'Ask Size', 'Bid Yield To Convention', 'Ask Yield To Convention'
    ]
    
    # Only use columns that exist in the DataFrame
    key_columns = [col for col in key_columns if col in df.columns]
    log(f"Using deduplication columns: {key_columns}")
    
    # Remove duplicates keeping the last occurrence
    before_dedup = len(df)
    df = df.drop_duplicates(subset=key_columns, keep='last')
    after_dedup = len(df)
    log(f"After deduplication: {after_dedup} rows (removed {before_dedup - after_dedup})")
    
    # Final validation of Date column
    if 'Date' in df.columns:
        log(f"Final Date column type: {df['Date'].dtype}")
        null_dates = df['Date'].isna().sum()
        if null_dates > 0:
            log(f"Warning: {null_dates} null dates found after cleaning")
    
    log(f"Final clean_and_deduplicate result: {len(df)} rows")
    
    return df

def log_date_coverage(df, label="Final DataFrame"):
    """Enhanced date coverage analysis with rich analytics."""
    if 'Date' in df.columns:
        unique_dates = sorted(df['Date'].dropna().unique())
        log_runs(f"\n=========================")
        log_runs(f"=== DATE COVERAGE ANALYSIS ({label}) ===")
        log_runs(f"=========================")
        log_runs(f"Total unique dates: {len(unique_dates)}")
        log_runs(f"Date column dtype: {df['Date'].dtype}")
        
        if unique_dates:
            # Convert to pandas datetime for formatting
            min_date = pd.to_datetime(unique_dates[0])
            max_date = pd.to_datetime(unique_dates[-1])
            
            log_runs(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
            
            # Calculate time span
            time_span = max_date - min_date
            log_runs(f"Time span: {time_span.days} days ({time_span.days/365.25:.1f} years)")
            
            # Year distribution analysis
            df_temp = df[df['Date'].notna()].copy()
            df_temp['year'] = df_temp['Date'].dt.year
            df_temp['weekday'] = df_temp['Date'].dt.day_name()
            
            year_dist = df_temp['year'].value_counts().sort_index()
            log_runs(f"\nYear distribution:")
            for year, count in year_dist.items():
                log_runs(f"  - {year}: {count} records")
            
            # Business day analysis
            business_days = df_temp[~df_temp['weekday'].isin(['Saturday', 'Sunday'])]
            weekend_days = df_temp[df_temp['weekday'].isin(['Saturday', 'Sunday'])]
            
            log_runs(f"\nBusiness day analysis:")
            log_runs(f"  - Business days: {len(business_days)} ({len(business_days)/len(df_temp)*100:.1f}%)")
            log_runs(f"  - Weekend days: {len(weekend_days)} ({len(weekend_days)/len(df_temp)*100:.1f}%)")
            
            # Sample dates with record counts
            log_runs(f"\nSample dates:")
            for i, date in enumerate(unique_dates[:5]):
                date_dt = pd.to_datetime(date)
                date_count = (df['Date'] == date).sum()
                log_runs(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
            
            if len(unique_dates) > 10:
                log_runs(f"  ... ({len(unique_dates) - 10} more dates) ...")
                for i, date in enumerate(unique_dates[-5:]):
                    date_dt = pd.to_datetime(date)
                    date_count = (df['Date'] == date).sum()
                    log_runs(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
        else:
            log_runs("No valid dates found in dataset")
        
        log_runs(f"\n=== END DATE ANALYSIS ===\n")

def log_blank_key_analysis(df):
    log_runs(f"\n=========================")
    log_runs(f"=== BLANK/INVALID KEY ANALYSIS ===")
    log_runs(f"=========================")
    total_rows = len(df)
    for col in df.columns:
        blank_count = df[col].isna().sum() + (df[col].astype(str).str.strip() == '').sum()
        if blank_count > 0:
            log_runs(f"Column '{col}': {blank_count} blank/invalid out of {total_rows} rows")

def main(force_all=False):
    log(f"\n===== Starting Data Pipeline: {datetime.now()} =====")
    log(f"CONFIG: {CONFIG}")
    
    # Load last processed date
    last_processed = load_last_processed()["last_processed"]
    log(f"Last processed date: {last_processed}")
    
    if force_all:
        log("FORCE-ALL mode: Will process all files regardless of modification date")
    
    # Find Excel files
    input_dir = CONFIG['INPUT_DIR']
    pattern = CONFIG['FILE_PATTERN']
    output_parquet = CONFIG['OUTPUT_PARQUET']
    
    if not os.path.isdir(input_dir):
        log(f"ERROR: Directory not found: {input_dir}")
        sys.exit(1)
    
    excel_files = glob(os.path.join(input_dir, pattern))
    log(f"Found {len(excel_files)} Excel files.")
    
    if not excel_files:
        log("No Excel files found. Exiting.")
        sys.exit(0)
    
    # Filter files based on last processed date (unless force_all is True)
    if last_processed and not force_all:
        log("\nChecking file modification dates:")
        filtered_files = []
        for f in excel_files:
            mod_time = datetime.fromtimestamp(os.path.getmtime(f))
            mod_date = mod_time.strftime("%Y-%m-%d")
            log(f"File: {os.path.basename(f)}, Modified: {mod_date}")
            if mod_date > last_processed:
                filtered_files.append(f)
        excel_files = filtered_files
        log(f"\nFound {len(excel_files)} new or modified files to process.")
    elif force_all:
        log("\nFORCE-ALL mode: Processing all files:")
        for i, f in enumerate(excel_files, 1):
            log(f"File {i}: {os.path.basename(f)}")
    else:
        log("\nNo last processed date found. Processing all files:")
        for i, f in enumerate(excel_files, 1):
            log(f"File {i}: {os.path.basename(f)}")
    
    # Load files (parallel if enabled)
    dfs = []
    if CONFIG['PARALLEL_LOAD'] and len(excel_files) > 1:
        log(f"Loading files in parallel with {CONFIG['N_WORKERS']} workers...")
        with ThreadPoolExecutor(max_workers=CONFIG['N_WORKERS']) as executor:
            future_to_file = {executor.submit(load_excel, file): file for file in excel_files}
            for future in as_completed(future_to_file):
                file, df = future.result()
                if df is not None:
                    dfs.append(df)
    else:
        for file in excel_files:
            _, df = load_excel(file)
            if df is not None:
                dfs.append(df)
    
    if not dfs:
        log("No DataFrames loaded successfully. Exiting.")
        sys.exit(1)
    
    # Combine new data
    log(f"Concatenating {len(dfs)} DataFrames...")
    try:
        new_df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        log(f"ERROR during concatenation: {e}")
        sys.exit(1)
    
    # Validate new data
    new_df = validate_dataframe(new_df, "NEW DATA")
    
    # Clean and deduplicate new data
    new_df = clean_and_deduplicate(new_df)
    log(f"Cleaned new data shape: {new_df.shape}")
    
    # Load existing Parquet file if it exists
    if os.path.exists(output_parquet):
        existing_df = pd.read_parquet(output_parquet)
        log(f"Existing Parquet file shape: {existing_df.shape}")
        
        # Validate existing data
        existing_df = validate_dataframe(existing_df, "EXISTING DATA")
        
        # Combine existing and new data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        log(f"Combined data shape before deduplication: {combined_df.shape}")
        
        # Final deduplication
        combined_df = clean_and_deduplicate(combined_df)
        log(f"Final combined data shape: {combined_df.shape}")
    else:
        combined_df = new_df
        log("No existing Parquet file found. Using only new data.")
    
    # Validate final data
    combined_df = validate_dataframe(combined_df, "FINAL COMBINED DATA")
    # Date coverage analysis
    log_date_coverage(combined_df, label="FINAL COMBINED DATA")
    # Blank/invalid key analysis
    log_blank_key_analysis(combined_df)
    
    # Save the combined DataFrame to Parquet
    try:
        combined_df.to_parquet(output_parquet, index=False)
        log(f"Saved combined DataFrame to Parquet: {output_parquet}")
    except Exception as e:
        log(f"ERROR saving to Parquet: {e}")
    
    # Update the last processed date
    save_last_processed(datetime.now().strftime("%Y-%m-%d"))
    
    log("\n===== Pipeline Summary =====")
    log(f"Processed {len(excel_files)} files:")
    for file in excel_files:
        log(f"- {file}")
    log(f"Final Parquet file saved to: {output_parquet}")
    log(f"Total rows in final dataset: {len(combined_df)}")
    log(f"\n===== Pipeline Complete: {datetime.now()} =====")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Excel files and generate a combined DataFrame.")
    parser.add_argument("--force-all", action="store_true", help="Force process all files regardless of modification date")
    parser.add_argument("--reset-date", action="store_true", help="Reset last processed date to None")
    args = parser.parse_args()

    if args.reset_date:
        save_last_processed(None)
        print("Last processed date reset to None.")
        sys.exit(0)

    main(force_all=args.force_all)
