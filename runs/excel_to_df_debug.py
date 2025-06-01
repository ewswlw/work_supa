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

Dependencies: pandas, openpyxl, pyarrow (for Parquet)
"""
import os
import sys
import json
import pandas as pd
from glob import glob
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def log(msg):
    """Log message to both file and console."""
    logging.info(msg)

def load_last_processed():
    """Load the last processed date from JSON file."""
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
            return data
    return {"last_processed": None}

def save_last_processed(date):
    """Save the last processed date to JSON file."""
    with open(CONFIG['LAST_PROCESSED_FILE'], "w") as f:
        json.dump({"last_processed": date}, f)

def validate_dataframe(df, stage=""):
    """Validate DataFrame and log statistics."""
    log(f"\n===== DataFrame Validation {stage} =====")
    log(f"Shape: {df.shape}")
    log(f"Columns: {df.columns.tolist()}")
    log(f"Data types:\n{df.dtypes}")
    
    # Check for nulls
    nulls = df.isnull().sum()
    log("\nNull values per column:")
    for col in df.columns:
        null_count = nulls[col]
        null_pct = (null_count / len(df)) * 100
        log(f"{col}: {null_count} ({null_pct:.2f}%)")
    
    # Check for duplicates
    dupes = df.duplicated().sum()
    log(f"\nDuplicate rows: {dupes}")
    
    # Basic statistics for numeric columns
    log("\nNumeric columns statistics:")
    log(str(df.describe()))
    
    return df

def parse_date_time_columns(df, file_name):
    """
    Parse 'Date' and 'Time' columns to datetime/time types (kept separate).
    Logs parsing errors and NaT/NaN counts.
    """
    if 'Date' in df.columns:
        try:
            df['Date'] = pd.to_datetime(df['Date'].astype(str).str.strip(), format=CONFIG['DATE_FORMAT'], errors='coerce')
            log(f"Parsed 'Date' in {os.path.basename(file_name)}; NaT count: {df['Date'].isna().sum()}")
        except Exception as e:
            log(f"ERROR parsing 'Date' in {file_name}: {e}")
    if 'Time' in df.columns:
        try:
            # Parse as datetime, then extract time
            times = pd.to_datetime(df['Time'].astype(str).str.strip(), format=CONFIG['TIME_FORMAT'], errors='coerce')
            df['Time'] = times.dt.time
            log(f"Parsed 'Time' in {os.path.basename(file_name)}; NaN count: {df['Time'].isna().sum()}")
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
    """Clean and deduplicate the DataFrame."""
    log(f"Starting clean_and_deduplicate with {len(df)} rows")
    
    # Remove negative values only from columns that should never be negative
    non_negative_cols = ['Bid Size', 'Ask Size', 'Bid Price', 'Ask Price']
    for col in non_negative_cols:
        if col in df.columns:
            before_count = len(df)
            df = df[df[col] >= 0]
            after_count = len(df)
            log(f"After filtering negative {col}: {after_count} rows (removed {before_count - after_count})")
    
    # Convert Date to string format for consistent deduplication
    log(f"Converting Date column to string...")
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    log(f"After Date conversion: {len(df)} rows")
    
    # Sort by Date and Time
    log(f"Sorting by Date and Time...")
    try:
        df = df.sort_values(['Date', 'Time'])
        log(f"After sorting: {len(df)} rows")
    except Exception as e:
        log(f"ERROR during sorting: {e}")
        log(f"Date column type: {df['Date'].dtype}")
        log(f"Time column type: {df['Time'].dtype}")
        log(f"Sample Date values: {df['Date'].head()}")
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
    
    # Convert Date back to datetime
    log(f"Converting Date back to datetime...")
    df['Date'] = pd.to_datetime(df['Date'])
    log(f"Final clean_and_deduplicate result: {len(df)} rows")
    
    return df

def main():
    log(f"\n===== Starting Data Pipeline: {datetime.now()} =====")
    log(f"CONFIG: {CONFIG}")
    
    # Load last processed date
    last_processed = load_last_processed()["last_processed"]
    log(f"Last processed date: {last_processed}")
    
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
    
    # Filter files based on last processed date
    if last_processed:
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
    
    # List files to be processed
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
    main()
