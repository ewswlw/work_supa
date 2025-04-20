"""
excel_to_df_debug.py

Highly configurable script to scan a directory for Excel files, load them into a single DataFrame (with parallel loading),
parse Date and Time columns as datetime/time objects (kept separate), print extensive debugging and integrity info, and output to Parquet.

Dependencies: pandas, openpyxl, pyarrow (for Parquet)
"""
import os
import sys
import pandas as pd
from glob import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== CONFIG SECTION =====================
CONFIG = {
    # Directory containing Excel files
    'INPUT_DIR': os.path.join(os.path.dirname(__file__), 'older files'),
    # Glob pattern for Excel files
    'FILE_PATTERN': '*.xls*',
    # Output Parquet file
    'OUTPUT_PARQUET': os.path.join(os.path.dirname(__file__), 'combined_runs.parquet'),
    # Date and Time formats (for parsing)
    'DATE_FORMAT': '%m/%d/%y',  # Excel date format in your files
    'TIME_FORMAT': '%H:%M',     # Excel time format in your files
    # Enable parallel loading
    'PARALLEL_LOAD': True,
    # Number of parallel workers (set to os.cpu_count() or override)
    'N_WORKERS': os.cpu_count() or 4,
    # How many rows to show in head/tail
    'SHOW_ROWS': 5,
}
# =================== END CONFIG SECTION ===================

def log(msg):
    print(f"[LOG] {msg}")

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

def main():
    log(f"CONFIG: {CONFIG}")
    input_dir = CONFIG['INPUT_DIR']
    pattern = CONFIG['FILE_PATTERN']
    output_parquet = CONFIG['OUTPUT_PARQUET']
    show_rows = CONFIG['SHOW_ROWS']
    # 1. Find Excel files
    log(f"Scanning directory: {input_dir}")
    if not os.path.isdir(input_dir):
        log(f"ERROR: Directory not found: {input_dir}")
        sys.exit(1)
    excel_files = glob(os.path.join(input_dir, pattern))
    log(f"Found {len(excel_files)} Excel files.")
    if not excel_files:
        log("No Excel files found. Exiting.")
        sys.exit(0)
    for i, f in enumerate(excel_files, 1):
        log(f"File {i}: {os.path.basename(f)}")
    # 2. Load files (parallel if enabled)
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
    # 3. Concatenate
    log(f"Concatenating {len(dfs)} DataFrames...")
    try:
        big_df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        log(f"ERROR during concatenation: {e}")
        sys.exit(1)
    log(f"Final DataFrame shape: {big_df.shape}")
    # === DATA CLEANING: Remove rows with any negative numeric values ===
    log(f"Shape before removing negatives: {big_df.shape}")
    num_before = big_df.shape[0]
    # Only check numeric columns for negatives
    big_df = big_df[~(big_df.select_dtypes(include='number') < 0).any(axis=1)]
    num_after = big_df.shape[0]
    log(f"Removed {num_before - num_after} rows with negative values. New shape: {big_df.shape}")

    # === SORT BY DATE COLUMN (earliest to latest) ===
    if 'Date' in big_df.columns:
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(big_df['Date']):
            try:
                big_df['Date'] = pd.to_datetime(big_df['Date'], errors='coerce')
                log("Converted 'Date' column to datetime.")
            except Exception as e:
                log(f"ERROR converting 'Date' to datetime: {e}")
        try:
            big_df = big_df.sort_values('Date', ascending=True)
            log("Sorted DataFrame by 'Date' (earliest to latest).")
        except Exception as e:
            log(f"ERROR sorting by 'Date': {e}")
    else:
        log("WARNING: 'Date' column not found, skipping sort.")

    # === SAVE TO PARQUET ===
    try:
        big_df.to_parquet(output_parquet, index=False)
        log(f"Saved cleaned DataFrame to Parquet: {output_parquet}")
    except Exception as e:
        log(f"ERROR saving to Parquet: {e}")

    # 4. Data Integrity & Debugging Output
    print("\n===== DataFrame INFO =====")
    big_df.info()
    print(f"\n===== DataFrame HEAD ({show_rows}) =====")
    print(big_df.head(show_rows))
    print(f"\n===== DataFrame TAIL ({show_rows}) =====")
    print(big_df.tail(show_rows))
    print("\n===== DataFrame DESCRIBE (all columns) =====")
    print(big_df.describe(include='all', datetime_is_numeric=True))
    print("\n===== Missing Values Per Column =====")
    print(big_df.isnull().sum())
    print("\n===== Duplicate Rows =====")
    print(f"Number of duplicate rows: {big_df.duplicated().sum()}")
    print("\n===== Columns with All NaN =====")
    all_nan_cols = [col for col in big_df.columns if big_df[col].isnull().all()]
    if all_nan_cols:
        print(f"Columns with all NaN values: {all_nan_cols}")
    else:
        print("No columns with all NaN values.")

    # 5. Deduplicate on ['Date', 'Dealer', 'CUSIP'] keeping most recent (latest Time)
    subset_cols = ['Date', 'Dealer', 'CUSIP']
    missing_cols = [col for col in subset_cols if col not in big_df.columns]
    if missing_cols:
        log(f"ERROR: Cannot deduplicate, missing columns: {missing_cols}")
    else:
        # Find duplicates
        dups_mask = big_df.duplicated(subset=subset_cols, keep=False)
        num_dups = dups_mask.sum()
        log(f"Duplicate rows on {subset_cols}: {num_dups}")
        if num_dups > 0:
            print("Sample duplicate rows (before deduplication):")
            print(big_df.loc[dups_mask].head(5))
            # Sort so latest Time is first in each group
            big_df['_TimeSort'] = big_df['Time'].apply(lambda t: (t.hour if t is not None else -1, t.minute if t is not None else -1))
            big_df = big_df.sort_values(by=subset_cols + ['_TimeSort'], ascending=[True, True, True, False])
            # Save sample of dropped rows
            to_drop = big_df.duplicated(subset=subset_cols, keep='first')
            dropped_sample = big_df.loc[to_drop].head(5)
            # Drop duplicates
            before_shape = big_df.shape
            big_df = big_df.drop_duplicates(subset=subset_cols, keep='first').drop(columns=['_TimeSort'])
            after_shape = big_df.shape
            log(f"Dropped {before_shape[0] - after_shape[0]} duplicate rows based on {subset_cols}, kept most recent Time.")
            print("Sample of dropped duplicates:")
            print(dropped_sample)
            # Confirm no duplicates remain
            num_dups_after = big_df.duplicated(subset=subset_cols, keep=False).sum()
            log(f"Duplicates remaining after deduplication: {num_dups_after}")
        else:
            log(f"No duplicates found on {subset_cols}.")

    # 6. Remove complete duplicate rows (all columns identical)
    num_full_dups = big_df.duplicated().sum()
    log(f"Complete duplicate rows (all columns identical): {num_full_dups}")
    if num_full_dups > 0:
        print("Sample of complete duplicates (before removal):")
        print(big_df[big_df.duplicated()].head(5))
        before_shape = big_df.shape
        big_df.drop_duplicates(inplace=True)
        after_shape = big_df.shape
        log(f"Dropped {before_shape[0] - after_shape[0]} complete duplicate rows. New shape: {after_shape}")
    else:
        log("No complete duplicate rows found.")

    # 7. Output to Parquet
    try:
        big_df.to_parquet(output_parquet, index=False)
        log(f"Saved combined DataFrame to Parquet: {output_parquet}")
    except Exception as e:
        log(f"ERROR saving to Parquet: {e}")
    log("Script completed.")

if __name__ == "__main__":
    main()
