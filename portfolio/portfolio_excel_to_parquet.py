import os
import re
import sys
import pandas as pd
from pathlib import Path
import datetime

RAW_DATA_DIR = Path(__file__).parent / 'raw data'
OUTPUT_PARQUET = Path(__file__).parent / 'portfolio.parquet'

# Regex to extract date from filename (e.g., Aggies 06.19.25.xlsx)
FILENAME_DATE_RE = re.compile(r'(\d{2})\.(\d{2})\.(\d{2})')

def extract_date_from_filename(filename):
    match = FILENAME_DATE_RE.search(filename)
    if not match:
        raise ValueError(f"No date found in filename: {filename}")
    mm, dd, yy = match.groups()
    yyyy = f"20{yy}" if int(yy) < 50 else f"19{yy}"  # crude Y2K logic
    return f"{mm}/{dd}/{yyyy}"

def main():
    print(f"Scanning directory: {RAW_DATA_DIR}")
    files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith('.xlsx')]
    print(f"Found {len(files)} Excel files: {files}")
    
    # Check for duplicate dates in filenames
    date_map = {}
    for fname in files:
        date_str = extract_date_from_filename(fname)
        if date_str in date_map:
            print(f"ERROR: Duplicate date {date_str} found in files: {date_map[date_str]} and {fname}")
            sys.exit(1)
        date_map[date_str] = fname
    print(f"All dates from filenames: {list(date_map.keys())}")
    
    all_dfs = []
    for fname in files:
        fpath = RAW_DATA_DIR / fname
        print(f"\nReading file: {fname}")
        df = pd.read_excel(fpath)
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        # Check for existing 'Date' column
        if any(col.lower() == 'date' for col in df.columns):
            print(f"ERROR: File {fname} already contains a 'Date' column. Please resolve this before proceeding.")
            sys.exit(1)
        print(f"  Head:\n{df.head(2)}")
        date_str = extract_date_from_filename(fname)
        # Insert Date as first column, formatted as mm/dd/yyyy
        df.insert(0, 'Date', pd.to_datetime(date_str, format='%m/%d/%Y').strftime('%m/%d/%Y'))
        print(f"  After adding Date column:\n{df.head(2)}")
        all_dfs.append(df)
    
    print("\nConcatenating all dataframes...")
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Combined shape: {combined.shape}")
    print(f"Combined columns: {list(combined.columns)}")
    print(f"Combined head:\n{combined.head(5)}")

    # Data cleaning steps
    # 1. Delete rows with missing values in 'SECURITY'
    before_rows = combined.shape[0]
    combined = combined[combined['SECURITY'].notna()]
    after_rows = combined.shape[0]
    print(f"Removed {before_rows - after_rows} rows with missing SECURITY.")

    # 2. Drop specified columns
    columns_to_drop = [
        'BBG YIELD SPREAD', 'CURRENT YIELD', 'BBG 1D CHANGE', 'CHANGE', 'CHANGE PCT', 'CHANGE BPS NAV',
        'INDUSTRY', 'SECTOR', 'OWNER', 'DAY PROFIT', 'AVERAGE PRICE', 'PROFIT', 'REALIZED', 'UNREALIZED',
        'TOTAL COST SETTLE CCY', 'TOTAL COST', 'COST PCT NAV', 'INTEREST', 'REALIZED SETTLE CCY',
        'PAID INTEREST LOCAL CCY', 'PAID INTEREST', 'UNREALIZED SETTLE CCY', 'FX EXPOSURE LOCAL CCY',
        'FX EXPOSURE DISPLAY CCY', 'FX EXPOSURE PCT NAV', 'PREVIOUS PROFIT', 'PREVIOUS DAY PRICE',
        'ACCRUED INTEREST', 'ANNUALIZED INCOME', 'BBG MTD YIELD CHANGE'
    ]
    drop_cols = [col for col in columns_to_drop if col in combined.columns]
    if drop_cols:
        print(f"Dropping columns: {drop_cols}")
        combined.drop(columns=drop_cols, inplace=True)
    else:
        print("No specified columns to drop.")

    # 3. Apply custom CUSIP/SECURITY logic
    # Rule 1: If SECURITY TYPE == 'CDX', set SECURITY = 'CDX' and CUSIP = 460
    mask_cdx = combined['SECURITY TYPE'] == 'CDX'
    print(f"Rows where SECURITY TYPE == 'CDX': {mask_cdx.sum()}")
    combined.loc[mask_cdx, 'SECURITY'] = 'CDX'
    combined.loc[mask_cdx, 'CUSIP'] = 460

    # Rule 2: If SECURITY == 'CASH CAD', set CUSIP = 123
    mask_cash_cad = combined['SECURITY'] == 'CASH CAD'
    print(f"Rows where SECURITY == 'CASH CAD': {mask_cash_cad.sum()}")
    combined.loc[mask_cash_cad, 'CUSIP'] = 123

    # Rule 3: If SECURITY == 'CASH USD', set CUSIP = 789
    mask_cash_usd = combined['SECURITY'] == 'CASH USD'
    print(f"Rows where SECURITY == 'CASH USD': {mask_cash_usd.sum()}")
    combined.loc[mask_cash_usd, 'CUSIP'] = 789

    # Ensure Date is the first column and is not the index
    if combined.index.name == 'Date':
        combined.reset_index(inplace=True)
    cols = list(combined.columns)
    if cols[0] != 'Date':
        cols.remove('Date')
        cols = ['Date'] + cols
        combined = combined[cols]

    # Drop unwanted columns
    drop_cols = [col for col in ['Unnamed: 0', 'Unnamed: 1'] if col in combined.columns]
    if drop_cols:
        print(f"Dropping columns: {drop_cols}")
        combined.drop(columns=drop_cols, inplace=True)
    else:
        print("No 'Unnamed: 0' or 'Unnamed: 1' columns to drop.")

    print(f"\nFinal DataFrame info:")
    print(combined.info())
    print(combined.head(5))
    
    # Ensure CUSIP is string type for Parquet compatibility
    if 'CUSIP' in combined.columns:
        combined['CUSIP'] = combined['CUSIP'].astype(str)

    # Ensure Date is a datetime object before saving (Parquet will preserve this type)
    if 'Date' in combined.columns:
        combined['Date'] = pd.to_datetime(combined['Date'], format='%m/%d/%Y')
        print(f"Date column dtype before saving: {combined['Date'].dtype}")

    # Save to Parquet
    print(f"\nSaving to Parquet: {OUTPUT_PARQUET}")
    combined.to_parquet(OUTPUT_PARQUET, index=False)
    print("Done! Parquet file created. (Date column is datetime64[ns] in Parquet)")

    # Save to CSV in processed data directory
    processed_data_dir = Path(__file__).parent / 'processed data'
    processed_data_dir.mkdir(exist_ok=True)
    output_csv = processed_data_dir / 'portfolio.csv'
    try:
        print(f"Saving to CSV: {output_csv}")
        combined.to_csv(output_csv, index=False)
        print("Done! CSV file created. (When reading, use parse_dates=['Date'] for Date column)")
    except PermissionError:
        # If file is locked, save with a timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        alt_csv = processed_data_dir / f'portfolio_{timestamp}.csv'
        print(f"Permission denied for {output_csv}, saving as {alt_csv} instead.")
        combined.to_csv(alt_csv, index=False)
        print(f"Done! CSV file created as {alt_csv}. (When reading, use parse_dates=['Date'] for Date column)")

    return combined

def print_blank_cusip_rows(df):
    blank_cusip = df[df['CUSIP'].isna() | (df['CUSIP'].astype(str).str.strip() == '')]
    print(f"\nRows with blank CUSIP ({len(blank_cusip)} rows):")
    print(blank_cusip)
    print(f"Total rows with blank CUSIP: {len(blank_cusip)}")

if __name__ == '__main__':
    final_df = main()
    print_blank_cusip_rows(final_df) 