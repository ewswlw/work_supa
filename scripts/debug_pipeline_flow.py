"""
Debug script to trace data flow through the pipeline for specific rows.
"""
import pandas as pd
import os
import sys
from pathlib import Path
from glob import glob

# Add src to Python path
project_root = Path(__file__).parent.parent
src_path = str(project_root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pipeline.excel_processor import ExcelProcessor
from utils.config import ConfigManager
from utils.logging import LogManager

# Configuration
BOND = 'F 7 02/10/26'
DEALER = 'TD'
DATES_OF_INTEREST = ['2025-05-28', '2025-05-29', '2025-05-30']

print("=== PIPELINE DATA FLOW DEBUGGING ===")

# Initialize pipeline components
config_manager = ConfigManager("config/config.yaml")
logger = LogManager(
    log_file=config_manager.pipeline_config.log_file,
    log_level='DEBUG',
    log_format=config_manager.logging_config.format
)
excel_processor = ExcelProcessor(config_manager.pipeline_config, logger)

# 1. Check raw Excel files
print("\n1. Checking raw Excel files...")
pattern = os.path.join(config_manager.pipeline_config.input_dir, config_manager.pipeline_config.file_pattern)
excel_files = glob(pattern)

# Find files that might contain our dates
relevant_files = []
for file in excel_files:
    if any(date.replace('-', '.')[5:] in os.path.basename(file) for date in DATES_OF_INTEREST):
        relevant_files.append(file)

print(f"   Files that might contain our dates: {[os.path.basename(f) for f in relevant_files]}")

# 2. Load each file individually and check for our rows
print("\n2. Loading files individually...")
for file in relevant_files:
    print(f"\n   Loading {os.path.basename(file)}...")
    try:
        df = pd.read_excel(file, engine='openpyxl')
        
        # Parse dates
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True, errors='coerce')
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        
        # Filter for our bond/dealer
        filtered = df[
            (df['Security'] == BOND) & 
            (df['Dealer'] == DEALER)
        ]
        
        if not filtered.empty:
            print(f"      Found {len(filtered)} rows for {BOND}, {DEALER}")
            for date in DATES_OF_INTEREST:
                date_rows = filtered[filtered['Date'] == date]
                if not date_rows.empty:
                    print(f"      - {date}: {len(date_rows)} rows")
    except Exception as e:
        print(f"      Error loading file: {e}")

# 3. Process through Excel processor (single file processing)
print("\n3. Processing through ExcelProcessor._load_single_file()...")
for file in relevant_files[:1]:  # Just process one file as example
    print(f"\n   Processing {os.path.basename(file)}...")
    df = excel_processor._load_single_file(file)
    if df is not None:
        # Check for our rows
        filtered = df[
            (df['Security'] == BOND) & 
            (df['Dealer'] == DEALER)
        ]
        print(f"   After _load_single_file: {len(filtered)} rows for {BOND}, {DEALER}")
        
        # Apply cleaning and deduplication manually
        print("\n   Applying _clean_and_deduplicate()...")
        cleaned_df = excel_processor._clean_and_deduplicate(df)
        
        filtered_clean = cleaned_df[
            (cleaned_df['Security'] == BOND) & 
            (cleaned_df['Dealer'] == DEALER)
        ]
        print(f"   After _clean_and_deduplicate: {len(filtered_clean)} rows for {BOND}, {DEALER}")
        
        # Check specific dates
        for date in DATES_OF_INTEREST:
            date_rows = filtered_clean[filtered_clean['Date'] == date]
            if not date_rows.empty:
                print(f"   - {date}: {len(date_rows)} rows")
                if 'Time' in date_rows.columns:
                    print(f"     Time: {date_rows['Time'].iloc[0]}")

# 4. Full Excel processor process
print("\n4. Running full ExcelProcessor.process()...")
result = excel_processor.process()

if result.success and result.data is not None:
    df = result.data
    print(f"   Total rows processed: {len(df)}")
    
    # Check for our rows
    filtered = df[
        (df['Security'] == BOND) & 
        (df['Dealer'] == DEALER)
    ]
    print(f"   Rows for {BOND}, {DEALER}: {len(filtered)}")
    
    # Check specific dates
    print(f"\n   Checking for dates {DATES_OF_INTEREST}:")
    for date in DATES_OF_INTEREST:
        date_rows = filtered[filtered['Date'] == date]
        print(f"   - {date}: {len(date_rows)} rows")
        if not date_rows.empty:
            print(f"     Time: {date_rows['Time'].iloc[0] if 'Time' in date_rows.columns else 'N/A'}")
            print(f"     Bid Price: {date_rows['Bid Price'].iloc[0]}")
            print(f"     SourceFile: {date_rows['SourceFile'].iloc[0] if 'SourceFile' in date_rows.columns else 'N/A'}")
    
    # Show all rows for our bond/dealer
    print(f"\n   All dates for {BOND}, {DEALER}:")
    print(filtered[['Date', 'Time', 'Bid Price', 'SourceFile']].sort_values('Date').to_string(index=False))
else:
    print(f"   Excel processing failed: {result.message}")

print("\n=== END DEBUG ===") 