"""
Debug script to understand why rows are missing during parquet merge.
"""
import pandas as pd
import os
import sys
from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent.parent
src_path = str(project_root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Configuration
PARQUET_FILE = 'runs/combined_runs.parquet'
BOND = 'F 7 02/10/26'
DEALER = 'TD'
DATES_OF_INTEREST = ['2025-05-28', '2025-05-29', '2025-05-30']

print("=== PARQUET MERGE DEBUGGING ===")

# 1. Check if parquet file exists
if not os.path.exists(PARQUET_FILE):
    print(f"Parquet file does not exist: {PARQUET_FILE}")
    sys.exit(1)

# 2. Load existing parquet data
print(f"\n1. Loading existing parquet file...")
existing_df = pd.read_parquet(PARQUET_FILE)
print(f"   Total rows in existing parquet: {len(existing_df)}")

# Filter for our bond/dealer
existing_filtered = existing_df[
    (existing_df['Security'] == BOND) & 
    (existing_df['Dealer'] == DEALER)
]
print(f"   Rows for {BOND}, {DEALER}: {len(existing_filtered)}")

# Check for specific dates
print(f"\n   Checking for dates {DATES_OF_INTEREST}:")
for date in DATES_OF_INTEREST:
    rows = existing_filtered[existing_filtered['Date'] == date]
    print(f"   - {date}: {len(rows)} rows")
    if not rows.empty:
        print(f"     Time: {rows['Time'].iloc[0] if 'Time' in rows.columns else 'N/A'}")

# 3. Simulate what would happen with new data
print(f"\n2. Simulating merge with new data...")

# Create simulated new data (like what would come from Excel processing)
new_data = pd.DataFrame({
    'Date': ['2025-05-28', '2025-05-29', '2025-05-30'] * 2,  # 2 rows per date
    'Time': ['07:17', '09:00', '07:27', '11:00', '15:02', '16:00'],
    'Security': [BOND] * 6,
    'CUSIP': ['34527ACL2'] * 6,
    'Dealer': [DEALER] * 6,
    'Bid Price': [101.85, 101.86, 101.86, 101.87, 101.89, 101.90],
    'Ask Price': [102.00, 102.01, 102.02, 102.03, 102.02, 102.03],
    'Bid Spread': [165.0, 166.0, 165.0, 164.0, 160.0, 161.0]
})

print(f"   New data shape: {new_data.shape}")
print("\n   New data rows:")
print(new_data[['Date', 'Time', 'Bid Price', 'Ask Price', 'Bid Spread']].to_string(index=False))

# 4. Combine data (as the pipeline would)
print(f"\n3. Combining existing and new data...")
combined_df = pd.concat([existing_df, new_data], ignore_index=True)
print(f"   Combined shape before deduplication: {combined_df.shape}")

# Filter combined for our bond/dealer
combined_filtered = combined_df[
    (combined_df['Security'] == BOND) & 
    (combined_df['Dealer'] == DEALER)
]
print(f"   Combined rows for {BOND}, {DEALER}: {len(combined_filtered)}")

# Show all rows for our dates before deduplication
print(f"\n   All rows for dates {DATES_OF_INTEREST} before dedup:")
date_mask = combined_filtered['Date'].isin(DATES_OF_INTEREST)
rows_before = combined_filtered[date_mask].sort_values(['Date', 'Time'])
print(rows_before[['Date', 'Time', 'Bid Price', 'Ask Price', 'Bid Spread']].to_string(index=False))

# 5. Apply deduplication logic (matching ParquetProcessor._deduplicate_combined_data)
print(f"\n4. Applying deduplication (keeping last per Date/CUSIP/Dealer)...")

# Ensure Date is datetime
combined_df['Date'] = pd.to_datetime(combined_df['Date'])

# Sort by Date, CUSIP, Dealer, Time
sort_columns = ['Date', 'CUSIP', 'Dealer', 'Time']
combined_df = combined_df.sort_values(sort_columns)
print(f"   Sorted by: {sort_columns}")

# Deduplicate
key_columns = ['Date', 'CUSIP', 'Dealer']
before_dedup = len(combined_df)
deduped_df = combined_df.drop_duplicates(subset=key_columns, keep='last')
after_dedup = len(deduped_df)
print(f"   Rows removed by deduplication: {before_dedup - after_dedup}")

# Check what happened to our specific rows
deduped_filtered = deduped_df[
    (deduped_df['Security'] == BOND) & 
    (deduped_df['Dealer'] == DEALER)
]
print(f"\n   After dedup, rows for {BOND}, {DEALER}: {len(deduped_filtered)}")

# Check for specific dates after dedup
print(f"\n   Checking for dates {DATES_OF_INTEREST} after dedup:")
for date in DATES_OF_INTEREST:
    date_dt = pd.to_datetime(date)
    rows = deduped_filtered[deduped_filtered['Date'] == date_dt]
    print(f"   - {date}: {len(rows)} rows")
    if not rows.empty:
        print(f"     Time: {rows['Time'].iloc[0] if 'Time' in rows.columns else 'N/A'}")
        print(f"     Bid Price: {rows['Bid Price'].iloc[0]}")

# 6. Show which specific rows were kept
print(f"\n5. Rows kept after deduplication for our dates:")
date_mask = deduped_filtered['Date'].isin(pd.to_datetime(DATES_OF_INTEREST))
rows_after = deduped_filtered[date_mask].sort_values(['Date', 'Time'])
print(rows_after[['Date', 'Time', 'Bid Price', 'Ask Price', 'Bid Spread']].to_string(index=False))

print("\n=== END DEBUG ===") 