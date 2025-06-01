import pandas as pd
import glob
import os

BOND = 'F 7 02/10/26'
DEALER = 'TD'
EXCEL_PATTERN = 'runs/older files/*.xls*'
PARQUET_PATH = 'runs/combined_runs.parquet'
DROP_COLS = ['Date', 'CUSIP', 'Dealer', 'Bid Spread']

# 1. Load all raw Excel rows for this bond/dealer
files = glob.glob(EXCEL_PATTERN)
raw_rows = []
for f in files:
    try:
        df = pd.read_excel(f, engine='openpyxl')
        mask = (df['Security'] == BOND) & (df['Dealer'] == DEALER)
        sub = df.loc[mask].copy()
        if not sub.empty:
            sub['SourceFile'] = os.path.basename(f)
            raw_rows.append(sub)
    except Exception as e:
        print(f'{os.path.basename(f)}: ERROR ({e})')
if raw_rows:
    raw_df = pd.concat(raw_rows, ignore_index=True)
else:
    raw_df = pd.DataFrame()

# 2. Load parquet data for this bond/dealer
try:
    parquet_df = pd.read_parquet(PARQUET_PATH)
    parquet_df = parquet_df[(parquet_df['Security'] == BOND) & (parquet_df['Dealer'] == DEALER)]
except Exception as e:
    print(f'Error reading parquet: {e}')
    parquet_df = pd.DataFrame()

# 3. Normalize Date columns for comparison
raw_df['Date'] = pd.to_datetime(raw_df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
parquet_df['Date'] = pd.to_datetime(parquet_df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')

# 4. Find missing dates
raw_dates = set(raw_df['Date'].dropna().unique())
parquet_dates = set(parquet_df['Date'].dropna().unique())
missing_dates = sorted(list(raw_dates - parquet_dates))

print(f'Found {len(missing_dates)} missing dates (in raw but not in parquet)')

for date in missing_dates:
    print(f'\n=== AUDIT FOR DATE: {date} ===')
    # All raw rows for this date
    rows = raw_df[raw_df['Date'] == date].copy()
    print(f'Raw rows for {date}: {len(rows)}')
    if not rows.empty:
        print(rows[['Date','Time','CUSIP','Bid Price','Ask Price','Bid Spread','Dealer','SourceFile']].to_string(index=False))
        # Highlight rows with NA in drop columns
        na_mask = rows[DROP_COLS].isna().any(axis=1)
        if na_mask.any():
            print(f'Rows with NA in {DROP_COLS}:')
            print(rows[na_mask][['Date','Time','CUSIP','Bid Price','Ask Price','Bid Spread','Dealer','SourceFile']].to_string(index=False))
        else:
            print('No rows with NA in drop columns.')
        # Rows that would be kept after dropping NAs
        valid_rows = rows[~na_mask].copy()
        print(f'Rows remaining after dropping NAs: {len(valid_rows)}')
        if not valid_rows.empty:
            # Sort for deduplication (by Date, CUSIP, Dealer, Time if present)
            sort_cols = ['Date','CUSIP','Dealer']
            if 'Time' in valid_rows.columns:
                sort_cols.append('Time')
            valid_rows = valid_rows.sort_values(sort_cols)
            # Deduplicate: keep last
            dedup_row = valid_rows.drop_duplicates(subset=['Date','CUSIP','Dealer'], keep='last')
            print('Row that would be kept after deduplication:')
            print(dedup_row[['Date','Time','CUSIP','Bid Price','Ask Price','Bid Spread','Dealer','SourceFile']].to_string(index=False))
        else:
            print('No valid rows remain after dropping NAs.')
    else:
        print('No raw rows for this date.')
    # Parquet rows for this date
    pq_rows = parquet_df[parquet_df['Date'] == date]
    print(f'Rows in parquet for {date}: {len(pq_rows)}')
    if not pq_rows.empty:
        print(pq_rows[['Date','Time','CUSIP','Bid Price','Ask Price','Bid Spread','Dealer']].to_string(index=False))
    else:
        print('No rows in parquet for this date.')
    print('---')

print('\n=== PIPELINE LOGIC SIMULATION ON RAW DATA ===')
# 1. Remove rows with negative prices
cleaned = raw_df.copy()
for col in ['Bid Price', 'Ask Price', 'Bid Size', 'Ask Size']:
    if col in cleaned.columns:
        cleaned = cleaned[cleaned[col] >= 0]
# 2. Drop rows with NA in key columns
key_na_cols = ['Date', 'CUSIP', 'Dealer', 'Bid Spread']
existing_na_cols = [col for col in key_na_cols if col in cleaned.columns]
cleaned = cleaned.dropna(subset=existing_na_cols)
# 3. Ensure Date is datetime for sorting
if 'Date' in cleaned.columns:
    cleaned['Date'] = pd.to_datetime(cleaned['Date'], errors='coerce')
# 4. Sort
sort_cols = [c for c in ['Date', 'CUSIP', 'Dealer', 'Time'] if c in cleaned.columns]
if sort_cols:
    cleaned = cleaned.sort_values(sort_cols)
# 5. Deduplicate
key_cols = [c for c in ['Date', 'CUSIP', 'Dealer'] if c in cleaned.columns]
if len(key_cols) == 3:
    cleaned = cleaned.drop_duplicates(subset=key_cols, keep='last')

# Normalize date for comparison
cleaned['Date'] = cleaned['Date'].dt.strftime('%Y-%m-%d')

print(f'After cleaning/deduplication: {cleaned.shape[0]} rows, {cleaned["Date"].nunique()} unique dates')

# Compare to parquet
pipeline_dates = set(cleaned['Date'].dropna().unique())
parquet_dates = set(parquet_df['Date'].dropna().unique())
missing_in_parquet = pipeline_dates - parquet_dates
extra_in_parquet = parquet_dates - pipeline_dates
print(f'\nDates in cleaned raw but missing in parquet: {sorted(missing_in_parquet)}')
print(f'Dates in parquet but not in cleaned raw: {sorted(extra_in_parquet)}')

# Show sample rows for missing dates
if missing_in_parquet:
    print('\nSample rows in cleaned raw but missing in parquet:')
    print(cleaned[cleaned['Date'].isin(missing_in_parquet)].to_string(index=False))
if extra_in_parquet:
    print('\nSample rows in parquet but not in cleaned raw:')
    print(parquet_df[parquet_df['Date'].isin(extra_in_parquet)].to_string(index=False))

print('\n=== END PIPELINE LOGIC SIMULATION ===')

print('\nAudit complete.') 