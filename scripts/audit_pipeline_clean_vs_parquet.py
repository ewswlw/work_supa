import pandas as pd
import glob
import os

BOND = 'F 7 02/10/26'
DEALER = 'TD'
EXCEL_PATTERN = 'runs/older files/*.xls*'
PARQUET_PATH = 'runs/combined_runs.parquet'

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

print(f'Loaded {len(raw_df)} raw rows for {BOND} from {DEALER}')

# 2. Simulate pipeline cleaning/deduplication
cleaned = raw_df.copy()
for col in ['Bid Price', 'Ask Price', 'Bid Size', 'Ask Size']:
    if col in cleaned.columns:
        cleaned = cleaned[cleaned[col] >= 0]
key_na_cols = ['Date', 'CUSIP', 'Dealer', 'Bid Spread']
existing_na_cols = [col for col in key_na_cols if col in cleaned.columns]
cleaned = cleaned.dropna(subset=existing_na_cols)
if 'Date' in cleaned.columns:
    cleaned['Date'] = pd.to_datetime(cleaned['Date'], errors='coerce')
sort_cols = [c for c in ['Date', 'CUSIP', 'Dealer', 'Time'] if c in cleaned.columns]
if sort_cols:
    cleaned = cleaned.sort_values(sort_cols)
key_cols = [c for c in ['Date', 'CUSIP', 'Dealer'] if c in cleaned.columns]
if len(key_cols) == 3:
    cleaned = cleaned.drop_duplicates(subset=key_cols, keep='last')
cleaned['Date'] = cleaned['Date'].dt.strftime('%Y-%m-%d')

print(f'After cleaning/deduplication: {cleaned.shape[0]} rows, {cleaned["Date"].nunique()} unique dates')

# 3. Load parquet data for this bond/dealer
try:
    parquet_df = pd.read_parquet(PARQUET_PATH)
    parquet_df = parquet_df[(parquet_df['Security'] == BOND) & (parquet_df['Dealer'] == DEALER)]
    parquet_df['Date'] = pd.to_datetime(parquet_df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
except Exception as e:
    print(f'Error reading parquet: {e}')
    parquet_df = pd.DataFrame()

print(f'Parquet: {parquet_df.shape[0]} rows, {parquet_df["Date"].nunique()} unique dates')

# 4. Compare dates
pipeline_dates = set(cleaned['Date'].dropna().unique())
parquet_dates = set(parquet_df['Date'].dropna().unique())
missing_in_parquet = pipeline_dates - parquet_dates
extra_in_parquet = parquet_dates - pipeline_dates
print(f'\nDates in cleaned raw but missing in parquet: {sorted(missing_in_parquet)}')
print(f'Dates in parquet but not in cleaned raw: {sorted(extra_in_parquet)}')

# 5. Show sample rows for missing dates
if missing_in_parquet:
    print('\nSample rows in cleaned raw but missing in parquet:')
    print(cleaned[cleaned['Date'].isin(missing_in_parquet)].to_string(index=False))
if extra_in_parquet:
    print('\nSample rows in parquet but not in cleaned raw:')
    print(parquet_df[parquet_df['Date'].isin(extra_in_parquet)].to_string(index=False))

print('\n=== DETAILED DATE LOSS AUDIT ===')
# 1. Count of raw rows per date
raw_counts = raw_df.groupby('Date').size().sort_index()
print('\nRaw row count per date:')
print(raw_counts)

# 2. After NA dropping
key_na_cols = ['Date', 'CUSIP', 'Dealer', 'Bid Spread']
existing_na_cols = [col for col in key_na_cols if col in raw_df.columns]
valid_mask = ~raw_df[existing_na_cols].isna().any(axis=1)
valid_df = raw_df[valid_mask].copy()
valid_counts = valid_df.groupby('Date').size().sort_index()
print('\nValid (non-NA) row count per date:')
print(valid_counts)

# 3. Dates lost due to NA dropping
raw_dates = set(raw_df['Date'].dropna().unique())
valid_dates = set(valid_df['Date'].dropna().unique())
lost_na_dates = sorted(list(raw_dates - valid_dates))
print(f'\nDates lost due to NA in drop columns: {lost_na_dates}')
if lost_na_dates:
    for d in lost_na_dates:
        print(f'\nRows for date {d} lost due to NA:')
        print(raw_df[raw_df['Date'] == d].to_string(index=False))

# 4. After deduplication
if 'Date' in valid_df.columns:
    valid_df['Date'] = pd.to_datetime(valid_df['Date'], errors='coerce')
sort_cols = [c for c in ['Date', 'CUSIP', 'Dealer', 'Time'] if c in valid_df.columns]
if sort_cols:
    valid_df = valid_df.sort_values(sort_cols)
key_cols = [c for c in ['Date', 'CUSIP', 'Dealer'] if c in valid_df.columns]
if len(key_cols) == 3:
    dedup_df = valid_df.drop_duplicates(subset=key_cols, keep='last')
else:
    dedup_df = valid_df.copy()
dedup_df['Date'] = dedup_df['Date'].dt.strftime('%Y-%m-%d')
dedup_counts = dedup_df.groupby('Date').size().sort_index()
print('\nDeduplicated row count per date:')
print(dedup_counts)

# 5. Dates lost due to deduplication (should be none, but for completeness)
valid_dates = set(valid_df['Date'].dropna().unique())
dedup_dates = set(dedup_df['Date'].dropna().unique())
lost_dedup_dates = sorted(list(valid_dates - dedup_dates))
print(f'\nDates lost due to deduplication: {lost_dedup_dates}')
if lost_dedup_dates:
    for d in lost_dedup_dates:
        print(f'\nRows for date {d} lost due to deduplication:')
        print(valid_df[valid_df['Date'] == d].to_string(index=False))

# 6. Summary
print(f'\nSummary:')
print(f'  Total unique dates in raw: {len(raw_dates)}')
print(f'  After NA dropping: {len(valid_dates)}')
print(f'  After deduplication: {len(dedup_dates)}')
print(f'  Dates lost due to NA: {len(lost_na_dates)}')
print(f'  Dates lost due to deduplication: {len(lost_dedup_dates)}')
print('\n=== END DETAILED DATE LOSS AUDIT ===')

print('\n=== END PIPELINE LOGIC SIMULATION ===')

# 7. Check for 2025-05-29 in deduplicated and parquet data
TARGET_DATE = '2025-05-29'
print(f'\n=== CHECK FOR {TARGET_DATE} ===')
row_dedup = dedup_df[dedup_df['Date'] == TARGET_DATE]
row_parquet = parquet_df[parquet_df['Date'] == TARGET_DATE]
print(f'Row in deduplicated cleaned data for {TARGET_DATE}:')
if not row_dedup.empty:
    print(row_dedup.to_string(index=False))
else:
    print('NOT FOUND in deduplicated cleaned data.')
print(f'Row in parquet file for {TARGET_DATE}:')
if not row_parquet.empty:
    print(row_parquet.to_string(index=False))
else:
    print('NOT FOUND in parquet file.')

# 8. Print all rows for the bond/dealer in parquet and dedup for dates around 2025-05-29
print('\n=== ROWS FOR 2025-05-28 to 2025-05-30 (inclusive) ===')
for df_name, df in [('Deduplicated cleaned data', dedup_df), ('Parquet file', parquet_df)]:
    print(f'\n{df_name}:')
    mask = df['Date'].isin(['2025-05-28', '2025-05-29', '2025-05-30'])
    cols = [c for c in ['Date', 'Time', 'CUSIP', 'Dealer', 'Bid Price', 'Ask Price', 'Bid Spread', 'SourceFile'] if c in df.columns]
    rows = df[mask]
    if not rows.empty:
        print(rows[cols].to_string(index=False))
    else:
        print('No rows found for these dates.') 