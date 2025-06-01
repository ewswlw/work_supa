import pandas as pd
import glob
import os
import numpy as np

files = glob.glob('runs/older files/*.xls*')
total = 0
all_dates = set()
print('File-by-file counts for F 7 02/10/26 from TD:')
for f in files:
    try:
        df = pd.read_excel(f, engine='openpyxl')
        mask = (df['Security'] == 'F 7 02/10/26') & (df['Dealer'] == 'TD')
        count = df[mask].shape[0]
        dates = df.loc[mask, 'Date'].astype(str).tolist()
        all_dates.update(dates)
        print(f'{os.path.basename(f)}: {count}')
        total += count
    except Exception as e:
        print(f'{os.path.basename(f)}: ERROR ({e})')
print('---')
print(f'Total: {total}')
print('---')
print('Unique dates for F 7 02/10/26 from TD:')
for d in sorted(all_dates, reverse=True):
    print(d)
print(f'Total unique dates: {len(all_dates)}')

print('\n---')
print('Unique dates for F 7 02/10/26 from TD in parquet:')
try:
    df_parquet = pd.read_parquet('runs/combined_runs.parquet')
    parquet_dates = set(df_parquet[(df_parquet['Security'] == 'F 7 02/10/26') & (df_parquet['Dealer'] == 'TD')]['Date'].astype(str))
    for d in sorted(parquet_dates, reverse=True):
        print(d)
    print(f'Total unique dates in parquet: {len(parquet_dates)}')
except Exception as e:
    print(f'Error reading parquet: {e}')

# Audit: Dates in raw but missing from parquet
missing_dates = set(all_dates) - set(parquet_dates)
print('\n---')
print('Dates in raw data but missing from parquet:')
for d in sorted(missing_dates, reverse=True):
    print(d)
print(f'Total missing dates: {len(missing_dates)}')

# Audit: Rows in raw data with missing or invalid Date
print('\n---')
print('Rows in raw data for F 7 02/10/26 from TD with missing or invalid Date:')
for f in files:
    try:
        df = pd.read_excel(f, engine='openpyxl')
        mask = (df['Security'] == 'F 7 02/10/26') & (df['Dealer'] == 'TD')
        sub = df.loc[mask]
        # Check for missing or invalid dates
        invalid = sub[sub['Date'].isnull() | (sub['Date'] == '')]
        if not invalid.empty:
            print(f'{os.path.basename(f)}:')
            print(invalid)
    except Exception as e:
        print(f'{os.path.basename(f)}: ERROR ({e})')

# Concatenate all raw rows for this bond and dealer
print('\n---')
print('Unique raw Date values and types for F 7 02/10/26 from TD:')
all_rows = []
for f in files:
    try:
        df = pd.read_excel(f, engine='openpyxl')
        mask = (df['Security'] == 'F 7 02/10/26') & (df['Dealer'] == 'TD')
        sub = df.loc[mask]
        if not sub.empty:
            all_rows.append(sub)
    except Exception as e:
        print(f'{os.path.basename(f)}: ERROR ({e})')
if all_rows:
    combined = pd.concat(all_rows, ignore_index=True)
    unique_dates = combined['Date'].unique()
    for d in unique_dates:
        print(f'Value: {repr(d)}  Type: {type(d)}')
    print(f'Total unique raw Date values: {len(unique_dates)}')
    print('\nSample of raw data:')
    print(combined[['Date','Time','Bid Price','Ask Price']].head(10))
else:
    print('No rows found for this bond and dealer.') 