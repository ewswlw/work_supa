"""
Debug why rows are being removed during cleaning.
"""
import pandas as pd
import os

# Configuration
EXCEL_FILE = 'runs/older files/RUNS 05.28.25.xlsx'
BOND = 'F 7 02/10/26'
DEALER = 'TD'

print("=== DEBUGGING CLEANING ISSUE ===")

# Load the Excel file
print(f"\n1. Loading {EXCEL_FILE}...")
df = pd.read_excel(EXCEL_FILE, engine='openpyxl')
print(f"   Total rows loaded: {len(df)}")

# Parse Date column
df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True, errors='coerce')
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

# Filter for our bond/dealer
filtered = df[(df['Security'] == BOND) & (df['Dealer'] == DEALER)]
print(f"   Rows for {BOND}, {DEALER}: {len(filtered)}")

if not filtered.empty:
    print("\n2. Examining the row before cleaning:")
    print(f"   Date: {filtered['Date'].iloc[0]}")
    print(f"   CUSIP: {filtered['CUSIP'].iloc[0]}")
    print(f"   Dealer: {filtered['Dealer'].iloc[0]}")
    print(f"   Bid Spread: {filtered['Bid Spread'].iloc[0]}")
    print(f"   Bid Price: {filtered['Bid Price'].iloc[0]}")
    print(f"   Ask Price: {filtered['Ask Price'].iloc[0]}")
    print(f"   Bid Size: {filtered['Bid Size'].iloc[0]}")
    print(f"   Ask Size: {filtered['Ask Size'].iloc[0]}")
    
    # Check for negative values
    print("\n3. Checking for negative values:")
    print(f"   Bid Price < 0: {(filtered['Bid Price'] < 0).any()}")
    print(f"   Ask Price < 0: {(filtered['Ask Price'] < 0).any()}")
    print(f"   Bid Size < 0: {(filtered['Bid Size'] < 0).any()}")
    print(f"   Ask Size < 0: {(filtered['Ask Size'] < 0).any()}")
    
    # Check for NA values in key columns
    print("\n4. Checking for NA values in key columns:")
    key_cols = ['Date', 'CUSIP', 'Dealer', 'Bid Spread']
    for col in key_cols:
        na_check = filtered[col].isna().any()
        print(f"   {col} has NA: {na_check}")
        if na_check:
            print(f"     Value: {filtered[col].iloc[0]}")

# Now apply the same cleaning logic as the pipeline
print("\n5. Applying cleaning logic step by step...")

# Step 1: Remove negative prices
initial_count = len(df)
for col in ['Bid Price', 'Ask Price', 'Bid Size', 'Ask Size']:
    before = len(df)
    df = df[df[col] >= 0]
    after = len(df)
    if before != after:
        print(f"   Removed {before - after} rows with negative {col}")

# Check if our row survived
filtered_after_neg = df[(df['Security'] == BOND) & (df['Dealer'] == DEALER)]
print(f"   Rows for {BOND}, {DEALER} after negative removal: {len(filtered_after_neg)}")

# Step 2: Drop NA in key columns
key_na_cols = ['Date', 'CUSIP', 'Dealer', 'Bid Spread']
before_na = len(df)
df = df.dropna(subset=key_na_cols)
after_na = len(df)
print(f"   Removed {before_na - after_na} rows with NA in {key_na_cols}")

# Check if our row survived
filtered_after_na = df[(df['Security'] == BOND) & (df['Dealer'] == DEALER)]
print(f"   Rows for {BOND}, {DEALER} after NA removal: {len(filtered_after_na)}")

if len(filtered_after_na) == 0 and len(filtered) > 0:
    print("\n6. ROW WAS REMOVED! Checking why...")
    
    # Re-load and check each condition
    df_fresh = pd.read_excel(EXCEL_FILE, engine='openpyxl')
    df_fresh['Date'] = pd.to_datetime(df_fresh['Date'], infer_datetime_format=True, errors='coerce')
    df_fresh['Date'] = df_fresh['Date'].dt.strftime('%Y-%m-%d')
    
    row = df_fresh[(df_fresh['Security'] == BOND) & (df_fresh['Dealer'] == DEALER)].iloc[0]
    
    print("\n   Checking each removal condition:")
    print(f"   - Bid Price ({row['Bid Price']}) < 0: {row['Bid Price'] < 0}")
    print(f"   - Ask Price ({row['Ask Price']}) < 0: {row['Ask Price'] < 0}")
    print(f"   - Bid Size ({row['Bid Size']}) < 0: {row['Bid Size'] < 0}")
    print(f"   - Ask Size ({row['Ask Size']}) < 0: {row['Ask Size'] < 0}")
    print(f"   - Date is NA: {pd.isna(row['Date'])}")
    print(f"   - CUSIP is NA: {pd.isna(row['CUSIP'])}")
    print(f"   - Dealer is NA: {pd.isna(row['Dealer'])}")
    print(f"   - Bid Spread is NA: {pd.isna(row['Bid Spread'])}")
    
    # Check entire row for our bond
    print("\n   Full row data:")
    print(row)

print("\n=== END DEBUG ===") 