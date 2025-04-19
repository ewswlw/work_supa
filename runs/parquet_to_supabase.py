"""
parquet_to_supabase.py

Loads data from a Parquet file into a Supabase table using the supabase-py library.
- Reads credentials from .env
- Uses original column names throughout (no snake_case conversion)
- Converts Date to date, Time to HH:MM string (no renaming)
- Inserts in batches, fails batch on error
- Logs actions and errors
- Supabase table schema must exactly match original DataFrame column names (with spaces and case)

Run in Poetry environment: poetry run python runs/parquet_to_supabase.py
"""
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
import time as pytime
import re

import math

def clean_nans(obj):
    """Recursively replace all float('nan') with None for JSON/SQL compatibility."""
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nans(x) for x in obj]
    return obj

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "runs")
PARQUET_PATH = os.path.join(os.path.dirname(__file__), "combined_runs.parquet")
BATCH_SIZE = 1000

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Supabase credentials not set in .env")
    sys.exit(1)

DEBUG_FILE = os.path.join(os.path.dirname(__file__), '..', 'supabase_etl_debug.txt')
def log(msg):
    full_msg = f"[LOG] {msg}"
    print(full_msg)
    with open(DEBUG_FILE, 'a', encoding='utf-8') as f:
        f.write(full_msg + '\n')
def debug_log(msg):
    with open(DEBUG_FILE, 'a', encoding='utf-8') as f:
        f.write(str(msg) + '\n')
    print(f"[DEBUG] {msg}")

# --- No snake_case conversion. Use original column names throughout. ---
def convert_types(df):
    # No column renaming! Only type conversions if necessary
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        df['Date'] = df['Date'].astype(str)  # Ensure JSON serializable
    if 'Time' in df.columns:
        df['Time'] = df['Time'].apply(lambda t: t.strftime('%H:%M') if pd.notnull(t) and hasattr(t, 'strftime') else (t if pd.isnull(t) else str(t)))
    return df

def fetch_supabase_table_schema(supabase: Client, table: str):
    try:
        # Fetch a single row to get column names from API
        resp = supabase.table(table).select('*').limit(1).execute()
        if hasattr(resp, 'data') and resp.data:
            return list(resp.data[0].keys())
        elif hasattr(resp, 'data') and resp.data == []:
            # Table exists but is empty, use information_schema
            # (This is a fallback, not always available via API)
            debug_log("Table is empty; using known schema.")
            return None
        else:
            debug_log(f"No schema info from Supabase API: {resp}")
            return None
    except Exception as e:
        debug_log(f"Error fetching Supabase schema: {e}")
        return None

def batch_insert(supabase: Client, df: pd.DataFrame, table: str, batch_size: int = 1000):
    total = len(df)
    # Log DataFrame columns and dtypes
    debug_log(f"DataFrame columns: {df.columns.tolist()}")
    debug_log(f"DataFrame dtypes: {df.dtypes}")
    debug_log(f"DataFrame shape: {df.shape}")
    # Fetch and log Supabase table schema
    api_schema = fetch_supabase_table_schema(supabase, table)
    if api_schema:
        debug_log(f"Supabase API table columns: {api_schema}")
        missing_in_df = set(api_schema) - set(df.columns)
        missing_in_api = set(df.columns) - set(api_schema)
        debug_log(f"Columns in Supabase but missing in DataFrame: {missing_in_df}")
        debug_log(f"Columns in DataFrame but missing in Supabase: {missing_in_api}")
        # Column-by-column check
        all_cols = sorted(set(api_schema) | set(df.columns))
        for col in all_cols:
            in_df = col in df.columns
            in_api = col in api_schema
            dtype = str(df[col].dtype) if in_df else 'N/A'
            all_nan = df[col].isnull().all() if in_df else 'N/A'
            msg = f"Column: '{col}' | In DataFrame: {in_df} | In Supabase: {in_api} | dtype: {dtype} | all-NaN: {all_nan}"
            if not in_df:
                debug_log(f"[WARNING] Column in Supabase but missing in DataFrame: {msg}")
            elif not in_api:
                debug_log(f"[WARNING] Column in DataFrame but missing in Supabase: {msg}")
            else:
                debug_log(msg)
        if missing_in_df:
            debug_log(f"[FATAL] Columns in Supabase but missing in DataFrame: {missing_in_df}")
            sys.exit(1)
        if missing_in_api:
            debug_log(f"[FATAL] Columns in DataFrame but missing in Supabase: {missing_in_api}")
            sys.exit(1)
    else:
        debug_log("Could not fetch Supabase schema via API; table may be empty or API cache issue.")
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = df.iloc[start:end].to_dict(orient='records')
        # Clean all NaN values in batch dicts before upserting
        batch = [clean_nans(row) for row in batch]
        log(f"Inserting rows {start} to {end-1}...")
        # Log keys and a sample record for the first batch
        if start == 0 and batch:
            debug_log(f"First batch keys: {list(batch[0].keys())}")
            debug_log(f"First batch sample record: {batch[0]}")
        # Check for all-NaN columns and unexpected types in the batch
        batch_keys = set(batch[0].keys()) if batch else set()
        for col in batch_keys:
            values = [row.get(col, None) for row in batch]
            all_nan = all(pd.isnull(v) or v is None for v in values)
            if all_nan:
                debug_log(f"[WARNING] Column '{col}' is all-NaN/None in batch {start}-{end-1}")
            # Check for unexpected types
            for v in values:
                if isinstance(v, (list, dict)):
                    debug_log(f"[FATAL] Column '{col}' has non-scalar value in batch {start}-{end-1}: {v}")
                    sys.exit(1)
        # Validate batch keys match Supabase columns
        if api_schema:
            extra_keys = batch_keys - set(api_schema)
            missing_keys = set(api_schema) - batch_keys
            if extra_keys:
                debug_log(f"[FATAL] Batch keys not in Supabase: {extra_keys}")
                sys.exit(1)
            if missing_keys:
                debug_log(f"[FATAL] Supabase columns missing in batch: {missing_keys}")
                sys.exit(1)
        try:
            resp = supabase.table(table).upsert(batch).execute()
            if hasattr(resp, 'status_code') and resp.status_code >= 400:
                debug_log(f"ERROR: Batch {start}-{end-1} failed: {getattr(resp, 'data', None)}")
                debug_log(f"Full response: {resp}")
                if hasattr(resp, 'json'):
                    debug_log(f"Response JSON: {resp.json}")
                debug_log(f"Failing batch sample: {batch[0] if batch else 'EMPTY'}")
                sys.exit(1)
        except Exception as e:
            import traceback
            debug_log(f"Exception during batch insert {start}-{end-1}: {e}")
            debug_log(traceback.format_exc())
            debug_log(f"Failing batch sample: {batch[0] if batch else 'EMPTY'}")
            sys.exit(1)
        pytime.sleep(0.2)  # avoid rate limits
    log(f"Inserted {total} rows into {table}.")

def main():
    log(f"Connecting to Supabase at {SUPABASE_URL}...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    log(f"Reading Parquet file: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    log(f"Loaded DataFrame shape: {df.shape}")
    # Fix column names to match Supabase schema exactly (case and spaces)
    df.rename(columns={
        'Bid Yield To Convention': 'Bid Yield to Convention',
        'Ask Yield To Convention': 'Ask Yield to Convention'
    }, inplace=True)
    df = convert_types(df)
    # Convert all NaN to None for JSON/SQL compatibility
    df = df.where(pd.notnull(df), None)
    log(f"Converted DataFrame columns: {df.columns.tolist()}")
    log(f"DataFrame dtypes before upsert: {df.dtypes}")  # Debugging: print dtypes
    # Clean debug file at the start of each run
    with open(DEBUG_FILE, 'w', encoding='utf-8') as f:
        f.write('--- Supabase ETL Debug Log ---\n')
    batch_insert(supabase, df, SUPABASE_TABLE, BATCH_SIZE)
    log("ETL complete.")

if __name__ == "__main__":
    main()
