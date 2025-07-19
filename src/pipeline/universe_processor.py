import pandas as pd
from pathlib import Path
import re
import json
from datetime import datetime
import yaml
import numpy as np
from logging import Logger
import io

from ..utils.validators import DataValidator
from ..utils.reporting import DataReporter

# --- Configuration Loading ---
def load_config():
    """Loads the main configuration file."""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    # Convert .inf strings to numpy.inf
    if 'bucketing' in config.get('universe_processor', {}):
        for bucket in config['universe_processor']['bucketing'].values():
            if 'bins' in bucket:
                bucket['bins'] = [np.inf if x == '.inf' else x for x in bucket['bins']]
    return config['universe_processor']

# --- Incremental Processing State ---
def get_file_metadata(file_path):
    """Get file modification time and size for change detection"""
    stat = file_path.stat()
    return {
        'name': file_path.name,
        'modified': stat.st_mtime,
        'size': stat.st_size
    }

def load_processing_state(logger: Logger):
    """Load the state of previously processed files"""
    state_file = Path(__file__).parent.parent.parent / 'universe' / 'processing_state.json'
    if state_file.exists():
        with open(state_file, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Could not decode processing_state.json. Starting fresh.")
                return {'processed_files': {}, 'last_processed': None}
    return {'processed_files': {}, 'last_processed': None}

def save_processing_state(processed_files):
    """Save the current processing state"""
    state_file = Path(__file__).parent.parent.parent / 'universe' / 'processing_state.json'
    state = {
        'processed_files': processed_files,
        'last_processed': datetime.now().isoformat()
    }
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)

def get_files_to_process(raw_data_path, existing_state, logger: Logger):
    """Determine which Excel files need processing"""
    files_to_process = []
    for file_path in raw_data_path.glob('*.xlsx'):
        current_metadata = get_file_metadata(file_path)
        file_name = file_path.name
        
        if file_name not in existing_state['processed_files']:
             files_to_process.append((file_path, current_metadata))
             logger.debug(f"Found new file to process: '{file_name}'")
        else:
            stored_metadata = existing_state['processed_files'][file_name]
            if (current_metadata['modified'] > stored_metadata.get('modified', 0) or
                current_metadata['size'] != stored_metadata.get('size')):
                files_to_process.append((file_path, current_metadata))
                logger.debug(f"Found modified file to process: '{file_name}'")
            else:
                logger.debug(f"Skipping '{file_name}' - already processed and unchanged")

    return files_to_process

# --- Main Processing Logic ---
def process_universe_files(logger: Logger, force_full_refresh: bool = False):
    """
    Reads all Excel files from 'universe/raw data', processes them incrementally,
    and updates the 'universe.parquet' file.
    
    Args:
        logger: Logger instance
        force_full_refresh: If True, ignores state tracking and processes ALL raw data
    """
    config = load_config()
    project_root = Path(__file__).parent.parent.parent
    raw_data_path = project_root / 'universe' / 'raw data'
    parquet_path = project_root / 'universe' / 'universe.parquet'
    
    if force_full_refresh:
        logger.info("🔄 FORCE FULL REFRESH: Ignoring state tracking, processing ALL universe files")
        existing_state = {'processed_files': {}, 'last_processed': None}
        # Get all Excel files for processing (with metadata tuple format)
        all_files = [f for f in raw_data_path.glob('*.xlsx') if f.is_file()]
        files_to_process = [(f, get_file_metadata(f)) for f in all_files]
        logger.info(f"📁 Found {len(files_to_process)} Excel files for full processing")
    else:
        existing_state = load_processing_state(logger)
        files_to_process = get_files_to_process(raw_data_path, existing_state, logger)
    
    existing_df = None
    if parquet_path.exists() and not force_full_refresh:
        logger.info("Found existing Parquet file. Loading for incremental processing...")
        try:
            existing_df = pd.read_parquet(parquet_path)
            logger.info(f"Loaded existing data: {existing_df.shape[0]} rows, {existing_df.shape[1]} columns")
            # Log DataFrame info after loading
            buf = io.StringIO()
            existing_df.info(buf=buf)
            logger.info("DataFrame info after loading from Parquet:\n" + buf.getvalue())
            
            # Log existing date coverage
            if 'Date' in existing_df.columns:
                existing_dates = sorted(existing_df['Date'].dropna().unique())
                logger.info(f"\n=========================")
                logger.info(f"=== EXISTING DATE COVERAGE ===")
                logger.info(f"=========================")
                logger.info(f"Total unique dates in existing data: {len(existing_dates)}")
                if existing_dates:
                    logger.info(f"Existing date range: {existing_dates[0]} to {existing_dates[-1]}")
                    logger.info(f"Existing dates in Parquet:")
                    for date in existing_dates:
                        date_count = (existing_df['Date'] == date).sum()
                        # Convert numpy datetime64 to pandas datetime for strftime
                        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
                        logger.info(f"  - {date_str}: {date_count} records")
        except Exception as e:
            logger.error(f"Error loading existing Parquet file: {e}. Will rebuild from scratch.")
            existing_df = None
            files_to_process = [(f, get_file_metadata(f)) for f in raw_data_path.glob('*.xlsx')]

    if not files_to_process:
        logger.info("\nNo new or modified files to process. The Parquet file is up-to-date.")
        return

    logger.info(f"\nFound {len(files_to_process)} files to process...")
    
    all_new_data = []
    processed_files_metadata = existing_state.get('processed_files', {}).copy()

    for file_path, metadata in files_to_process:
        logger.info(f"Processing '{file_path.name}'...")
        try:
            match = re.search(r'(\d{2}\.\d{2}\.\d{2})', file_path.name)
            if not match:
                logger.warning(f"Warning: Could not extract date from '{file_path.name}'. Skipping file.")
                continue
            
            # Extract and convert date to proper datetime object immediately
            report_date = pd.to_datetime(match.group(1), format='%m.%d.%y')
            logger.info(f"  Extracted date: {report_date.strftime('%Y-%m-%d')} (datetime type)")
            
            df = pd.read_excel(file_path, header=1)
            
            df.columns = df.iloc[0]
            df = df[1:]
            
            # Add Date column as datetime object
            df['Date'] = report_date
            logger.info(f"  Added Date column with dtype: {df['Date'].dtype}")
            logger.info(f"  Processed shape: {df.shape}")
            
            all_new_data.append(df)
            processed_files_metadata[file_path.name] = metadata
        except Exception as e:
            logger.error(f"Failed to process file {file_path.name}: {e}")

    if not all_new_data:
        logger.warning("No new data was loaded from the files.")
        return
        
    new_df = pd.concat(all_new_data, ignore_index=True)
    
    if existing_df is not None:
        files_being_reprocessed_dates = [pd.to_datetime(re.search(r'(\d{2}\.\d{2}\.\d{2})', f.name).group(1), format='%m.%d.%y') for f, m in files_to_process]
        existing_df = existing_df[~existing_df['Date'].isin(files_being_reprocessed_dates)]
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    logger.info("\n--- Starting Data Processing Pipeline ---")

    # Clean the data first
    combined_df.dropna(subset=['CUSIP'], inplace=True)
    logger.info(f"Dropped {new_df.shape[0] - combined_df.shape[0]} rows with null CUSIPs.")
    
    initial_rows = combined_df.shape[0]
    # Deduplicate by (Date, CUSIP), keep last
    if 'Date' in combined_df.columns and 'CUSIP' in combined_df.columns:
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        combined_df = combined_df.sort_values(['Date', 'CUSIP'])
        before_dedup = combined_df.shape[0]
        combined_df = combined_df.drop_duplicates(subset=['Date', 'CUSIP'], keep='last')
        logger.info(f"Dropped {before_dedup - combined_df.shape[0]} duplicate (Date, CUSIP) rows.")
    else:
        combined_df.drop_duplicates(inplace=True)
        logger.info(f"Dropped {initial_rows - combined_df.shape[0]} duplicate rows.")


    # --- Enhanced Validation and Reporting ---
    logger.info("--- Running Data Validation and Quality Analysis ---")
    validator = DataValidator(
        combined_df, 
        numeric_cols=config['validation']['numeric_columns']
    )
    validator.run_all_checks()

    # Log validation errors if any were found
    if validator.errors:
        error_report = DataReporter.generate_validation_error_report(validator.errors)
        logger.warning(error_report)
    else:
        logger.info("[OK] All data validation checks passed.")

    # Log the detailed data quality report
    quality_report = DataReporter.generate_data_quality_report(validator.results)
    logger.info(quality_report)
    # --- End Validation ---


    for bucket_name, b_config in config['bucketing'].items():
        col_name = b_config['column_name']
        if col_name in combined_df.columns:
            logger.debug(f"Creating bucket for '{col_name}'...")
            numeric_col = pd.to_numeric(combined_df[col_name], errors='coerce')
            combined_df[b_config['new_column_name']] = pd.cut(numeric_col, bins=b_config['bins'], labels=b_config['labels'], right=False)

    columns_to_keep = [
        'Date', 'CUSIP', 'Benchmark Cusip', 'Custom_Sector', 'Marketing Sector', 'Notes',
        'Bloomberg Cusip', 'Security', 'Benchmark', 'Make_Whole', 'Back End', 'Floating Index',
        'Stochastic Duration', 'Stochastic Convexity', 'Pricing Date', 'Pricing Date (Bench)',
        'MTD Return', 'QTD Return', 'YTD Return', 'MTD Bench Return', 'QTD Bench Return',
        'YTD Bench Return', 'Worst Date', 'Yrs (Worst)', 'YTC', 'Excess MTD', 'Excess YTD',
        'CPN TYPE', 'Ticker', 'G Sprd', 'Yrs (Cvn)', 'OAS (Mid)', 'Currency', 'CAD Equiv Swap',
        'G (RBC Crv)', 'vs BI', 'vs BCE', 'Equity Ticker', 'YTD Equity', 'MTD Equity',
        'Yrs Since Issue', 'Risk', 'Rating', 'Yrs (Mat)', 'Z Spread',
        'Yrs Since Issue Bucket', 'Yrs (Mat) Bucket'
    ]
    existing_cols_to_keep = [col for col in columns_to_keep if col in combined_df.columns]
    final_df = combined_df[existing_cols_to_keep]

    cols = ['Date'] + [col for col in final_df.columns if col != 'Date']
    final_df = final_df[cols]

    # --- Final Data Snapshot Report ---
    summary_report = DataReporter.generate_summary_report(final_df)
    logger.info(summary_report)
    # --- End Snapshot ---

    for col in final_df.columns:
        if final_df[col].dtype == 'object':
            final_df[col] = final_df[col].astype(str)

    # Convert specified columns to float
    float_columns = [
        'Make_Whole', 'Back End', 'Stochastic Duration', 'Stochastic Convexity',
        'MTD Return', 'QTD Return', 'YTD Return', 'MTD Bench Return', 'QTD Bench Return',
        'YTD Bench Return', 'Yrs (Worst)', 'YTC', 'Excess MTD', 'Excess YTD', 'G Sprd',
        'Yrs (Cvn)', 'OAS (Mid)', 'CAD Equiv Swap', 'G (RBC Crv)', 'vs BI', 'vs BCE',
        'YTD Equity', 'MTD Equity', 'Yrs Since Issue', 'Yrs (Mat)', 'Z Spread'
    ]
    for col in float_columns:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

    # Convert specified columns to datetime
    datetime_columns = ['Pricing Date', 'Pricing Date (Bench)', 'Worst Date']
    for col in datetime_columns:
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors='coerce')

    logger.info(f"--- Processing Complete ---")
    logger.info(f"Final DataFrame Shape: {final_df.shape}")
    
    # Log unique dates analysis
    if 'Date' in final_df.columns:
        unique_dates = sorted(final_df['Date'].dropna().unique())
        logger.info(f"\n=========================")
        logger.info(f"=== DATE COVERAGE ANALYSIS ===")
        logger.info(f"=========================")
        logger.info(f"Total unique dates: {len(unique_dates)}")
        logger.info(f"Date column dtype: {final_df['Date'].dtype}")
        
        if unique_dates:
            min_date = pd.to_datetime(unique_dates[0])
            max_date = pd.to_datetime(unique_dates[-1])
            
            logger.info(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
            
            # Calculate time span
            time_span = max_date - min_date
            logger.info(f"Time span: {time_span.days} days ({time_span.days/365.25:.1f} years)")
            
            # Year distribution analysis
            df_temp = final_df[final_df['Date'].notna()].copy()
            df_temp['year'] = df_temp['Date'].dt.year
            df_temp['weekday'] = df_temp['Date'].dt.day_name()
            
            year_dist = df_temp['year'].value_counts().sort_index()
            logger.info(f"\nYear distribution:")
            for year, count in year_dist.items():
                logger.info(f"  - {year}: {count} records")
            
            # Business day analysis
            business_days = df_temp[~df_temp['weekday'].isin(['Saturday', 'Sunday'])]
            weekend_days = df_temp[df_temp['weekday'].isin(['Saturday', 'Sunday'])]
            
            logger.info(f"\nBusiness day analysis:")
            logger.info(f"  - Business days: {len(business_days)} ({len(business_days)/len(df_temp)*100:.1f}%)")
            logger.info(f"  - Weekend days: {len(weekend_days)} ({len(weekend_days)/len(df_temp)*100:.1f}%)")
            
            # Sample dates with record counts
            logger.info(f"\nSample dates:")
            for i, date in enumerate(unique_dates[:5]):
                date_dt = pd.to_datetime(date)
                date_count = (final_df['Date'] == date).sum()
                logger.info(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
            
            if len(unique_dates) > 10:
                logger.info(f"  ... ({len(unique_dates) - 10} more dates) ...")
                for i, date in enumerate(unique_dates[-5:]):
                    date_dt = pd.to_datetime(date)
                    date_count = (final_df['Date'] == date).sum()
                    logger.info(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
        else:
            logger.warning("No valid dates found in dataset")
        
        logger.info(f"\n=== END DATE ANALYSIS ===\n")
    
    buf = io.StringIO()
    final_df.info(buf=buf)
    logger.info("DataFrame info before saving to Parquet:\n" + buf.getvalue())
    try:
        final_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        logger.info(f"Successfully saved updated DataFrame to '{parquet_path}'.")
        save_processing_state(processed_files_metadata)
        logger.info("Updated processing state file.")
    except Exception as e:
        logger.critical(f"Error saving to Parquet: {e}")

# --- Analytics Helper Functions ---
def filter_universe_by_date_range(df: pd.DataFrame, start_date: str, end_date: str, logger: Logger = None) -> pd.DataFrame:
    """Filter universe DataFrame by date range. Expects datetime Date column.
    
    Args:
        df: DataFrame with datetime Date column
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        logger: Optional logger for messages
        
    Returns:
        Filtered DataFrame
    """
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        raise ValueError("Date column must be datetime type for date filtering")
    
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    mask = (df['Date'] >= start_dt) & (df['Date'] <= end_dt)
    filtered_df = df[mask].copy()
    
    if logger:
        logger.info(f"Filtered universe by date range {start_date} to {end_date}: {len(filtered_df)} records")
    return filtered_df

def get_universe_latest_date(df: pd.DataFrame, logger: Logger = None) -> pd.DataFrame:
    """Get universe data for the most recent date.
    
    Args:
        df: DataFrame with datetime Date column
        logger: Optional logger for messages
        
    Returns:
        DataFrame filtered to the latest date
    """
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        raise ValueError("Date column must be datetime type for date operations")
    
    latest_date = df['Date'].max()
    latest_df = df[df['Date'] == latest_date].copy()
    
    if logger:
        latest_str = pd.to_datetime(latest_date).strftime('%Y-%m-%d')
        logger.info(f"Extracted latest universe date {latest_str}: {len(latest_df)} records")
    return latest_df

def get_universe_by_cusip(df: pd.DataFrame, cusip: str, logger: Logger = None) -> pd.DataFrame:
    """Get universe data for a specific CUSIP across all dates.
    
    Args:
        df: DataFrame with CUSIP and Date columns
        cusip: CUSIP identifier to filter by
        logger: Optional logger for messages
        
    Returns:
        DataFrame filtered to the specified CUSIP
    """
    if 'CUSIP' not in df.columns:
        raise ValueError("CUSIP column must be present for CUSIP filtering")
    
    cusip_df = df[df['CUSIP'] == cusip].copy()
    
    if logger:
        date_count = cusip_df['Date'].nunique() if 'Date' in cusip_df.columns else 0
        logger.info(f"Found CUSIP {cusip}: {len(cusip_df)} records across {date_count} dates")
    return cusip_df

def add_universe_date_features(df: pd.DataFrame, logger: Logger = None) -> pd.DataFrame:
    """Add useful date-based features for universe analytics.
    
    Args:
        df: DataFrame with datetime Date column
        logger: Optional logger for messages
        
    Returns:
        DataFrame with additional date features
    """
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        raise ValueError("Date column must be datetime type for feature extraction")
    
    df_enhanced = df.copy()
    
    # Add date features
    df_enhanced['Year'] = df_enhanced['Date'].dt.year
    df_enhanced['Month'] = df_enhanced['Date'].dt.month
    df_enhanced['Quarter'] = df_enhanced['Date'].dt.quarter
    df_enhanced['DayOfWeek'] = df_enhanced['Date'].dt.dayofweek  # Monday=0
    df_enhanced['DayName'] = df_enhanced['Date'].dt.day_name()
    df_enhanced['MonthName'] = df_enhanced['Date'].dt.month_name()
    df_enhanced['IsBusinessDay'] = df_enhanced['Date'].dt.weekday < 5
    df_enhanced['IsMonthEnd'] = df_enhanced['Date'].dt.is_month_end
    df_enhanced['IsQuarterEnd'] = df_enhanced['Date'].dt.is_quarter_end
    df_enhanced['IsYearEnd'] = df_enhanced['Date'].dt.is_year_end
    
    if logger:
        logger.info(f"Added {9} date features for universe analytics")
    return df_enhanced 