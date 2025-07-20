"""
Portfolio file processing module.
Handles Excel files from portfolio/raw data and processes them into Parquet format.
"""
import pandas as pd
from pathlib import Path
import re
import json
import os
from datetime import datetime
import yaml
import numpy as np
from logging import Logger
import io

from ..utils.validators import DataValidator
from ..utils.reporting import DataReporter

# --- Configuration Loading ---
def load_config():
    """Loads the portfolio processor configuration."""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config['portfolio_processor']

# --- File Processing State Management ---
def get_file_metadata(file_path):
    """Get file modification time and size for change detection"""
    stat = file_path.stat()
    return {
        'name': file_path.name,
        'modified': stat.st_mtime,
        'size': stat.st_size
    }

def load_processing_state(state_file_path, logger: Logger):
    """Load the state of previously processed files"""
    if state_file_path.exists():
        with open(state_file_path, 'r') as f:
            try:
                state = json.load(f)
                logger.debug(f"Loaded processing state with {len(state.get('processed_files', {}))} files")
                return state
            except json.JSONDecodeError:
                logger.warning("Could not decode processing_state.json. Starting fresh.")
                return {'processed_files': {}, 'last_processed': None}
    logger.info("No existing processing state found. Starting fresh.")
    return {'processed_files': {}, 'last_processed': None}

def save_processing_state(state_file_path, processed_files, logger: Logger):
    """Save the current processing state"""
    state = {
        'processed_files': processed_files,
        'last_processed': datetime.now().isoformat()
    }
    with open(state_file_path, 'w') as f:
        json.dump(state, f, indent=2)
    logger.info(f"Updated processing state with {len(processed_files)} files")

def get_files_to_process(raw_data_path, existing_state, logger: Logger):
    """Determine which Excel files need processing"""
    files_to_process = []
    all_files = list(raw_data_path.glob('*.xlsx'))
    
    logger.info(f"Found {len(all_files)} Excel files in {raw_data_path}")
    
    for file_path in all_files:
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
    
    logger.info(f"Identified {len(files_to_process)} files for processing")
    return files_to_process

# --- Date Extraction ---
def extract_date_from_filename(filename, logger: Logger):
    """Extract date from filename using regex pattern"""
    pattern = re.compile(r'(\d{2})\.(\d{2})\.(\d{2})')
    match = pattern.search(filename)
    if not match:
        logger.error(f"No date found in filename: {filename}")
        raise ValueError(f"No date found in filename: {filename}")
    
    mm, dd, yy = match.groups()
    yyyy = f"20{yy}" if int(yy) < 50 else f"19{yy}"  # Y2K logic
    date_str = f"{mm}/{dd}/{yyyy}"
    logger.debug(f"Extracted date '{date_str}' from filename '{filename}'")
    return date_str

# --- Data Processing Functions ---
def process_single_file(file_path, processed_files_metadata, logger: Logger):
    """Process a single Excel file with enhanced datetime handling"""
    logger.info(f"Processing file: {file_path.name}")
    
    try:
        # Read Excel file
        logger.debug(f"Reading Excel file: {file_path}")
        df = pd.read_excel(file_path)
        logger.info(f"  Initial shape: {df.shape}")
        logger.debug(f"  Columns: {list(df.columns)}")
        
        # Check for existing 'Date' column
        if any(col.lower() == 'date' for col in df.columns):
            error_msg = f"File {file_path.name} already contains a 'Date' column. Please resolve this before proceeding."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Extract date and add Date column with immediate datetime conversion
        date_str = extract_date_from_filename(file_path.name, logger)
        date_obj = pd.to_datetime(date_str, format='%m/%d/%Y')
        
        # Add Date column as datetime object (not string)
        df.insert(0, 'Date', date_obj)
        logger.info(f"  Added Date column with datetime value: {date_obj.strftime('%Y-%m-%d')}")
        logger.info(f"  Date column dtype: {df['Date'].dtype}")
        logger.info(f"  Shape after adding Date: {df.shape}")
        
        # Update metadata
        processed_files_metadata[file_path.name] = get_file_metadata(file_path)
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to process file {file_path.name}: {str(e)}")
        raise

def clean_and_validate_data(df, config, logger: Logger):
    """Clean and validate the combined DataFrame"""
    logger.info("Starting data cleaning and validation...")
    
    # Log initial state
    initial_shape = df.shape
    logger.info(f"Initial combined DataFrame shape: {initial_shape}")
    
    # Log DataFrame info
    buf = io.StringIO()
    df.info(buf=buf)
    logger.debug("DataFrame info before cleaning:\n" + buf.getvalue())
    
    # 1. Remove rows with missing SECURITY
    before_security = len(df)
    df = df[df['SECURITY'].notna()]
    removed_security = before_security - len(df)
    if removed_security > 0:
        logger.warning(f"Removed {removed_security} rows with missing SECURITY")
    else:
        logger.info("No rows with missing SECURITY found")
    
    # 2. Drop specified columns
    columns_to_drop = config.get('columns_to_drop', [])
    existing_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_cols_to_drop:
        logger.info(f"Dropping {len(existing_cols_to_drop)} columns: {existing_cols_to_drop}")
        df.drop(columns=existing_cols_to_drop, inplace=True)
    else:
        logger.info("No specified columns to drop found in DataFrame")
    
    # 3. Apply CUSIP mappings
    cusip_mappings = config.get('cusip_mappings', {})
    logger.info("Applying CUSIP mappings...")
    
    for mapping_name, mapping_config in cusip_mappings.items():
        if mapping_name == 'CDX' and 'SECURITY TYPE' in df.columns:
            mask = df['SECURITY TYPE'] == mapping_config.get('security_type', 'CDX')
            count = mask.sum()
            if count > 0:
                df.loc[mask, 'SECURITY'] = mapping_config.get('security_name', 'CDX')
                df.loc[mask, 'CUSIP'] = mapping_config.get('cusip', '460')
                logger.info(f"Applied CDX mapping to {count} rows")
            else:
                logger.debug("No CDX rows found")
        
        elif mapping_name in ['CASH_CAD', 'CASH_USD']:
            security_name = mapping_config.get('security_name')
            mask = df['SECURITY'] == security_name
            count = mask.sum()
            if count > 0:
                df.loc[mask, 'CUSIP'] = mapping_config.get('cusip')
                logger.info(f"Applied {mapping_name} mapping to {count} rows")
            else:
                logger.debug(f"No {security_name} rows found")
    
    # 4. Ensure CUSIP is string type
    if 'CUSIP' in df.columns:
        df['CUSIP'] = df['CUSIP'].astype(str)
        logger.debug("Converted CUSIP column to string type")
    
    # 5. Validate Date column (should already be datetime from processing)
    if 'Date' in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df['Date']):
            logger.warning("Date column is not datetime type - converting...")
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
        
        logger.info(f"Date column validated. Type: {df['Date'].dtype}")
        
        # Enhanced date analysis
        if len(df) > 0:
            unique_dates = df['Date'].dropna().unique()
            logger.info(f"Date coverage: {len(unique_dates)} unique dates")
            
            if len(unique_dates) > 0:
                sorted_dates = sorted(unique_dates)
                min_date = pd.to_datetime(sorted_dates[0])
                max_date = pd.to_datetime(sorted_dates[-1])
                
                logger.info(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                
                # Check for date gaps or consistency
                date_counts = df['Date'].value_counts().sort_index()
                logger.info(f"Records per date: min={date_counts.min()}, max={date_counts.max()}, avg={date_counts.mean():.1f}")
    
    # 6. Reorder columns (Date first)
    if 'Date' in df.columns:
        cols = ['Date'] + [col for col in df.columns if col != 'Date']
        df = df[cols]
        logger.debug("Reordered columns with Date first")
    
    # Log final cleaning results
    final_shape = df.shape
    removed_total = initial_shape[0] - final_shape[0]
    logger.info(f"Data cleaning complete:")
    logger.info(f"  Initial rows: {initial_shape[0]}")
    logger.info(f"  Final rows: {final_shape[0]}")
    logger.info(f"  Rows removed: {removed_total}")
    logger.info(f"  Final columns: {final_shape[1]}")
    
    return df

def run_data_validation(df, config, logger: Logger):
    """Run comprehensive data validation"""
    logger.info("Running data validation and quality analysis...")
    
    # Create validator instance
    validator = DataValidator(
        df,
        numeric_cols=config.get('validation', {}).get('numeric_columns', [])
    )
    
    # Run all validation checks
    validator.run_all_checks()
    
    # Log validation results
    if validator.errors:
        error_report = DataReporter.generate_validation_error_report(validator.errors)
        logger.warning(error_report)
    else:
        logger.info("[OK] All data validation checks passed")
    
    # Log detailed quality report
    quality_report = DataReporter.generate_data_quality_report(validator.results)
    logger.info(quality_report)
    
    # Log final summary
    summary_report = DataReporter.generate_summary_report(df)
    logger.info(summary_report)

# --- Analytics Helper Functions ---
def filter_portfolio_by_date_range(df: pd.DataFrame, start_date: str, end_date: str, logger: Logger = None) -> pd.DataFrame:
    """Filter portfolio DataFrame by date range. Expects datetime Date column.
    
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
        logger.info(f"Filtered portfolio by date range {start_date} to {end_date}: {len(filtered_df)} records")
    return filtered_df

def get_portfolio_latest_date(df: pd.DataFrame, logger: Logger = None) -> pd.DataFrame:
    """Get portfolio data for the most recent date.
    
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
        logger.info(f"Extracted latest portfolio date {latest_str}: {len(latest_df)} records")
    return latest_df

def add_portfolio_date_features(df: pd.DataFrame, logger: Logger = None) -> pd.DataFrame:
    """Add useful date-based features for portfolio analytics.
    
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
        logger.info(f"Added {9} date features for portfolio analytics")
    return df_enhanced

# --- Main Processing Function ---
def process_portfolio_files(logger: Logger, force_full_refresh: bool = False):
    """
    Main function to process portfolio Excel files.
    Reads files from 'portfolio/raw data', processes them incrementally,
    and updates the 'portfolio.parquet' file.
    
    Args:
        logger: Logger instance
        force_full_refresh: If True, ignores state tracking and processes ALL raw data
    """
    logger.info("Starting portfolio processing pipeline...")
    
    try:
        # Load configuration
        config = load_config()
        logger.debug("Loaded portfolio processor configuration")
        
        # Set up paths
        project_root = Path(__file__).parent.parent.parent
        raw_data_path = project_root / 'portfolio' / 'raw data'
        parquet_path = project_root / 'portfolio' / 'portfolio.parquet'
        state_file_path = project_root / 'portfolio' / 'processing_state.json'
        
        logger.info(f"Raw data path: {raw_data_path}")
        logger.info(f"Output Parquet path: {parquet_path}")
        logger.info(f"State file path: {state_file_path}")
        
        # Load processing state
        if force_full_refresh:
            logger.info("🔄 FORCE FULL REFRESH: Ignoring state tracking, processing ALL portfolio files")
            existing_state = {'processed_files': {}, 'last_processed': None}
            # Get all Excel files for processing (with metadata tuple format)
            all_files = [f for f in raw_data_path.glob('*.xlsx') if f.is_file()]
            files_to_process = [(f, get_file_metadata(f)) for f in all_files]
            logger.info(f"📁 Found {len(files_to_process)} Excel files for full processing")
        else:
            existing_state = load_processing_state(state_file_path, logger)
            # Determine files to process
            files_to_process = get_files_to_process(raw_data_path, existing_state, logger)
        
        # Load existing data if exists
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
                # Process all files if can't load existing data
                files_to_process = [(f, get_file_metadata(f)) for f in raw_data_path.glob('*.xlsx')]
        
        # Check if processing is needed
        if not files_to_process:
            logger.info("No new or modified files to process. The Parquet file is up-to-date.")
            return existing_df
        
        logger.info(f"Processing {len(files_to_process)} files...")
        
        # Process files
        all_new_data = []
        processed_files_metadata = existing_state.get('processed_files', {}).copy()
        
        for file_path, metadata in files_to_process:
            try:
                df = process_single_file(file_path, processed_files_metadata, logger)
                all_new_data.append(df)
            except Exception as e:
                logger.error(f"Failed to process file {file_path.name}. Pipeline stopped.")
                raise
        
        if not all_new_data:
            logger.warning("No new data was loaded from the files.")
            return existing_df
        
        # Combine new data
        logger.info("Combining new data...")
        new_df = pd.concat(all_new_data, ignore_index=True)
        logger.info(f"Combined new data shape: {new_df.shape}")
        
        # Combine with existing data if available
        if existing_df is not None:
            logger.info("Combining with existing data...")
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
        
        logger.info(f"Final combined shape before cleaning: {combined_df.shape}")
        
        # Clean and validate data
        cleaned_df = clean_and_validate_data(combined_df, config, logger)
        
        # Run validation and quality analysis
        run_data_validation(cleaned_df, config, logger)
        
        # Log DataFrame info before saving
        logger.info("--- Processing Complete ---")
        logger.info(f"Final DataFrame Shape: {cleaned_df.shape}")
        
        # Log unique dates analysis
        if 'Date' in cleaned_df.columns:
            unique_dates = sorted(cleaned_df['Date'].dropna().unique())
            logger.info(f"\n=========================")
            logger.info(f"=== DATE COVERAGE ANALYSIS ===")
            logger.info(f"=========================")
            logger.info(f"Total unique dates: {len(unique_dates)}")
            logger.info(f"Date column dtype: {cleaned_df['Date'].dtype}")
            
            if unique_dates:
                min_date = pd.to_datetime(unique_dates[0])
                max_date = pd.to_datetime(unique_dates[-1])
                
                logger.info(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                
                # Calculate time span
                time_span = max_date - min_date
                logger.info(f"Time span: {time_span.days} days ({time_span.days/365.25:.1f} years)")
                
                # Year distribution analysis
                df_temp = cleaned_df[cleaned_df['Date'].notna()].copy()
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
                    date_count = (cleaned_df['Date'] == date).sum()
                    logger.info(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
                
                if len(unique_dates) > 10:
                    logger.info(f"  ... ({len(unique_dates) - 10} more dates) ...")
                    for i, date in enumerate(unique_dates[-5:]):
                        date_dt = pd.to_datetime(date)
                        date_count = (cleaned_df['Date'] == date).sum()
                        logger.info(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
            else:
                logger.warning("No valid dates found in dataset")
            
            logger.info(f"\n=== END DATE ANALYSIS ===\n")
        
        buf = io.StringIO()
        cleaned_df.info(buf=buf)
        logger.info("DataFrame info before saving to Parquet:\n" + buf.getvalue())
        
        # Save to Parquet
        try:
            cleaned_df.to_parquet(parquet_path, index=False, engine='pyarrow')
            logger.info(f"Successfully saved {len(cleaned_df)} rows to '{parquet_path}'")
            
            # Update processing state
            save_processing_state(state_file_path, processed_files_metadata, logger)
            
        except Exception as e:
            logger.critical(f"Error saving to Parquet: {e}")
            raise
        
        logger.info("Portfolio processing pipeline completed successfully")
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Portfolio processing pipeline failed: {str(e)}")
        raise 