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
def process_universe_files(logger: Logger):
    """
    Reads all Excel files from 'universe/raw data', processes them incrementally,
    and updates the 'universe.parquet' file.
    """
    config = load_config()
    project_root = Path(__file__).parent.parent.parent
    raw_data_path = project_root / 'universe' / 'raw data'
    parquet_path = project_root / 'universe' / 'universe.parquet'
    
    existing_state = load_processing_state(logger)
    
    files_to_process = get_files_to_process(raw_data_path, existing_state, logger)
    
    existing_df = None
    if parquet_path.exists():
        logger.info("Found existing Parquet file. Loading for incremental processing...")
        try:
            existing_df = pd.read_parquet(parquet_path)
            logger.info(f"Loaded existing data: {existing_df.shape[0]} rows, {existing_df.shape[1]} columns")
            # Log DataFrame info after loading from Parquet
            buf = io.StringIO()
            existing_df.info(buf=buf)
            logger.info("DataFrame info after loading from Parquet:\n" + buf.getvalue())
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
            
            report_date = pd.to_datetime(match.group(1), format='%m.%d.%y')
            df = pd.read_excel(file_path, header=1)
            
            df.columns = df.iloc[0]
            df = df[1:]
            
            df['Date'] = report_date
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
        logger.info("✅ All data validation checks passed.")

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
    # Log DataFrame info before saving to Parquet
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