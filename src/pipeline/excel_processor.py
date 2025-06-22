"""
Excel file processing module.
"""
import os
import json
import pandas as pd
from glob import glob
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional
from pathlib import Path

from .base import BaseProcessor, ProcessingError
from ..models.data_models import ProcessingResult, ExcelFileInfo
from ..utils.validators import DataValidator


class ExcelProcessor(BaseProcessor):
    """Processes Excel files into a DataFrame"""
    
    def process(self) -> ProcessingResult:
        """Process Excel files into a DataFrame"""
        try:
            self._start_timer()
            self.logger.info("Starting Excel file processing")
            
            # Get list of files to process
            files = self._get_files_to_process()
            if not files:
                self.logger.info("No new files to process")
                return ProcessingResult.success_result(
                    "No new files to process",
                    data=None,
                    metadata={'files_found': 0}
                )
            
            self.logger.info(f"Found {len(files)} files to process")
            
            # Process files
            dfs = self._process_files(files)
            if not dfs:
                return ProcessingResult.failure_result(
                    "No data processed successfully",
                    metadata={'files_attempted': len(files)}
                )
            
            self.logger.info(f"Successfully loaded {len(dfs)} files")
            
            # Combine and clean data
            final_df = self._combine_and_clean_data(dfs)
            
            # Update statistics
            self.stats.files_processed = len(files)
            self.stats.rows_processed = len(final_df)
            self.stats.rows_loaded = len(final_df)
            
            self._stop_timer()
            self.log_stats()
            
            # Update last processed date
            self._update_processed_files(files)
            
            return ProcessingResult.success_result(
                f"Excel processing completed successfully. Processed {len(files)} files, {len(final_df)} rows",
                data=final_df,
                metadata={'files_processed': len(files), 'rows_processed': len(final_df)}
            )
            
        except Exception as e:
            self._stop_timer()
            self.logger.error("Excel processing failed", e)
            return ProcessingResult.failure_result(
                "Excel processing failed",
                error=e
            )
    
    def _get_files_to_process(self) -> List[str]:
        """Get list of files that need to be processed"""
        pattern = os.path.join(self.config.input_dir, self.config.file_pattern)
        all_files = glob(pattern)
        self.logger.debug(f"[DEBUG] All files found by glob: {all_files}")
        
        if not all_files:
            self.logger.warning(f"No files found matching pattern: {pattern}")
            return []
        
        # Get list of already processed files
        processed_files = self._get_processed_files()
        self.logger.debug(f"[DEBUG] Processed files set: {processed_files}")
        
        # Filter out already processed files
        filtered_files = []
        
        for file_path in all_files:
            file_name = os.path.basename(file_path)
            
            if file_name in processed_files:
                self.logger.debug(f"Skipping already processed file: {file_name}")
            else:
                filtered_files.append(file_path)
                self.logger.info(f"Will process: {file_name}")
        
        if not filtered_files:
            self.logger.info("No new files to process")
        else:
            self.logger.info(f"Found {len(filtered_files)} new files to process")
        
        return filtered_files
    
    def _get_processed_files(self) -> set:
        """Get the set of already processed files"""
        try:
            if os.path.exists(self.config.last_processed_file):
                with open(self.config.last_processed_file, 'r') as f:
                    data = json.load(f)
                    # Support both old date-based and new file-based tracking
                    if 'processed_files' in data:
                        return set(data['processed_files'])
                    else:
                        # Convert old date-based tracking to empty set for fresh start
                        return set()
            return set()
        except Exception as e:
            self.logger.warning(f"Error reading processed files: {e}")
            return set()
    
    def _update_processed_files(self, processed_files: List[str]):
        """Update the list of processed files"""
        try:
            # Get existing processed files
            existing_processed = self._get_processed_files()
            
            # Add new files to the set
            new_file_names = [os.path.basename(f) for f in processed_files]
            updated_processed = existing_processed.union(set(new_file_names))
            
            # Save updated list
            today = datetime.now().strftime("%Y-%m-%d")
            tracking_data = {
                "last_run": today,
                "processed_files": list(updated_processed)
            }
            
            with open(self.config.last_processed_file, 'w') as f:
                json.dump(tracking_data, f, indent=2)
            
            self.logger.info(f"Updated processed files list. Total files tracked: {len(updated_processed)}")
            self.logger.info(f"Newly processed: {new_file_names}")
            
        except Exception as e:
            self.logger.error(f"Error updating processed files: {e}")
    
    def _process_files(self, files: List[str]) -> List[pd.DataFrame]:
        """Process files in parallel or sequentially"""
        dfs = []
        
        if self.config.parallel_load and len(files) > 1:
            self.logger.info(f"Processing {len(files)} files in parallel with {self.config.n_workers} workers")
            dfs = self._process_files_parallel(files)
        else:
            self.logger.info(f"Processing {len(files)} files sequentially")
            dfs = self._process_files_sequential(files)
        
        return dfs
    
    def _process_files_parallel(self, files: List[str]) -> List[pd.DataFrame]:
        """Process files in parallel"""
        dfs = []
        
        with ThreadPoolExecutor(max_workers=self.config.n_workers) as executor:
            # Submit all files for processing
            future_to_file = {executor.submit(self._load_single_file, file): file for file in files}
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    df = future.result()
                    if df is not None:
                        dfs.append(df)
                except Exception as e:
                    self.logger.error(f"Error processing {file_path}", e)
                    continue
        
        return dfs
    
    def _process_files_sequential(self, files: List[str]) -> List[pd.DataFrame]:
        """Process files sequentially"""
        dfs = []
        
        for file_path in files:
            try:
                df = self._load_single_file(file_path)
                if df is not None:
                    dfs.append(df)
            except Exception as e:
                self.logger.error(f"Error processing {file_path}", e)
                continue
        
        return dfs
    
    def _load_single_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Load a single Excel file"""
        try:
            self.logger.debug(f"Loading file: {os.path.basename(file_path)}")
            
            # Read Excel file
            df = pd.read_excel(file_path, engine='openpyxl')
            
            if df.empty:
                self.logger.warning(f"File is empty: {file_path}")
                return None
            
            self.logger.info(f"Loaded {os.path.basename(file_path)}: shape={df.shape}")
            
            # Parse date and time columns
            df = self._parse_date_time_columns(df, file_path)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading {file_path}: {e}")
            return None
    
    def _parse_date_time_columns(self, df: pd.DataFrame, file_path: str) -> pd.DataFrame:
        """Parse Date and Time columns to appropriate types"""
        file_name = os.path.basename(file_path)
        
        # Parse Date column robustly
        if 'Date' in df.columns:
            try:
                # Strip whitespace
                df['Date'] = df['Date'].astype(str).str.strip()
                # Robust parsing with inference
                df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True, errors='coerce')
                nat_count = df['Date'].isna().sum()
                if nat_count > 0:
                    self.logger.warning(f"Found {nat_count} invalid dates in {file_name}")
                    # Log a sample of unparseable dates
                    invalid_dates = df[df['Date'].isna()]
                    self.logger.debug(f"Sample unparseable dates in {file_name}: {invalid_dates['Date'].head(5).tolist()}")
                else:
                    self.logger.debug(f"Parsed Date column in {file_name}")
                # Normalize to standard format for deduplication
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            except Exception as e:
                self.logger.error(f"Error parsing Date column in {file_name}: {e}")
        
        # Parse Time column
        if 'Time' in df.columns:
            try:
                # Parse as datetime, then extract time
                times = pd.to_datetime(
                    df['Time'].astype(str).str.strip(), 
                    format=self.config.time_format, 
                    errors='coerce'
                )
                df['Time'] = times.dt.time
                nan_count = df['Time'].isna().sum()
                if nan_count > 0:
                    self.logger.warning(f"Found {nan_count} invalid times in {file_name}")
                else:
                    self.logger.debug(f"Parsed Time column in {file_name}")
            except Exception as e:
                self.logger.error(f"Error parsing Time column in {file_name}: {e}")
        
        return df
    
    def _combine_and_clean_data(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Combine multiple DataFrames and clean the result"""
        self.logger.info(f"Combining {len(dfs)} DataFrames")
        
        # Concatenate all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Combined shape before cleaning: {combined_df.shape}")
        
        # Validate data quality
        quality_report = DataValidator.validate_data_quality(combined_df, self.logger)
        DataValidator.log_quality_report(quality_report, self.logger)
        
        # Clean and deduplicate
        cleaned_df = self._clean_and_deduplicate(combined_df)
        
        self.logger.info(f"Final shape after cleaning: {cleaned_df.shape}")
        
        return cleaned_df
    
    def _clean_and_deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and deduplicate the DataFrame"""
        self.logger.info("Starting data cleaning and deduplication")
        
        initial_count = len(df)
        
        # Remove rows with negative prices (data quality issue)
        for col in ['Bid Price', 'Ask Price', 'Bid Size', 'Ask Size']:
            if col in df.columns:
                before_count = len(df)
                df = df[~(df[col] < 0)]
                removed_count = before_count - len(df)
                if removed_count > 0:
                    self.logger.warning(f"Removed {removed_count} rows with negative {col}")
        
        # Drop rows with NA in key columns before deduplication
        key_na_cols = ['Date', 'CUSIP', 'Dealer', 'Bid Spread']
        existing_na_cols = [col for col in key_na_cols if col in df.columns]
        before_na = len(df)
        df = df.dropna(subset=existing_na_cols)
        removed_na = before_na - len(df)
        if removed_na > 0:
            self.logger.warning(f"Removed {removed_na} rows with NA in {existing_na_cols} before deduplication")
        
        # Ensure Date is datetime for proper sorting
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        
        # Sort by Date, CUSIP, Dealer, and Time to ensure consistent ordering
        sort_columns = []
        for col in ['Date', 'CUSIP', 'Dealer', 'Time']:
            if col in df.columns:
                sort_columns.append(col)
        
        if sort_columns:
            df = df.sort_values(sort_columns)
            self.logger.debug(f"Sorted data by: {sort_columns}")
        
        # Define deduplication columns - one quote per Date/CUSIP/Dealer
        key_columns = ['Date', 'CUSIP', 'Dealer']
        
        # Only use columns that exist in the DataFrame
        existing_key_columns = [col for col in key_columns if col in df.columns]
        self.logger.info(f"Using deduplication columns: {existing_key_columns}")
        
        if len(existing_key_columns) == len(key_columns):
            # Remove duplicates keeping the last occurrence (latest time for same date/cusip/dealer)
            before_dedup = len(df)
            df = df.drop_duplicates(subset=existing_key_columns, keep='last')
            after_dedup = len(df)
            removed_dupes = before_dedup - after_dedup
            
            if removed_dupes > 0:
                self.logger.info(f"Removed {removed_dupes} duplicate rows (same Date/CUSIP/Dealer)")
                self.stats.rows_duplicated = removed_dupes
            else:
                self.logger.info("No duplicates found")
        else:
            self.logger.warning(f"Cannot deduplicate - missing required columns. Have: {existing_key_columns}, Need: {key_columns}")
        
        final_count = len(df)
        total_removed = initial_count - final_count
        self.logger.info(f"Data cleaning complete. Removed {total_removed} rows total")
        
        return df 

    def _get_last_processed_date(self) -> Optional[str]:
        """Get the last processed date from the tracking file (legacy method)"""
        try:
            if os.path.exists(self.config.last_processed_file):
                with open(self.config.last_processed_file, 'r') as f:
                    data = json.load(f)
                    return data.get("last_processed")
        except Exception as e:
            self.logger.warning(f"Error reading last processed date: {e}")
        return None
    
    def _update_last_processed_date(self):
        """Update the last processed date (legacy method - now handled by _update_processed_files)"""
        # This method is now deprecated in favor of file-based tracking
        pass 