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

from pipeline.base import BaseProcessor, ProcessingError
from models.data_models import ProcessingResult, ExcelFileInfo
from utils.validators import DataValidator


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
            self._update_last_processed_date()
            
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
        """Get list of files that need processing"""
        # Find all Excel files
        pattern = os.path.join(self.config.input_dir, self.config.file_pattern)
        all_files = glob(pattern)
        
        if not all_files:
            self.logger.warning(f"No files found matching pattern: {pattern}")
            return []
        
        # Get last processed date
        last_processed = self._get_last_processed_date()
        
        if not last_processed:
            self.logger.info("No previous processing date found, processing all files")
            return all_files
        
        # Filter files based on modification date
        self.logger.info(f"Last processed date: {last_processed}")
        filtered_files = []
        
        for file_path in all_files:
            try:
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                mod_date = mod_time.strftime("%Y-%m-%d")
                
                if mod_date > last_processed:
                    filtered_files.append(file_path)
                    self.logger.info(f"Will process: {os.path.basename(file_path)} (modified: {mod_date})")
                else:
                    self.logger.debug(f"Skipping: {os.path.basename(file_path)} (modified: {mod_date})")
                    
            except Exception as e:
                self.logger.warning(f"Error checking file {file_path}: {e}")
                continue
        
        return filtered_files
    
    def _get_last_processed_date(self) -> Optional[str]:
        """Get the last processed date from the tracking file"""
        try:
            if os.path.exists(self.config.last_processed_file):
                with open(self.config.last_processed_file, 'r') as f:
                    data = json.load(f)
                    return data.get("last_processed")
        except Exception as e:
            self.logger.warning(f"Error reading last processed date: {e}")
        return None
    
    def _update_last_processed_date(self):
        """Update the last processed date"""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            with open(self.config.last_processed_file, 'w') as f:
                json.dump({"last_processed": today}, f)
            self.logger.info(f"Updated last processed date to: {today}")
        except Exception as e:
            self.logger.error(f"Error updating last processed date: {e}")
    
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
        
        # Parse Date column
        if 'Date' in df.columns:
            try:
                df['Date'] = pd.to_datetime(
                    df['Date'].astype(str).str.strip(), 
                    format=self.config.date_format, 
                    errors='coerce'
                )
                nat_count = df['Date'].isna().sum()
                if nat_count > 0:
                    self.logger.warning(f"Found {nat_count} invalid dates in {file_name}")
                else:
                    self.logger.debug(f"Parsed Date column in {file_name}")
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
                df = df[df[col] >= 0]
                removed_count = before_count - len(df)
                if removed_count > 0:
                    self.logger.warning(f"Removed {removed_count} rows with negative {col}")
        
        # Convert Date to string format for deduplication
        if 'Date' in df.columns:
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        
        # Sort by Date and Time
        try:
            if 'Date' in df.columns and 'Time' in df.columns:
                df = df.sort_values(['Date', 'Time'])
                self.logger.debug("Sorted data by Date and Time")
        except Exception as e:
            self.logger.warning(f"Error sorting data: {e}")
        
        # Define deduplication columns - one quote per Date/CUSIP/Dealer
        key_columns = [
            'Date', 'CUSIP', 'Dealer'
        ]
        
        # Only use columns that exist in the DataFrame
        existing_key_columns = [col for col in key_columns if col in df.columns]
        self.logger.info(f"Using deduplication columns: {existing_key_columns}")
        
        # Remove duplicates keeping the last occurrence (latest time for same date/cusip/dealer)
        before_dedup = len(df)
        df = df.drop_duplicates(subset=existing_key_columns, keep='last')
        after_dedup = len(df)
        removed_dupes = before_dedup - after_dedup
        
        if removed_dupes > 0:
            self.logger.info(f"Removed {removed_dupes} duplicate rows")
            self.stats.rows_duplicated = removed_dupes
        
        # Convert Date back to datetime
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        
        final_count = len(df)
        total_removed = initial_count - final_count
        self.logger.info(f"Data cleaning complete. Removed {total_removed} rows total")
        
        return df 