"""
Parquet file processing module.
"""
import os
import pandas as pd
from typing import Optional

from .base import BaseProcessor
from ..models.data_models import ProcessingResult
from ..utils.config import PipelineConfig
from ..utils.validators import DataValidator


class ParquetProcessor(BaseProcessor):
    """Handles saving and loading Parquet files"""
    
    def process(self, df: pd.DataFrame = None, operation: str = "save") -> ProcessingResult:
        """Process method to satisfy abstract base class"""
        if operation == "save" and df is not None:
            return self.save_to_parquet(df)
        elif operation == "load":
            return self.load_from_parquet()
        else:
            return ProcessingResult.failure_result(
                "Invalid operation or missing data for ParquetProcessor"
            )
    
    def save_to_parquet(self, df: pd.DataFrame, file_path: str = None) -> ProcessingResult:
        """Save DataFrame to Parquet file"""
        try:
            self._start_timer()
            
            # Use configured path if not provided
            if file_path is None:
                file_path = self.config.output_parquet
            
            self.logger.info(f"Saving DataFrame to Parquet: {file_path}")
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Check if existing file exists and should be merged
            if os.path.exists(file_path):
                merged_df = self._merge_with_existing(df, file_path)
                if merged_df is not None:
                    df = merged_df
            
            # DEBUG: Print rows for F 7 02/10/26, TD, 2025-05-28 to 2025-05-30
            try:
                mask = (
                    (df['Security'] == 'F 7 02/10/26') &
                    (df['Dealer'] == 'TD') &
                    (df['Date'] >= '2025-05-28') & (df['Date'] <= '2025-05-30')
                )
                debug_rows = df.loc[mask]
                self.logger.info('DEBUG: Rows for F 7 02/10/26, TD, 2025-05-28 to 2025-05-30 before saving to Parquet:')
                if not debug_rows.empty:
                    self.logger.info(f"\n{debug_rows[['Date','Time','CUSIP','Dealer','Bid Price','Ask Price','Bid Spread']].to_string(index=False)}")
                else:
                    self.logger.info('No rows found for F 7 02/10/26, TD, 2025-05-28 to 2025-05-30 before saving to Parquet.')
            except Exception as e:
                self.logger.error(f'Error in Parquet debug print: {e}')
            
            # Save to Parquet
            df.to_parquet(file_path, index=False, engine='pyarrow')
            
            # Update statistics
            self.stats.rows_loaded = len(df)
            
            # Log date coverage analysis
            if 'Date' in df.columns:
                unique_dates = sorted(df['Date'].dropna().unique())
                self.logger.info(f"\n=========================")
                self.logger.info(f"=== DATE COVERAGE ANALYSIS (PARQUET) ===")
                self.logger.info(f"=========================")
                self.logger.info(f"Total unique dates: {len(unique_dates)}")
                if unique_dates:
                    # Convert numpy datetime64 to pandas datetime for proper formatting
                    start_date = pd.to_datetime(unique_dates[0]).strftime('%Y-%m-%d')
                    end_date = pd.to_datetime(unique_dates[-1]).strftime('%Y-%m-%d')
                    self.logger.info(f"Date range: {start_date} to {end_date}")
                    self.logger.info(f"Unique dates saved to Parquet:")
                    for date in unique_dates:
                        date_count = (df['Date'] == date).sum()
                        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
                        self.logger.info(f"  - {date_str}: {date_count} records")
                else:
                    self.logger.warning("No valid dates found in dataset")
            
            self._stop_timer()
            self.log_stats()
            
            self.logger.info(f"Successfully saved {len(df)} rows to {file_path}")
            
            return ProcessingResult.success_result(
                f"Successfully saved {len(df)} rows to Parquet file",
                data=df,
                metadata={'file_path': file_path, 'rows_saved': len(df)}
            )
            
        except Exception as e:
            self._stop_timer()
            self.logger.error("Failed to save Parquet file", e)
            return ProcessingResult.failure_result(
                "Failed to save Parquet file",
                error=e
            )
    
    def load_from_parquet(self, file_path: str = None) -> ProcessingResult:
        """Load DataFrame from Parquet file"""
        try:
            self._start_timer()
            
            # Use configured path if not provided
            if file_path is None:
                file_path = self.config.output_parquet
            
            if not os.path.exists(file_path):
                self.logger.info(f"Parquet file does not exist: {file_path}")
                return ProcessingResult.success_result(
                    "No existing Parquet file found",
                    data=None,
                    metadata={'file_exists': False}
                )
            
            self.logger.info(f"Loading DataFrame from Parquet: {file_path}")
            
            # Load from Parquet
            df = pd.read_parquet(file_path)
            
            # Validate loaded data
            if df.empty:
                self.logger.warning("Loaded Parquet file is empty")
                return ProcessingResult.warning_result(
                    "Loaded Parquet file is empty",
                    data=df
                )
            
            # Update statistics
            self.stats.rows_processed = len(df)
            
            # Log date coverage analysis
            if 'Date' in df.columns:
                unique_dates = sorted(df['Date'].dropna().unique())
                self.logger.info(f"\n=========================")
                self.logger.info(f"=== DATE COVERAGE ANALYSIS (LOADED FROM PARQUET) ===")
                self.logger.info(f"=========================")
                self.logger.info(f"Total unique dates: {len(unique_dates)}")
                if unique_dates:
                    # Convert numpy datetime64 to pandas datetime for proper formatting
                    start_date = pd.to_datetime(unique_dates[0]).strftime('%Y-%m-%d')
                    end_date = pd.to_datetime(unique_dates[-1]).strftime('%Y-%m-%d')
                    self.logger.info(f"Date range: {start_date} to {end_date}")
                    self.logger.info(f"Unique dates loaded from Parquet:")
                    for date in unique_dates:
                        date_count = (df['Date'] == date).sum()
                        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
                        self.logger.info(f"  - {date_str}: {date_count} records")
                else:
                    self.logger.warning("No valid dates found in dataset")
            
            self._stop_timer()
            
            self.logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
            
            return ProcessingResult.success_result(
                f"Successfully loaded {len(df)} rows from Parquet file",
                data=df,
                metadata={'file_path': file_path, 'rows_loaded': len(df)}
            )
            
        except Exception as e:
            self._stop_timer()
            self.logger.error("Failed to load Parquet file", e)
            return ProcessingResult.failure_result(
                "Failed to load Parquet file",
                error=e
            )
    
    def _merge_with_existing(self, new_df: pd.DataFrame, file_path: str) -> Optional[pd.DataFrame]:
        """Merge new DataFrame with existing Parquet file"""
        try:
            self.logger.info("Merging with existing Parquet file")
            
            # Load existing data
            existing_df = pd.read_parquet(file_path)
            self.logger.info(f"Existing Parquet file shape: {existing_df.shape}")
            
            # Validate existing data
            quality_report = DataValidator.validate_data_quality(existing_df, self.logger)
            
            # Combine new and existing data
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            self.logger.info(f"Combined shape before deduplication: {combined_df.shape}")
            
            # Deduplicate combined data
            combined_df = self._deduplicate_combined_data(combined_df)
            
            self.logger.info(f"Final combined shape: {combined_df.shape}")
            
            return combined_df
            
        except Exception as e:
            self.logger.error("Error merging with existing Parquet file", e)
            self.logger.info("Proceeding with new data only")
            return new_df
    
    def _deduplicate_combined_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate the combined DataFrame"""
        initial_count = len(df)
        
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
            self.logger.debug(f"Sorted combined data by: {sort_columns}")
        
        # Define deduplication columns - one quote per Date/CUSIP/Dealer
        key_columns = ['Date', 'CUSIP', 'Dealer']
        
        # Only use columns that exist in the DataFrame
        existing_key_columns = [col for col in key_columns if col in df.columns]
        
        if len(existing_key_columns) == len(key_columns):
            # Remove duplicates keeping the last occurrence (latest time for same date/cusip/dealer)
            df = df.drop_duplicates(subset=existing_key_columns, keep='last')
            
            final_count = len(df)
            removed_count = initial_count - final_count
            
            if removed_count > 0:
                self.logger.info(f"Removed {removed_count} duplicate rows during merge (same Date/CUSIP/Dealer)")
                self.stats.rows_duplicated = removed_count
            else:
                self.logger.info("No duplicates found during merge")
        else:
            self.logger.warning(f"Cannot deduplicate - missing required columns. Have: {existing_key_columns}, Need: {key_columns}")
        
        return df
    
    def get_file_info(self, file_path: str = None) -> dict:
        """Get information about the Parquet file"""
        if file_path is None:
            file_path = self.config.output_parquet
        
        info = {
            'file_exists': False,
            'file_path': file_path,
            'file_size': 0,
            'row_count': 0,
            'column_count': 0,
            'last_modified': None
        }
        
        try:
            if os.path.exists(file_path):
                info['file_exists'] = True
                info['file_size'] = os.path.getsize(file_path)
                info['last_modified'] = os.path.getmtime(file_path)
                
                # Load file to get row/column counts
                df = pd.read_parquet(file_path)
                info['row_count'] = len(df)
                info['column_count'] = len(df.columns)
                
        except Exception as e:
            self.logger.warning(f"Error getting file info: {e}")
        
        return info 