"""
Supabase data upload module.
"""
import pandas as pd
import numpy as np
from supabase import create_client, Client
from typing import List, Dict, Any
import time
import math

from pipeline.base import BaseProcessor
from models.data_models import ProcessingResult
from utils.validators import DataValidator


class SupabaseProcessor(BaseProcessor):
    """Uploads data to Supabase"""
    
    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.client = self._create_client()
    
    def process(self, df: pd.DataFrame) -> ProcessingResult:
        """Upload DataFrame to Supabase"""
        try:
            self._start_timer()
            self.logger.info("Starting Supabase upload process")
            
            if df is None or df.empty:
                return ProcessingResult.warning_result(
                    "No data to upload to Supabase",
                    metadata={'rows_attempted': 0}
                )
            
            # Validate and prepare data
            prepared_df = self._prepare_data_for_upload(df)
            if prepared_df is None:
                return ProcessingResult.failure_result(
                    "Data preparation failed"
                )
            
            # Validate schema compatibility
            if not self._validate_schema_compatibility(prepared_df):
                return ProcessingResult.failure_result(
                    "Schema validation failed"
                )
            
            # Upload data in batches
            upload_result = self._upload_data_in_batches(prepared_df)
            
            # Update statistics
            self.stats.rows_processed = len(df)
            self.stats.rows_loaded = len(prepared_df)
            
            self._stop_timer()
            self.log_stats()
            
            if upload_result:
                return ProcessingResult.success_result(
                    f"Successfully uploaded {len(prepared_df)} rows to Supabase",
                    metadata={'rows_uploaded': len(prepared_df), 'table': self.config.table}
                )
            else:
                return ProcessingResult.failure_result(
                    "Upload to Supabase failed"
                )
            
        except Exception as e:
            self._stop_timer()
            self.logger.error("Supabase upload failed", e)
            return ProcessingResult.failure_result(
                "Supabase upload failed",
                error=e
            )
    
    def _create_client(self) -> Client:
        """Create Supabase client"""
        try:
            client = create_client(self.config.url, self.config.key)
            self.logger.info(f"Connected to Supabase at {self.config.url}")
            return client
        except Exception as e:
            self.logger.error("Failed to create Supabase client", e)
            raise
    
    def _prepare_data_for_upload(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare data for Supabase upload"""
        try:
            self.logger.info("Preparing data for Supabase upload")
            
            # Create a copy to avoid modifying original
            prepared_df = df.copy()
            
            # Fix column names to match Supabase schema
            prepared_df = self._fix_column_names(prepared_df)
            
            # Convert data types for Supabase compatibility
            prepared_df = self._convert_data_types(prepared_df)
            
            # Clean NaN values for JSON/SQL compatibility
            prepared_df = self._clean_nan_values(prepared_df)
            
            self.logger.info(f"Data preparation complete. Shape: {prepared_df.shape}")
            return prepared_df
            
        except Exception as e:
            self.logger.error("Error preparing data for upload", e)
            return None
    
    def _fix_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix column names to match Supabase schema exactly"""
        # Known column name mappings
        column_mappings = {
            'Bid Yield To Convention': 'Bid Yield to Convention',
            'Ask Yield To Convention': 'Ask Yield to Convention'
        }
        
        df = df.rename(columns=column_mappings)
        self.logger.debug(f"Fixed column names: {list(column_mappings.keys())}")
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert data types for Supabase compatibility"""
        # Convert Date column
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
            df['Date'] = df['Date'].astype(str)  # Ensure JSON serializable
            self.logger.debug("Converted Date column to string format")
        
        # Convert Time column
        if 'Time' in df.columns:
            df['Time'] = df['Time'].apply(
                lambda t: t.strftime('%H:%M') if pd.notnull(t) and hasattr(t, 'strftime') 
                else (t if pd.isnull(t) else str(t))
            )
            self.logger.debug("Converted Time column to string format")
        
        return df
    
    def _clean_nan_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace NaN values with None for JSON/SQL compatibility"""
        # Replace all NaN with None
        df = df.where(pd.notnull(df), None)
        self.logger.debug("Cleaned NaN values")
        return df
    
    def _validate_schema_compatibility(self, df: pd.DataFrame) -> bool:
        """Validate that DataFrame schema is compatible with Supabase table"""
        try:
            self.logger.info("Validating schema compatibility with Supabase")
            
            # Fetch Supabase table schema
            supabase_columns = self._fetch_table_schema()
            
            if not supabase_columns:
                self.logger.warning("Could not fetch Supabase schema, proceeding with upload")
                return True
            
            df_columns = set(df.columns)
            supabase_columns_set = set(supabase_columns)
            
            # Check for missing columns
            missing_in_df = supabase_columns_set - df_columns
            missing_in_supabase = df_columns - supabase_columns_set
            
            if missing_in_df:
                self.logger.error(f"DataFrame missing required columns: {missing_in_df}")
                return False
            
            if missing_in_supabase:
                self.logger.error(f"DataFrame has columns not in Supabase: {missing_in_supabase}")
                return False
            
            self.logger.info("Schema validation passed")
            return True
            
        except Exception as e:
            self.logger.error("Error validating schema compatibility", e)
            return False
    
    def _fetch_table_schema(self) -> List[str]:
        """Fetch column names from Supabase table"""
        try:
            # Try to get a single row to determine schema
            response = self.client.table(self.config.table).select('*').limit(1).execute()
            
            if hasattr(response, 'data') and response.data:
                columns = list(response.data[0].keys())
                self.logger.debug(f"Fetched Supabase schema: {columns}")
                return columns
            else:
                self.logger.warning("Could not fetch schema from Supabase API")
                return []
                
        except Exception as e:
            self.logger.warning(f"Error fetching Supabase schema: {e}")
            return []
    
    def _upload_data_in_batches(self, df: pd.DataFrame) -> bool:
        """Upload data to Supabase in batches"""
        try:
            total_rows = len(df)
            batch_size = self.config.batch_size
            
            self.logger.info(f"Uploading {total_rows} rows in batches of {batch_size}")
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Convert batch to records
                batch_records = batch_df.to_dict(orient='records')
                
                # Clean NaN values in batch records
                batch_records = [self._clean_nans_recursive(record) for record in batch_records]
                
                self.logger.info(f"Uploading batch {start_idx + 1}-{end_idx} of {total_rows}")
                
                # Log sample record for first batch
                if start_idx == 0 and batch_records:
                    self.logger.debug(f"Sample record: {batch_records[0]}")
                
                # Upload batch
                if not self._upload_batch(batch_records):
                    return False
                
                # Small delay to avoid rate limits
                time.sleep(0.1)
            
            self.logger.info(f"Successfully uploaded all {total_rows} rows")
            return True
            
        except Exception as e:
            self.logger.error("Error during batch upload", e)
            return False
    
    def _upload_batch(self, batch_records: List[Dict]) -> bool:
        """Upload a single batch to Supabase"""
        try:
            response = self.client.table(self.config.table).upsert(batch_records).execute()
            
            # Check for errors in response
            if hasattr(response, 'status_code') and response.status_code >= 400:
                self.logger.error(f"Batch upload failed with status {response.status_code}")
                if hasattr(response, 'data'):
                    self.logger.error(f"Error details: {response.data}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Exception during batch upload: {e}")
            return False
    
    def _clean_nans_recursive(self, obj):
        """Recursively replace all float('nan') with None for JSON/SQL compatibility"""
        if isinstance(obj, float) and math.isnan(obj):
            return None
        elif isinstance(obj, dict):
            return {k: self._clean_nans_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_nans_recursive(x) for x in obj]
        else:
            return obj
    
    def test_connection(self) -> bool:
        """Test connection to Supabase"""
        try:
            response = self.client.table(self.config.table).select('*').limit(1).execute()
            self.logger.info("Supabase connection test successful")
            return True
        except Exception as e:
            self.logger.error("Supabase connection test failed", e)
            return False
    
    def get_table_info(self) -> Dict[str, Any]:
        """Get information about the Supabase table"""
        info = {
            'table_name': self.config.table,
            'row_count': 0,
            'columns': [],
            'accessible': False
        }
        
        try:
            # Try to get row count
            response = self.client.table(self.config.table).select('*', count='exact').execute()
            if hasattr(response, 'count'):
                info['row_count'] = response.count
            
            # Try to get schema
            columns = self._fetch_table_schema()
            if columns:
                info['columns'] = columns
            
            info['accessible'] = True
            
        except Exception as e:
            self.logger.warning(f"Error getting table info: {e}")
        
        return info 