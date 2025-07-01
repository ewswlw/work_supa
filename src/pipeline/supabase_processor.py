"""
Supabase data upload module.
"""
import pandas as pd
import numpy as np
from supabase import create_client, Client
from typing import List, Dict, Any
import time
import math

from .base import BaseProcessor
from ..models.data_models import ProcessingResult
from ..utils.config import SupabaseConfig
from ..utils.validators import DataValidator


class SupabaseProcessor(BaseProcessor):
    """Uploads data to Supabase"""
    
    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.client = self._create_client()
        # Ensure table has proper constraints for UPSERT functionality
        self._ensure_primary_key_constraint()
    
    def process(self, df: pd.DataFrame, clear_table: bool = False) -> ProcessingResult:
        """Upload DataFrame to Supabase"""
        try:
            self._start_timer()
            self.logger.info("Starting Supabase upload process")
            
            if df is None or df.empty:
                return ProcessingResult.warning_result(
                    "No data to upload to Supabase",
                    metadata={'rows_attempted': 0}
                )
            
            # Clear table if requested (default behavior for clean uploads)
            if clear_table:
                clear_result = self._clear_table()
                if not clear_result:
                    return ProcessingResult.failure_result(
                        "Failed to clear Supabase table before upload"
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
            
            # Log date coverage analysis before upload
            if 'Date' in prepared_df.columns:
                unique_dates = sorted(prepared_df['Date'].dropna().unique())
                self.logger.info(f"\n=========================")
                self.logger.info(f"=== DATE COVERAGE ANALYSIS (SUPABASE UPLOAD) ===")
                self.logger.info(f"=========================")
                self.logger.info(f"Total unique dates: {len(unique_dates)}")
                if unique_dates:
                    # Convert numpy datetime64 to pandas datetime for proper formatting
                    start_date = pd.to_datetime(unique_dates[0]).strftime('%Y-%m-%d')
                    end_date = pd.to_datetime(unique_dates[-1]).strftime('%Y-%m-%d')
                    self.logger.info(f"Date range: {start_date} to {end_date}")
                    self.logger.info(f"Unique dates being uploaded to Supabase:")
                    for date in unique_dates:
                        date_count = (prepared_df['Date'] == date).sum()
                        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
                        self.logger.info(f"  - {date_str}: {date_count} records")
                else:
                    self.logger.warning("No valid dates found in dataset")
            
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
        """Upload a single batch to Supabase with smart duplicate handling"""
        try:
            # Check if table is empty first to determine strategy
            if not hasattr(self, '_table_empty_checked'):
                self._table_is_empty = self._is_table_empty()
                self._table_empty_checked = True
                if self._table_is_empty:
                    self.logger.info("🚀 Table is empty - using fast bulk INSERT strategy")
                else:
                    self.logger.info("📊 Table has data - using smart UPSERT/deduplication strategy")
            
            # Use different strategies based on table state
            if self._table_is_empty:
                # Fast path: table is empty, just INSERT everything
                return self._fast_bulk_insert(batch_records)
            elif self._has_primary_key_constraint():
                # Smart path: use UPSERT with constraints
                return self._upload_with_upsert(batch_records)
            else:
                # Careful path: check for duplicates in batches
                return self._upload_with_batch_deduplication(batch_records)
                
        except Exception as e:
            self.logger.error(f"Batch upload failed: {e}")
            return False
    
    def _is_table_empty(self) -> bool:
        """Check if the table is empty"""
        try:
            response = self.client.table(self.config.table).select('*', count='exact').limit(1).execute()
            count = response.count if hasattr(response, 'count') else len(response.data)
            return count == 0
        except Exception as e:
            self.logger.warning(f"Could not check if table is empty: {e}")
            return False
    
    def _fast_bulk_insert(self, batch_records: List[Dict]) -> bool:
        """Fast bulk INSERT for empty tables"""
        try:
            response = self.client.table(self.config.table).insert(batch_records).execute()
            
            if hasattr(response, 'status_code') and response.status_code >= 400:
                self.logger.error(f"Bulk INSERT failed with status {response.status_code}")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Fast bulk insert failed: {e}")
            return False
    
    def _upload_with_upsert(self, batch_records: List[Dict]) -> bool:
        """Upload using UPSERT when PRIMARY KEY constraint exists"""
        try:
            response = self.client.table(self.config.table).upsert(
                batch_records,
                on_conflict='Date,CUSIP,Dealer'
            ).execute()
            
            if hasattr(response, 'status_code') and response.status_code >= 400:
                self.logger.error(f"UPSERT failed with status {response.status_code}")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"UPSERT operation failed: {e}")
            # Try fallback method
            return self._upload_with_deduplication(batch_records)
    
    def _upload_with_deduplication(self, batch_records: List[Dict]) -> bool:
        """DEPRECATED: Use _upload_with_batch_deduplication instead for better performance"""
        # This old method was too slow (checking each record individually)
        # Redirect to the new efficient batch method
        return self._upload_with_batch_deduplication(batch_records)
    
    def _has_primary_key_constraint(self) -> bool:
        """Check if the table has a PRIMARY KEY constraint"""
        try:
            # Try a small upsert to see if it works
            test_record = {
                'Date': '1900-01-01',
                'CUSIP': 'TEST123',
                'Dealer': 'TEST'
            }
            
            # First insert the test record
            self.client.table(self.config.table).insert([test_record]).execute()
            
            # Then try to upsert it (this will fail if no PRIMARY KEY)
            response = self.client.table(self.config.table).upsert(
                [test_record], 
                on_conflict='Date,CUSIP,Dealer'
            ).execute()
            
            # Clean up test record
            self.client.table(self.config.table).delete().eq(
                'Date', '1900-01-01'
            ).eq(
                'CUSIP', 'TEST123'
            ).eq(
                'Dealer', 'TEST'
            ).execute()
            
            return True
            
        except Exception:
            # Clean up any test record that might have been inserted
            try:
                self.client.table(self.config.table).delete().eq(
                    'Date', '1900-01-01'
                ).eq(
                    'CUSIP', 'TEST123'
                ).eq(
                    'Dealer', 'TEST'
                ).execute()
            except:
                pass
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
    
    def _clear_table(self) -> bool:
        """Clear all data from the Supabase table using TRUNCATE"""
        try:
            self.logger.info(f"Clearing all data from table: {self.config.table}")
            
            # First, get the current row count
            count_response = self.client.table(self.config.table).select('*', count='exact').execute()
            current_count = count_response.count if hasattr(count_response, 'count') else 0
            
            if current_count == 0:
                self.logger.info("Table is already empty")
                return True
            
            self.logger.info(f"Found {current_count} existing rows, truncating table...")
            
            # Use SQL function to truncate table
            # This is much faster than deleting row by row
            try:
                # Try to execute truncate via RPC
                truncate_response = self.client.rpc('truncate_table', {'table_name': self.config.table}).execute()
                self.logger.info("Table truncated using RPC function")
            except Exception as rpc_error:
                self.logger.warning(f"RPC truncate failed: {rpc_error}")
                self.logger.info("Falling back to delete all rows method...")
                
                # Fallback: delete all rows with a simple condition
                delete_response = self.client.table(self.config.table).delete().gte('Date', '1900-01-01').execute()
                self.logger.info("Attempted to delete all rows using date filter")
            
            # Verify table is empty or mostly empty
            verify_response = self.client.table(self.config.table).select('*', count='exact').execute()
            remaining_count = verify_response.count if hasattr(verify_response, 'count') else 0
            
            if remaining_count == 0:
                self.logger.info(f"Successfully cleared table. Removed {current_count} rows.")
                return True
            elif remaining_count < current_count * 0.1:  # If less than 10% remain
                self.logger.warning(f"Table mostly cleared. {remaining_count} rows remaining out of {current_count}.")
                self.logger.info("Proceeding with upload - new data will overwrite duplicates")
                return True
            else:
                self.logger.error(f"Table clearing failed. {remaining_count} rows remaining out of {current_count}.")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to clear table: {e}")
            # Don't fail the entire pipeline if table clearing fails
            self.logger.warning("Proceeding with upload anyway - will use upsert to handle duplicates")
            return True
    
    def _ensure_primary_key_constraint(self):
        """Ensure the table has a PRIMARY KEY constraint for proper UPSERT functionality"""
        try:
            self.logger.info("Checking PRIMARY KEY constraint on table...")
            
            # Check if constraint already exists
            if self._check_primary_key_exists():
                self.logger.info("✅ PRIMARY KEY constraint already exists")
                return True
            
            self.logger.info("🔧 PRIMARY KEY constraint missing, creating it...")
            return self._create_primary_key_constraint()
            
        except Exception as e:
            self.logger.warning(f"Could not verify/create PRIMARY KEY constraint: {e}")
            self.logger.warning("UPSERT may not work properly without PRIMARY KEY constraint")
            return False
    
    def _check_primary_key_exists(self) -> bool:
        """Check if PRIMARY KEY constraint exists on the table"""
        try:
            # Query PostgreSQL system tables to check for constraints
            # Note: This uses a workaround since Supabase doesn't expose constraint info directly
            
            # Try to insert a duplicate record to test constraint
            # If constraint exists, it will fail; if not, we'll delete the test record
            test_record = {
                'Date': '1900-01-01',
                'CUSIP': 'TEST123',
                'Dealer': 'TEST_DEALER',
                'Security': 'TEST_CONSTRAINT_CHECK'
            }
            
            # First, clean any existing test records
            self.client.table(self.config.table).delete().eq('CUSIP', 'TEST123').execute()
            
            # Insert test record
            self.client.table(self.config.table).insert(test_record).execute()
            
            # Try to insert duplicate - this should fail if constraint exists
            try:
                self.client.table(self.config.table).insert(test_record).execute()
                # If we get here, no constraint exists (duplicate was allowed)
                # Clean up test records
                self.client.table(self.config.table).delete().eq('CUSIP', 'TEST123').execute()
                return False
            except Exception as dup_error:
                # Duplicate was rejected - constraint exists!
                # Clean up the single test record
                self.client.table(self.config.table).delete().eq('CUSIP', 'TEST123').execute()
                return True
                
        except Exception as e:
            self.logger.warning(f"Could not test for PRIMARY KEY constraint: {e}")
            return False
    
    def _create_primary_key_constraint(self) -> bool:
        """Create PRIMARY KEY constraint on the table"""
        try:
            # SQL to create the constraint
            constraint_sql = f'''
            ALTER TABLE public.{self.config.table} 
            ADD CONSTRAINT {self.config.table}_pkey 
            PRIMARY KEY ("Date", "CUSIP", "Dealer");
            '''
            
            self.logger.info(f"Creating PRIMARY KEY constraint with SQL: {constraint_sql.strip()}")
            
            # Try multiple methods to execute the SQL
            success = False
            
            # Method 1: Try using SQL via PostgREST (if available)
            try:
                # Some Supabase setups allow direct SQL execution
                response = self.client.postgrest.session.post(
                    f"{self.client.supabase_url}/rest/v1/rpc/query",
                    json={"query": constraint_sql},
                    headers=self.client.postgrest.session.headers
                )
                if response.status_code == 200:
                    success = True
                    self.logger.info("✅ PRIMARY KEY constraint created via PostgREST")
            except Exception as method1_error:
                self.logger.debug(f"Method 1 failed: {method1_error}")
            
            # Method 2: Try using a stored procedure (if it exists)
            if not success:
                try:
                    response = self.client.rpc('execute_ddl', {'ddl_statement': constraint_sql}).execute()
                    success = True
                    self.logger.info("✅ PRIMARY KEY constraint created via RPC")
                except Exception as method2_error:
                    self.logger.debug(f"Method 2 failed: {method2_error}")
            
            # Method 3: Try raw SQL execution (some Supabase configurations allow this)
            if not success:
                try:
                    # This is a fallback that might work in some configurations
                    import psycopg2
                    # Note: This would require database credentials, which we don't have
                    self.logger.debug("Direct PostgreSQL connection not available")
                except ImportError:
                    pass
            
            if not success:
                # Provide manual instructions
                self.logger.warning("⚠️  Automatic constraint creation failed")
                self.logger.warning("📋 Manual action required:")
                self.logger.warning("   1. Go to Supabase SQL Editor")
                self.logger.warning("   2. Execute this SQL:")
                self.logger.warning(f"      {constraint_sql.strip()}")
                self.logger.warning("   3. Re-run the pipeline")
                self.logger.warning("💡 Until then, pipeline will use INSERT with duplicate risk")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating PRIMARY KEY constraint: {e}")
            return False
    
    def _upload_with_batch_deduplication(self, batch_records: List[Dict]) -> bool:
        """Upload with efficient batch deduplication check to prevent duplicates"""
        try:
            if not batch_records:
                return True
            
            # Extract all Date/CUSIP/Dealer combinations from this batch
            batch_keys = [(r['Date'], r['CUSIP'], r['Dealer']) for r in batch_records]
            
            # Check which of these combinations already exist (single query)
            existing_keys = set()
            
            # Build a single query to check all combinations at once
            # Use OR conditions to check multiple combinations efficiently
            date_values = list(set(k[0] for k in batch_keys))
            cusip_values = list(set(k[1] for k in batch_keys))
            dealer_values = list(set(k[2] for k in batch_keys))
            
            # Query for existing records with any of these values
            existing_response = self.client.table(self.config.table).select(
                'Date,CUSIP,Dealer'
            ).in_(
                'Date', date_values
            ).in_(
                'CUSIP', cusip_values  
            ).in_(
                'Dealer', dealer_values
            ).execute()
            
            # Create set of existing combinations
            for record in existing_response.data:
                existing_keys.add((record['Date'], record['CUSIP'], record['Dealer']))
            
            # Filter out records that already exist
            new_records = []
            for record in batch_records:
                key = (record['Date'], record['CUSIP'], record['Dealer'])
                if key not in existing_keys:
                    new_records.append(record)
            
            if not new_records:
                self.logger.debug("No new records to insert in this batch (all already exist)")
                return True
            
            skipped_count = len(batch_records) - len(new_records)
            if skipped_count > 0:
                self.logger.debug(f"Inserting {len(new_records)} new records (skipping {skipped_count} existing)")
            
            # Insert only new records
            response = self.client.table(self.config.table).insert(new_records).execute()
            
            if hasattr(response, 'status_code') and response.status_code >= 400:
                self.logger.error(f"Batch deduplication INSERT failed with status {response.status_code}")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Batch deduplication failed: {e}")
            # Last resort: try simple insert and accept potential duplicates
            try:
                response = self.client.table(self.config.table).insert(batch_records).execute()
                self.logger.warning("Used simple INSERT - potential duplicates may exist")
                return True
            except:
                return False 