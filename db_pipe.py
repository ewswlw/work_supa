"""
MAIN DATABASE PIPELINE
=====================

Central orchestrator for the trading analytics SQLite database system.
Integrates all components: logging, CUSIP validation, schema management,
and data processing with comprehensive error handling and monitoring.

Features:
- Complete schema initialization and validation
- CUSIP standardization and universe matching
- Incremental data loading with conflict resolution
- Comprehensive logging and audit trails
- Data quality validation
- Backup and maintenance operations
- Performance monitoring and optimization
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, time as datetime_time
import argparse
import time
import pandas as pd
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import gc
import logging

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import database components
from db.database.connection import DatabaseConnection
from db.database.schema import DatabaseSchema
from db.utils.db_logger import DatabaseLogger
from db.utils.cusip_standardizer import CUSIPStandardizer

# Import existing pipeline components for data reading
from src.utils.config import load_config
from src.utils.expert_logging import setup_logging
from src.pipeline.excel_processor import ExcelProcessor
from src.pipeline.parquet_processor import ParquetProcessor


class DatabasePipeline:
    """
    Main database pipeline orchestrator for trading analytics.
    
    Manages:
    - Database initialization and schema management
    - Data ingestion from existing pipeline outputs
    - CUSIP validation and standardization
    - Incremental updates and data versioning
    - Comprehensive logging and monitoring
    - Data quality validation and reporting
    """
    
    def __init__(self, database_path: str = "trading_analytics.db", config_path: str = "config/config.yaml",
                 batch_size: int = 1000, parallel: bool = False, low_memory: bool = False, 
                 optimize_db: bool = False, disable_logging: bool = False):
        """
        Initialize database pipeline with configuration and optimization options.
        
        Args:
            database_path: Path to SQLite database file
            config_path: Path to configuration file
            batch_size: Batch size for database operations
            parallel: Enable parallel processing for CUSIP standardization
            low_memory: Enable low memory mode with garbage collection
            optimize_db: Optimize database after loading
            disable_logging: Disable detailed logging for faster execution
        """
        self.database_path = Path(database_path)
        self.config_path = Path(config_path)
        self.batch_size = batch_size
        self.parallel = parallel
        self.low_memory = low_memory
        self.optimize_db = optimize_db
        self.disable_logging = disable_logging
        
        # Load configuration
        self.config = load_config() if self.config_path.exists() else {}
        
        # Initialize logging with optimization options
        self.logger = DatabaseLogger(log_dir="logs")
        
        # Set log level based on optimization settings
        if disable_logging:
            # Set all loggers to WARNING level to reduce output
            for logger_name in ['db', 'db_sql', 'db_cusip', 'db_audit', 'db_perf']:
                logger = logging.getLogger(f'db_{logger_name}')
                logger.setLevel(logging.WARNING)
        
        # Initialize database components
        db_config = {
            'connection_timeout': 60.0,
            'busy_timeout': 60000,  # 60 seconds
            'retry_attempts': 3,
            'retry_delay': 2.0
        }
        
        self.db_connection = DatabaseConnection(
            str(self.database_path), 
            logger=self.logger, 
            config=db_config
        )
        
        self.db_schema = DatabaseSchema(logger=self.logger)
        self.cusip_standardizer = CUSIPStandardizer(
            logger=self.logger, 
            enable_check_digit_validation=True
        )
        
        # Pipeline state
        self.pipeline_stats = {
            'total_records_processed': 0,
            'cusips_matched': 0,
            'cusips_unmatched': 0,
            'tables_updated': [],
            'errors_encountered': 0,
            'start_time': None,
            'end_time': None
        }
        
        self._log_pipeline_event("Database pipeline initialized with optimizations", {
            'database_path': str(self.database_path),
            'config_path': str(self.config_path),
            'batch_size': batch_size,
            'parallel': parallel,
            'low_memory': low_memory,
            'optimize_db': optimize_db,
            'disable_logging': disable_logging
        })
    
    def initialize_database(self, force_recreate: bool = False) -> bool:
        """
        Initialize database with complete schema.
        
        Args:
            force_recreate: Whether to recreate database even if it exists
            
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            with self.logger.operation_context("database_initialization"):
                
                # Check if database exists and is valid
                if self.database_path.exists() and not force_recreate:
                    self._log_pipeline_event("Database file exists, validating schema")
                    
                    # Validate existing schema
                    conn = self.db_connection.connect()
                    validation_results = self.db_schema.validate_schema(conn)
                    
                    if validation_results['schema_valid']:
                        self._log_pipeline_event("Existing database schema is valid")
                        return True
                    else:
                        self._log_pipeline_event("Schema validation failed, recreating database", {
                            'missing_tables': validation_results['missing_tables'],
                            'missing_views': validation_results['missing_views'],
                            'missing_indexes': validation_results['missing_indexes']
                        })
                        force_recreate = True
                
                # Create or recreate database
                if force_recreate and self.database_path.exists():
                    self._log_pipeline_event("Removing existing database file")
                    self.db_connection.disconnect()
                    os.unlink(self.database_path)
                
                # Initialize database connection
                self._log_pipeline_event("Establishing database connection")
                conn = self.db_connection.connect()
                
                # Create complete schema
                self._log_pipeline_event("Creating database schema")
                schema_success = self.db_schema.create_complete_schema(conn)
                
                if not schema_success:
                    raise Exception("Failed to create database schema")
                
                # Validate schema creation
                validation_results = self.db_schema.validate_schema(conn)
                if not validation_results['schema_valid']:
                    raise Exception(f"Schema validation failed: {validation_results}")
                
                # Run initial health check
                health_results = self.db_connection.check_health()
                if not health_results['connection_healthy']:
                    raise Exception(f"Database health check failed: {health_results}")
                
                self._log_pipeline_event("Database initialization completed successfully", {
                    'database_size_mb': health_results['database_size_mb'],
                    'tables_created': len(self.db_schema.core_tables) + 
                                    len(self.db_schema.cusip_tracking_tables) + 
                                    len(self.db_schema.audit_tables),
                    'views_created': len(self.db_schema.views),
                    'indexes_created': len(self.db_schema.indexes)
                })
                
                return True
                
        except Exception as e:
            self._log_pipeline_error("Database initialization failed", e)
            return False
    
    def load_universe_data(self, universe_file: str, force_full_refresh: bool = False) -> bool:
        """
        Load universe data with incremental update logic.
        
        Args:
            universe_file: Path to universe parquet file
            force_full_refresh: Whether to force full refresh instead of incremental
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            with self.logger.operation_context("load_universe_data", {'file': universe_file}):
                
                # Read universe data - handle both CSV and parquet files
                if universe_file.lower().endswith('.csv'):
                    # Use pandas to read CSV
                    df = pd.read_csv(universe_file)
                    result = type('ProcessingResult', (), {'success': True, 'data': df, 'error': None})()
                else:
                    # Use parquet processor for parquet files
                    parquet_processor = ParquetProcessor(config={}, logger=self.logger.db_logger)
                    result = parquet_processor.load_from_parquet(universe_file)
                if result.success:
                    universe_df = result.data
                else:
                    raise Exception(f"Failed to load universe file: {result.error}")
                
                if universe_df is None or universe_df.empty:
                    raise Exception("Universe file is empty or could not be read")
                
                # Data validation and cleaning
                self._log_pipeline_event("Validating and cleaning universe data")
                
                # Handle G Sprd outliers - clamp to valid range or set to NULL
                if 'G Sprd' in universe_df.columns:
                    original_count = len(universe_df)
                    outliers_mask = (universe_df['G Sprd'] < -1000) | (universe_df['G Sprd'] > 1000)
                    outlier_count = outliers_mask.sum()
                    
                    if outlier_count > 0:
                        self._log_pipeline_event("Found G Sprd outliers, applying data cleaning", {
                            'total_records': original_count,
                            'outlier_count': outlier_count,
                            'outlier_percentage': (outlier_count / original_count) * 100
                        })
                        
                        # Set extreme outliers (>10000) to NULL, clamp others to range
                        extreme_outliers = (universe_df['G Sprd'] < -10000) | (universe_df['G Sprd'] > 10000)
                        universe_df.loc[extreme_outliers, 'G Sprd'] = None
                        
                        # Clamp remaining outliers to valid range
                        universe_df.loc[universe_df['G Sprd'] < -1000, 'G Sprd'] = -1000
                        universe_df.loc[universe_df['G Sprd'] > 1000, 'G Sprd'] = 1000
                        
                        self._log_pipeline_event("G Sprd data cleaning completed", {
                            'extreme_outliers_set_to_null': extreme_outliers.sum(),
                            'outliers_clamped_to_range': outlier_count - extreme_outliers.sum()
                        })
                
                self._log_pipeline_event("Universe data loaded from file", {
                    'file_path': universe_file,
                    'rows_loaded': len(universe_df),
                    'columns': list(universe_df.columns),
                    'date_range': f"{universe_df['date'].min()} to {universe_df['date'].max()}" if 'date' in universe_df.columns else 'No date column'
                })
                
                # Determine incremental vs full refresh
                update_decision = self._decide_update_strategy(
                    table_name='universe_historical',
                    source_file=universe_file,
                    new_data_df=universe_df,
                    force_full_refresh=force_full_refresh
                )
                
                # Process universe data
                processed_records = 0
                with self.db_connection.transaction():
                    
                    if update_decision['update_type'] == 'full_refresh':
                        # Clear existing data
                        self.db_connection.execute_query(
                            "DELETE FROM universe_historical",
                            fetch_results=False
                        )
                        self._log_pipeline_event("Cleared existing universe data for full refresh")
                    
                    # Process data in batches
                    batch_size = 1000
                    for start_idx in range(0, len(universe_df), batch_size):
                        end_idx = min(start_idx + batch_size, len(universe_df))
                        batch_df = universe_df.iloc[start_idx:end_idx]
                        
                        # Standardize CUSIPs in batch
                        cusip_results = self.cusip_standardizer.standardize_cusip_batch(
                            batch_df['CUSIP'],
                            context={'table_name': 'universe_historical', 'source_file': universe_file}
                        )
                        
                        # Prepare batch data for insertion
                        batch_records = []
                        for idx, (_, row) in enumerate(batch_df.iterrows()):
                            cusip_result = cusip_results.iloc[idx]
                            
                            # Convert pandas Timestamp to string for SQLite compatibility
                            date_str = str(row['Date']) if pd.notna(row['Date']) else None
                            
                            record = (
                                date_str,
                                row['CUSIP'],
                                cusip_result['cusip_standardized'],
                                row.get('Security', ''),
                                row.get('G Sprd'),
                                row.get('OAS (Mid)'),
                                row.get('Yrs (Mat)'),
                                row.get('Rating', ''),
                                universe_file,
                                date_str  # file_date same as date for universe
                            )
                            batch_records.append(record)
                        
                        # Insert batch
                        insert_sql = """
                            INSERT OR REPLACE INTO universe_historical 
                            ("Date", "CUSIP", cusip_standardized, "Security", "G Sprd", "OAS (Mid)", 
                             "Yrs (Mat)", "Rating", source_file, file_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        rows_affected = self.db_connection.execute_many(insert_sql, batch_records)
                        processed_records += rows_affected
                        
                        self._log_pipeline_event(f"Processed universe batch {start_idx//batch_size + 1}", {
                            'records_in_batch': len(batch_records),
                            'rows_affected': rows_affected,
                            'total_processed': processed_records
                        })
                
                # Update pipeline statistics
                self.pipeline_stats['total_records_processed'] += processed_records
                self.pipeline_stats['tables_updated'].append('universe_historical')
                
                self._log_pipeline_event("Universe data loading completed", {
                    'total_records_processed': processed_records,
                    'update_type': update_decision['update_type'],
                    'file_processed': universe_file
                })
                
                return True
                
        except Exception as e:
            self.pipeline_stats['errors_encountered'] += 1
            self._log_pipeline_error("Universe data loading failed", e, {'file': universe_file})
            return False
    
    def load_portfolio_data(self, portfolio_file: str, force_full_refresh: bool = False) -> bool:
        """
        Load portfolio data with CUSIP validation against universe.
        
        Args:
            portfolio_file: Path to portfolio parquet file
            force_full_refresh: Whether to force full refresh instead of incremental
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            with self.logger.operation_context("load_portfolio_data", {'file': portfolio_file}):
                
                # Read portfolio data - handle both CSV and parquet files
                if portfolio_file.lower().endswith('.csv'):
                    # Use pandas to read CSV
                    df = pd.read_csv(portfolio_file)
                    result = type('ProcessingResult', (), {'success': True, 'data': df, 'error': None})()
                else:
                    # Use parquet processor for parquet files
                    parquet_processor = ParquetProcessor(config={}, logger=self.logger.db_logger)
                    result = parquet_processor.load_from_parquet(portfolio_file)
                if result.success:
                    portfolio_df = result.data
                else:
                    raise Exception(f"Failed to load portfolio file: {result.error}")
                
                if portfolio_df is None or portfolio_df.empty:
                    raise Exception("Portfolio file is empty or could not be read")
                
                # Data validation and cleaning
                self._log_pipeline_event("Validating and cleaning portfolio data")
                
                # Handle negative quantities - these are normal for portfolio data (short positions)
                if 'QUANTITY' in portfolio_df.columns:
                    original_count = len(portfolio_df)
                    negative_quantities = (portfolio_df['QUANTITY'] < 0).sum()
                    
                    if negative_quantities > 0:
                        self._log_pipeline_event("Found negative quantities in portfolio data", {
                            'total_records': original_count,
                            'negative_quantities': negative_quantities,
                            'negative_percentage': (negative_quantities / original_count) * 100
                        })
                        
                        # For portfolio data, negative quantities are normal (short positions)
                        # We'll keep them as-is since this is expected behavior
                        self._log_pipeline_event("Negative quantities represent short positions - keeping as-is")
                
                self._log_pipeline_event("Portfolio data loaded from file", {
                    'file_path': portfolio_file,
                    'rows_loaded': len(portfolio_df),
                    'columns': list(portfolio_df.columns)
                })
                
                # Get current universe for CUSIP validation
                universe_cusips = self._get_current_universe_cusips()
                
                # Determine update strategy
                update_decision = self._decide_update_strategy(
                    table_name='portfolio_historical',
                    source_file=portfolio_file,
                    new_data_df=portfolio_df,
                    force_full_refresh=force_full_refresh
                )
                
                # Process portfolio data
                processed_records = 0
                matched_cusips = 0
                unmatched_cusips = 0
                
                with self.db_connection.transaction():
                    
                    if update_decision['update_type'] == 'full_refresh':
                        # Clear existing data
                        self.db_connection.execute_query(
                            "DELETE FROM portfolio_historical",
                            fetch_results=False
                        )
                        self._log_pipeline_event("Cleared existing portfolio data for full refresh")
                    
                    # Process data in batches
                    batch_size = 1000
                    for start_idx in range(0, len(portfolio_df), batch_size):
                        end_idx = min(start_idx + batch_size, len(portfolio_df))
                        batch_df = portfolio_df.iloc[start_idx:end_idx]
                        
                        # Standardize CUSIPs and validate against universe
                        batch_records = []
                        batch_unmatched = []
                        
                        for _, row in batch_df.iterrows():
                            # Standardize CUSIP
                            cusip_result = self.cusip_standardizer.standardize_cusip(
                                row['CUSIP'],
                                context={'table_name': 'portfolio_historical', 'source_file': portfolio_file}
                            )
                            
                            standardized_cusip = cusip_result['cusip_standardized']
                            
                            # Check universe match
                            universe_match = standardized_cusip in universe_cusips
                            if universe_match:
                                matched_cusips += 1
                                match_status = 'matched'
                            else:
                                unmatched_cusips += 1
                                match_status = 'unmatched'
                                
                                # Track unmatched CUSIP
                                unmatched_date = str(row['Date']) if pd.notna(row['Date']) else None
                                batch_unmatched.append({
                                    'cusip_original': row['CUSIP'],
                                    'cusip_standardized': standardized_cusip,
                                    'security_name': row.get('SECURITY', ''),
                                    'date': unmatched_date
                                })
                            
                            # Convert pandas Timestamp to string for SQLite compatibility
                            date_str = str(row['Date']) if pd.notna(row['Date']) else None
                            
                            # Prepare record for insertion
                            record = (
                                date_str,
                                row['CUSIP'],
                                standardized_cusip,
                                row.get('SECURITY', ''),
                                row.get('QUANTITY'),
                                row.get('PRICE'),
                                row.get('VALUE'),  # This maps to MARKET VALUE column
                                row.get('VALUE PCT NAV'),  # This maps to WEIGHT column
                                match_status,
                                datetime.now().date(),
                                portfolio_file,
                                date_str  # file_date same as date
                            )
                            batch_records.append(record)
                        
                        # Insert portfolio batch
                        insert_sql = """
                            INSERT OR REPLACE INTO portfolio_historical 
                            ("Date", "CUSIP", cusip_standardized, "SECURITY", "QUANTITY", "PRICE", "MARKET VALUE", 
                             "WEIGHT", universe_match_status, universe_match_date, 
                             source_file, file_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        rows_affected = self.db_connection.execute_many(insert_sql, batch_records)
                        processed_records += rows_affected
                        
                        # Insert unmatched CUSIPs
                        if batch_unmatched:
                            self._insert_unmatched_cusips('portfolio_historical', batch_unmatched, portfolio_file)
                        
                        self._log_pipeline_event(f"Processed portfolio batch {start_idx//batch_size + 1}", {
                            'records_in_batch': len(batch_records),
                            'rows_affected': rows_affected,
                            'matched_cusips': matched_cusips,
                            'unmatched_cusips': unmatched_cusips
                        })
                
                # Update pipeline statistics
                self.pipeline_stats['total_records_processed'] += processed_records
                self.pipeline_stats['cusips_matched'] += matched_cusips
                self.pipeline_stats['cusips_unmatched'] += unmatched_cusips
                self.pipeline_stats['tables_updated'].append('portfolio_historical')
                
                match_rate = (matched_cusips / (matched_cusips + unmatched_cusips)) * 100 if (matched_cusips + unmatched_cusips) > 0 else 0
                
                self._log_pipeline_event("Portfolio data loading completed", {
                    'total_records_processed': processed_records,
                    'match_rate_percentage': match_rate,
                    'matched_cusips': matched_cusips,
                    'unmatched_cusips': unmatched_cusips,
                    'update_type': update_decision['update_type']
                })
                
                return True
                
        except Exception as e:
            self.pipeline_stats['errors_encountered'] += 1
            self._log_pipeline_error("Portfolio data loading failed", e, {'file': portfolio_file})
            return False
    
    def load_combined_runs_data(self, runs_file: str, force_full_refresh: bool = False) -> bool:
        """
        Load combined runs data with incremental update logic.
        
        Args:
            runs_file: Path to combined runs parquet file
            force_full_refresh: Whether to force full refresh instead of incremental
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            with self.logger.operation_context("load_combined_runs_data", {'file': runs_file}):
                
                # Read combined runs data - handle both CSV and parquet files
                if runs_file.lower().endswith('.csv'):
                    # Use pandas to read CSV
                    df = pd.read_csv(runs_file)
                    result = type('ProcessingResult', (), {'success': True, 'data': df, 'error': None})()
                else:
                    # Use parquet processor for parquet files
                    parquet_processor = ParquetProcessor(config={}, logger=self.logger.db_logger)
                    result = parquet_processor.load_from_parquet(runs_file)
                
                if not result.success:
                    raise Exception(f"Failed to load combined runs data: {result.error}")
                
                df = result.data
                self._log_pipeline_event("Combined runs data loaded successfully", {
                    'file': runs_file,
                    'records': len(df),
                    'columns': list(df.columns)
                })
                
                # Standardize CUSIPs with error handling for logging issues
                df['cusip_original'] = df['CUSIP'].copy()
                
                def safe_standardize_cusip(cusip):
                    if pd.isna(cusip):
                        return None
                    try:
                        result = self.cusip_standardizer.standardize_cusip(cusip)
                        if isinstance(result, dict):
                            standardized = result.get('cusip_standardized')
                            if standardized and standardized.strip():  # Check if we got a valid result
                                return standardized
                            else:
                                # If standardization failed, return original CUSIP as fallback
                                return cusip
                        else:
                            return result if result else cusip  # Fallback to original if None
                    except Exception as e:
                        # Log the error but don't fail the pipeline
                        print(f"Warning: CUSIP standardization failed for {cusip}: {e}")
                        return cusip  # Return original CUSIP as fallback
                
                df['cusip_standardized'] = df['CUSIP'].apply(safe_standardize_cusip)
                
                # Handle unmatched CUSIPs
                unmatched_mask = df['cusip_standardized'].isna()
                unmatched_count = unmatched_mask.sum()
                
                if unmatched_count > 0:
                    self._log_pipeline_event(f"Found {unmatched_count} unmatched CUSIPs in combined runs data")
                    unmatched_list = df[unmatched_mask][['cusip_original', 'Security']].to_dict('records')
                    self._insert_unmatched_cusips('combined_runs_historical', unmatched_list, runs_file)
                    df = df[~unmatched_mask]  # Remove unmatched records
                
                # Prepare data for database insertion
                df['date'] = pd.to_datetime(df['Date']).dt.date
                df['source_file'] = runs_file
                df['file_date'] = df['date']  # file_date same as date for combined runs
                df['loaded_timestamp'] = datetime.now()
                
                # Aggregate data by date, CUSIP, and dealer to handle duplicates
                self._log_pipeline_event("Checking for duplicates and taking most recent records")
                
                # Check if there are duplicates
                original_count = len(df)
                unique_combinations = len(df.groupby(['date', 'cusip_standardized', 'Dealer']))
                
                if original_count == unique_combinations:
                    self._log_pipeline_event("No duplicates found - data is already unique")
                    # No aggregation needed
                else:
                    self._log_pipeline_event(f"Found {original_count - unique_combinations} duplicates - taking most recent records")
                    
                    # Sort by date, CUSIP, dealer, and time (descending) to get most recent first
                    df = df.sort_values(['date', 'cusip_standardized', 'Dealer', 'Time'], ascending=[True, True, True, False])
                    
                    # Take the first (most recent) record for each date/CUSIP/dealer combination
                    df = df.groupby(['date', 'cusip_standardized', 'Dealer']).first().reset_index()
                    
                    self._log_pipeline_event("Most recent records selected", {
                        'original_records': original_count,
                        'final_records': len(df),
                        'duplicates_removed': original_count - len(df)
                    })
                
                # Data is already deduplicated by taking most recent records (no averaging needed)
                
                # Convert timestamp columns to strings for SQLite compatibility
                timestamp_columns = ['date']
                for col in timestamp_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str)
                
                # Decide update strategy
                update_strategy = self._decide_update_strategy(
                    'combined_runs_historical', runs_file, df, force_full_refresh
                )
                
                # Execute database operations
                conn = self.db_connection.connect()
                
                if update_strategy['update_type'] == 'full_refresh':
                    self._log_pipeline_event("Performing full refresh of combined runs data")
                    
                    # Clear existing data
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM combined_runs_historical")
                    
                    # Insert new data in batches
                    batch_size = 1000
                    total_batches = (len(df) + batch_size - 1) // batch_size
                    
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i:i+batch_size]
                        batch_records = batch.to_dict('records')
                        
                        # Transform column names to match SQL parameters (replace spaces with underscores)
                        # Only keep the keys required by the SQL insert
                        required_keys = [
                            'date', 'cusip_original', 'cusip_standardized', 'Security', 'Dealer',
                            'Bid_Spread', 'Ask_Spread', 'Bid_Size', 'Ask_Size',
                            'Bid_Interpolated_Spread_to_Government', 'Keyword',
                            'source_file', 'file_date', 'loaded_timestamp'
                        ]
                        
                        # Transform and filter records
                        transformed_batch_records = []
                        for record in batch_records:
                            transformed_record = {}
                            for key, value in record.items():
                                if key == 'Bid Spread':
                                    transformed_record['Bid_Spread'] = value
                                elif key == 'Ask Spread':
                                    transformed_record['Ask_Spread'] = value
                                elif key == 'Bid Size':
                                    transformed_record['Bid_Size'] = value
                                elif key == 'Ask Size':
                                    transformed_record['Ask_Size'] = value
                                elif key == 'Bid Interpolated Spread to Government':
                                    transformed_record['Bid_Interpolated_Spread_to_Government'] = value
                                else:
                                    transformed_record[key] = value
                            # Convert datetime/timestamp objects to strings for SQLite compatibility
                            for k, v in transformed_record.items():
                                if isinstance(v, (pd.Timestamp, datetime, datetime_time)):
                                    transformed_record[k] = str(v)
                            # Filter to only required keys
                            filtered_record = {k: transformed_record.get(k, None) for k in required_keys}
                            transformed_batch_records.append(filtered_record)
                        batch_records = transformed_batch_records
                        if batch_records:
                            print(f"DEBUG: Combined runs batch {i//batch_size + 1}, first record keys: {list(batch_records[0].keys())}")
                            print(f"DEBUG: Combined runs batch {i//batch_size + 1}, first record types: {[type(v) for v in batch_records[0].values()]}")
                        
                        # Insert batch
                        cursor.executemany("""
                            INSERT INTO combined_runs_historical (
                                "Date", "CUSIP", cusip_standardized, "Security", "Dealer", 
                                "Bid Spread", "Ask Spread", "Bid Size", "Ask Size",
                                "Bid Interpolated Spread to Government", "Keyword",
                                source_file, file_date, loaded_timestamp
                            ) VALUES (
                                :date, :cusip_original, :cusip_standardized, :Security, :Dealer,
                                :Bid_Spread, :Ask_Spread, :Bid_Size, :Ask_Size,
                                :Bid_Interpolated_Spread_to_Government, :Keyword,
                                :source_file, :file_date, :loaded_timestamp
                            )
                        """, batch_records)
                        
                        batch_num = (i // batch_size) + 1
                        self._log_pipeline_event(f"Inserted batch {batch_num}/{total_batches}", {
                            'batch_size': len(batch),
                            'total_inserted': i + len(batch)
                        })
                    
                    conn.commit()
                    
                else:  # Incremental update
                    self._log_pipeline_event("Performing incremental update of combined runs data")
                    
                    # Get existing dates
                    cursor = conn.cursor()
                    cursor.execute("SELECT DISTINCT date FROM combined_runs_historical")
                    existing_dates = {row[0] for row in cursor.fetchall()}
                    
                    # Filter to new dates only
                    new_dates = set(df['date'].unique()) - existing_dates
                    new_data = df[df['date'].isin(new_dates)]
                    
                    if len(new_data) > 0:
                        # Insert new data in batches
                        batch_size = 1000
                        total_batches = (len(new_data) + batch_size - 1) // batch_size
                        
                        for i in range(0, len(new_data), batch_size):
                            batch = new_data.iloc[i:i+batch_size]
                            batch_records = batch.to_dict('records')
                            
                            # Transform column names to match SQL parameters (replace spaces with underscores)
                            for idx, record in enumerate(batch_records):
                                transformed_record = {}
                                for key, value in record.items():
                                    if key == 'Bid Spread':
                                        transformed_record['Bid_Spread'] = value
                                    elif key == 'Ask Spread':
                                        transformed_record['Ask_Spread'] = value
                                    elif key == 'Bid Size':
                                        transformed_record['Bid_Size'] = value
                                    elif key == 'Ask Size':
                                        transformed_record['Ask_Size'] = value
                                    elif key == 'Bid Interpolated Spread to Government':
                                        transformed_record['Bid_Interpolated_Spread_to_Government'] = value
                                    else:
                                        transformed_record[key] = value
                                
                                # Convert datetime/timestamp objects to strings for SQLite compatibility
                                for key, value in transformed_record.items():
                                    if isinstance(value, (pd.Timestamp, datetime, datetime_time)):
                                        transformed_record[key] = str(value)
                                
                                batch_records[idx] = transformed_record
                            
                            cursor.executemany("""
                                INSERT INTO combined_runs_historical (
                                    "Date", "CUSIP", cusip_standardized, "Security", "Dealer", 
                                    "Bid Spread", "Ask Spread", "Bid Size", "Ask Size",
                                    "Bid Interpolated Spread to Government", "Keyword",
                                    source_file, file_date, loaded_timestamp
                                ) VALUES (
                                    :date, :cusip_original, :cusip_standardized, :Security, :Dealer,
                                    :Bid_Spread, :Ask_Spread, :Bid_Size, :Ask_Size,
                                    :Bid_Interpolated_Spread_to_Government, :Keyword,
                                    :source_file, :file_date, :loaded_timestamp
                                )
                            """, batch_records)
                            
                            batch_num = (i // batch_size) + 1
                            self._log_pipeline_event(f"Inserted batch {batch_num}/{total_batches}", {
                                'batch_size': len(batch),
                                'total_inserted': i + len(batch)
                            })
                        
                        conn.commit()
                    else:
                        self._log_pipeline_event("No new data to insert for combined runs")
                
                # Update pipeline statistics
                self.pipeline_stats['total_records_processed'] += len(df)
                self.pipeline_stats['cusips_matched'] += len(df) - unmatched_count
                self.pipeline_stats['cusips_unmatched'] += unmatched_count
                self.pipeline_stats['tables_updated'].append('combined_runs_historical')
                
                self._log_pipeline_event("Combined runs data loading completed successfully", {
                    'records_processed': len(df),
                    'cusips_matched': len(df) - unmatched_count,
                    'cusips_unmatched': unmatched_count,
                    'update_strategy': update_strategy['update_type']
                })
                
                return True
                
        except Exception as e:
            self._log_pipeline_error("Failed to load combined runs data", e, {'file': runs_file})
            return False

    def load_run_monitor_data(self, run_monitor_file: str, force_full_refresh: bool = False) -> bool:
        """
        Load run monitor analytics data with full refresh logic.
        
        Args:
            run_monitor_file: Path to run monitor parquet file
            force_full_refresh: Whether to force full refresh (always true for run monitor)
            
        Returns:
            True if load successful, False otherwise
        """
        print(f"DEBUG: Starting load_run_monitor_data with file: {run_monitor_file}")
        try:
            print(f"DEBUG: Entering try block")
            with self.logger.operation_context("load_run_monitor_data", {'file': run_monitor_file}):
                print(f"DEBUG: Inside logger context")
                
                # Read run monitor data - handle both CSV and parquet files
                print(f"DEBUG: Reading file: {run_monitor_file}")
                if run_monitor_file.lower().endswith('.csv'):
                    # Use pandas to read CSV
                    df = pd.read_csv(run_monitor_file)
                    result = type('ProcessingResult', (), {'success': True, 'data': df, 'error': None})()
                else:
                    # Use parquet processor for parquet files
                    parquet_processor = ParquetProcessor(config={}, logger=self.logger.db_logger)
                    result = parquet_processor.load_from_parquet(run_monitor_file)
                print(f"DEBUG: Parquet load result: {result}")
                
                if not result.success:
                    print(f"DEBUG: Parquet load failed: {result.error}")
                    raise Exception(f"Failed to load run monitor data: {result.error}")
                print(f"DEBUG: Parquet load successful, proceeding to data processing")
                
                df = result.data
                print(f"DEBUG: Data loaded, shape: {df.shape}, columns: {list(df.columns)}")
                self._log_pipeline_event("Run monitor data loaded successfully", {
                    'file': run_monitor_file,
                    'records': len(df),
                    'columns': list(df.columns)
                })
                
                # Standardize CUSIPs with error handling for logging issues
                print(f"DEBUG: Standardizing CUSIPs")
                df['cusip_original'] = df['CUSIP'].copy()
                
                def safe_standardize_cusip(cusip):
                    if pd.isna(cusip):
                        return None
                    try:
                        result = self.cusip_standardizer.standardize_cusip(cusip)
                        if isinstance(result, dict):
                            return result.get('standardized_cusip')
                        else:
                            return result
                    except Exception as e:
                        # Log the error but don't fail the pipeline
                        print(f"Warning: CUSIP standardization failed for {cusip}: {e}")
                        return cusip  # Return original CUSIP as fallback
                
                df['cusip_standardized'] = df['CUSIP'].apply(safe_standardize_cusip)
                print(f"DEBUG: CUSIP standardization complete, standardized count: {df['cusip_standardized'].notna().sum()}, nulls: {df['cusip_standardized'].isna().sum()}")
                
                # Handle unmatched CUSIPs
                unmatched_mask = df['cusip_standardized'].isna()
                unmatched_count = unmatched_mask.sum()
                
                if unmatched_count > 0:
                    self._log_pipeline_event(f"Found {unmatched_count} unmatched CUSIPs in run monitor data")
                    unmatched_list = df[unmatched_mask][['cusip_original', 'Security']].to_dict('records')
                    self._insert_unmatched_cusips('run_monitor', unmatched_list, run_monitor_file)
                    df = df[~unmatched_mask]  # Remove unmatched records
                
                # Aggregate data by CUSIP (since run_monitor has unique constraint on cusip_standardized)
                self._log_pipeline_event("Aggregating run monitor data by CUSIP")
                
                # Group by CUSIP and aggregate metrics - include all columns from parquet file
                agg_columns = {}
                
                # Core columns that need aggregation
                if 'Bid Spread' in df.columns:
                    agg_columns['Bid Spread'] = 'mean'
                if 'Ask Spread' in df.columns:
                    agg_columns['Ask Spread'] = 'mean'
                if 'Bid Size' in df.columns:
                    agg_columns['Bid Size'] = 'sum'
                if 'Ask Size' in df.columns:
                    agg_columns['Ask Size'] = 'sum'
                if 'DoD' in df.columns:
                    agg_columns['DoD'] = 'mean'
                if 'WoW' in df.columns:
                    agg_columns['WoW'] = 'mean'
                if 'MTD' in df.columns:
                    agg_columns['MTD'] = 'mean'
                if 'QTD' in df.columns:
                    agg_columns['QTD'] = 'mean'
                if 'YTD' in df.columns:
                    agg_columns['YTD'] = 'mean'
                if '1YR' in df.columns:
                    agg_columns['1YR'] = 'mean'
                if 'DoD Chg Bid Size' in df.columns:
                    agg_columns['DoD Chg Bid Size'] = 'mean'
                if 'DoD Chg Ask Size' in df.columns:
                    agg_columns['DoD Chg Ask Size'] = 'mean'
                if 'MTD Chg Bid Size' in df.columns:
                    agg_columns['MTD Chg Bid Size'] = 'mean'
                if 'MTD Chg Ask Size' in df.columns:
                    agg_columns['MTD Chg Ask Size'] = 'mean'
                if 'Best Bid' in df.columns:
                    agg_columns['Best Bid'] = 'mean'
                if 'Best Offer' in df.columns:
                    agg_columns['Best Offer'] = 'mean'
                if 'Bid/Offer' in df.columns:
                    agg_columns['Bid/Offer'] = 'mean'
                if 'Dealer @ Best Bid' in df.columns:
                    agg_columns['Dealer @ Best Bid'] = 'first'
                if 'Dealer @ Best Offer' in df.columns:
                    agg_columns['Dealer @ Best Offer'] = 'first'
                if 'Size @ Best Bid' in df.columns:
                    agg_columns['Size @ Best Bid'] = 'sum'
                if 'Size @ Best Offer' in df.columns:
                    agg_columns['Size @ Best Offer'] = 'sum'
                if 'G Spread' in df.columns:
                    agg_columns['G Spread'] = 'mean'
                if 'Keyword' in df.columns:
                    agg_columns['Keyword'] = 'first'
                
                agg_df = df.groupby(['cusip_standardized', 'cusip_original', 'Security']).agg(agg_columns).reset_index()
                
                # Add metadata columns
                agg_df['source_file'] = run_monitor_file
                agg_df['loaded_timestamp'] = datetime.now()
                
                self._log_pipeline_event("Run monitor data aggregation completed", {
                    'original_records': len(df),
                    'aggregated_records': len(agg_df),
                    'unique_cusips': agg_df['cusip_standardized'].nunique()
                })
                
                # Convert all datetime columns to string for SQLite compatibility
                for col in agg_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(agg_df[col]):
                        agg_df[col] = agg_df[col].astype(str)
                
                # Run monitor is always full refresh (no date dimension)
                self._log_pipeline_event("Performing full refresh of run monitor data")
                
                # Execute database operations
                conn = self.db_connection.connect()
                cursor = conn.cursor()
                
                # Clear existing data
                cursor.execute("DELETE FROM run_monitor")
                
                # Insert new data in batches
                print(f"DEBUG: Starting batch insert, agg_df shape: {agg_df.shape}")
                print(f"DEBUG: DataFrame columns: {list(agg_df.columns)}")
                batch_size = 1000
                total_batches = (len(agg_df) + batch_size - 1) // batch_size
                
                for i in range(0, len(agg_df), batch_size):
                    batch = agg_df.iloc[i:i+batch_size]
                    batch_records = batch.to_dict('records')
                    print(f"DEBUG: Batch {i//batch_size + 1}, records: {len(batch_records)}")
                    print(f"DEBUG: First record keys: {list(batch_records[0].keys()) if batch_records else 'No records'}")
                    
                    # Transform column names to match SQL parameters (replace spaces with underscores)
                    for record in batch_records:
                        # Create new record with transformed keys
                        transformed_record = {}
                        for key, value in record.items():
                            # Transform column names to match SQL parameters
                            if key == 'Bid Spread':
                                transformed_record['Bid_Spread'] = value
                            elif key == 'Ask Spread':
                                transformed_record['Ask_Spread'] = value
                            elif key == 'Bid Size':
                                transformed_record['Bid_Size'] = value
                            elif key == 'Ask Size':
                                transformed_record['Ask_Size'] = value
                            elif key == 'DoD Chg Bid Size':
                                transformed_record['DoD_Chg_Bid_Size'] = value
                            elif key == 'DoD Chg Ask Size':
                                transformed_record['DoD_Chg_Ask_Size'] = value
                            elif key == 'MTD Chg Bid Size':
                                transformed_record['MTD_Chg_Bid_Size'] = value
                            elif key == 'MTD Chg Ask Size':
                                transformed_record['MTD_Chg_Ask_Size'] = value
                            elif key == 'Best Bid':
                                transformed_record['Best_Bid'] = value
                            elif key == 'Best Offer':
                                transformed_record['Best_Offer'] = value
                            elif key == 'Bid/Offer':
                                transformed_record['Bid_Offer'] = value
                            elif key == 'Dealer @ Best Bid':
                                transformed_record['Dealer_at_Best_Bid'] = value
                            elif key == 'Dealer @ Best Offer':
                                transformed_record['Dealer_at_Best_Offer'] = value
                            elif key == 'Size @ Best Bid':
                                transformed_record['Size_at_Best_Bid'] = value
                            elif key == 'Size @ Best Offer':
                                transformed_record['Size_at_Best_Offer'] = value
                            elif key == 'G Spread':
                                transformed_record['G_Spread'] = value
                            else:
                                transformed_record[key] = value
                        # Replace original record with transformed one
                        record.clear()
                        record.update(transformed_record)
                    
                    try:
                        # Insert batch with column mapping
                        cursor.executemany("""
                            INSERT INTO run_monitor (
                                "CUSIP", cusip_standardized, "Security", "Bid Spread", "Ask Spread",
                                "Bid Size", "Ask Size", "DoD", "WoW", "MTD", "QTD", "YTD", "1YR",
                                "DoD Chg Bid Size", "DoD Chg Ask Size", "MTD Chg Bid Size", "MTD Chg Ask Size",
                                "Best Bid", "Best Offer", "Bid/Offer", "Dealer @ Best Bid", "Dealer @ Best Offer",
                                "Size @ Best Bid", "Size @ Best Offer", "G Spread", "Keyword",
                                universe_match_status, universe_match_date, source_file, loaded_timestamp
                            ) VALUES (
                                :cusip_original, :cusip_standardized, :Security, :Bid_Spread, :Ask_Spread,
                                :Bid_Size, :Ask_Size, :DoD, :WoW, :MTD, :QTD, :YTD, :1YR,
                                :DoD_Chg_Bid_Size, :DoD_Chg_Ask_Size, :MTD_Chg_Bid_Size, :MTD_Chg_Ask_Size,
                                :Best_Bid, :Best_Offer, :Bid_Offer, :Dealer_at_Best_Bid, :Dealer_at_Best_Offer,
                                :Size_at_Best_Bid, :Size_at_Best_Offer, :G_Spread, :Keyword,
                                'matched', NULL, :source_file, :loaded_timestamp
                            )
                        """, batch_records)
                        print(f"DEBUG: Batch insert successful")
                    except Exception as e:
                        print(f"DEBUG: Batch insert failed with error: {e}")
                        raise
                    
                    batch_num = (i // batch_size) + 1
                    self._log_pipeline_event(f"Inserted batch {batch_num}/{total_batches}", {
                        'batch_size': len(batch),
                        'total_inserted': i + len(batch)
                    })
                
                conn.commit()
                
                # Update pipeline statistics
                self.pipeline_stats['total_records_processed'] += len(agg_df)
                self.pipeline_stats['cusips_matched'] += len(agg_df) - unmatched_count
                self.pipeline_stats['cusips_unmatched'] += unmatched_count
                self.pipeline_stats['tables_updated'].append('run_monitor')
                
                self._log_pipeline_event("Run monitor data loading completed successfully", {
                    'records_processed': len(agg_df),
                    'cusips_matched': len(agg_df) - unmatched_count,
                    'cusips_unmatched': unmatched_count,
                    'update_strategy': 'full_refresh'
                })
                
                return True
                
        except Exception as e:
            print(f"DEBUG: Exception caught in load_run_monitor_data: {e}")
            import traceback
            traceback.print_exc()
            self._log_pipeline_error("Failed to load run monitor data", e, {'file': run_monitor_file})
            return False

    def load_gspread_analytics_data(self, gspread_file: str, force_full_refresh: bool = False) -> bool:
        """
        Load G-spread analytics data with full refresh logic.
        
        Args:
            gspread_file: Path to G-spread analytics parquet file
            force_full_refresh: Whether to force full refresh (always true for G-spread analytics)
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            with self.logger.operation_context("load_gspread_analytics_data", {'file': gspread_file}):
                
                # Read G-spread analytics data - handle both CSV and parquet files
                if gspread_file.lower().endswith('.csv'):
                    # Use pandas to read CSV
                    df = pd.read_csv(gspread_file)
                    result = type('ProcessingResult', (), {'success': True, 'data': df, 'error': None})()
                else:
                    # Use parquet processor for parquet files
                    parquet_processor = ParquetProcessor(config={}, logger=self.logger.db_logger)
                    result = parquet_processor.load_from_parquet(gspread_file)
                
                if not result.success:
                    raise Exception(f"Failed to load G-spread analytics data: {result.error}")
                
                df = result.data
                self._log_pipeline_event("G-spread analytics data loaded successfully", {
                    'file': gspread_file,
                    'records': len(df),
                    'columns': list(df.columns)
                })
                
                # Standardize CUSIPs for the single CUSIP column
                df['cusip_original'] = df['CUSIP'].copy()
                
                def safe_standardize_cusip(cusip):
                    if pd.isna(cusip):
                        return None
                    try:
                        result = self.cusip_standardizer.standardize_cusip(cusip)
                        if isinstance(result, dict):
                            standardized = result.get('cusip_standardized')
                            if standardized and standardized.strip():  # Check if we got a valid result
                                return standardized
                            else:
                                # If standardization failed, return original CUSIP as fallback
                                return cusip
                        else:
                            return result if result else cusip  # Fallback to original if None
                    except Exception as e:
                        # Log the error but don't fail the pipeline
                        if not self.disable_logging:
                            print(f"Warning: CUSIP standardization failed for {cusip}: {e}")
                        return cusip  # Return original CUSIP as fallback
                
                # Use parallel processing if enabled
                if self.parallel and len(df) > 1000:  # Only parallelize for large datasets
                    self._log_pipeline_event("Using parallel CUSIP standardization", {
                        'total_records': len(df),
                        'workers': min(mp.cpu_count(), 8)  # Limit to 8 workers
                    })
                    
                    # Process CUSIP in parallel
                    with ThreadPoolExecutor(max_workers=min(mp.cpu_count(), 8)) as executor:
                        cusip_results = list(executor.map(safe_standardize_cusip, df['CUSIP']))
                    df['cusip_standardized'] = cusip_results
                    
                    # Garbage collection if low memory mode
                    if self.low_memory:
                        gc.collect()
                else:
                    # Sequential processing
                    df['cusip_standardized'] = df['CUSIP'].apply(safe_standardize_cusip)
                
                # Handle unmatched CUSIPs
                unmatched_mask = df['cusip_standardized'].isna()
                unmatched_count = unmatched_mask.sum()
                
                if unmatched_count > 0:
                    self._log_pipeline_event(f"Found {unmatched_count} unmatched CUSIPs in G-spread analytics data")
                    
                    # Collect unmatched CUSIPs
                    unmatched_data = df[unmatched_mask][['cusip_original', 'Security']].copy()
                    unmatched_data.columns = ['cusip_original', 'security_name']
                    unmatched_list = unmatched_data.to_dict('records')
                    
                    self._insert_unmatched_cusips('gspread_analytics', unmatched_list, gspread_file)
                    
                    # Remove records where CUSIP is unmatched
                    df = df[~unmatched_mask]
                
                # Prepare data for database insertion
                df['source_file'] = gspread_file
                df['loaded_timestamp'] = datetime.now()
                
                # Add ownership flags (default to 0, will be updated based on portfolio data)
                # df['own_1'] = 0  # Removed - ownership columns not used
                # df['own_2'] = 0  # Removed - ownership columns not used
                
                # G-spread analytics is always full refresh (no date dimension)
                self._log_pipeline_event("Performing full refresh of G-spread analytics data")
                
                # Execute database operations
                conn = self.db_connection.connect()
                cursor = conn.cursor()
                
                # Clear existing data
                cursor.execute("DELETE FROM gspread_analytics")
                
                # Garbage collection if low memory mode
                if self.low_memory:
                    gc.collect()
                
                # Insert new data in batches with optimization
                total_batches = (len(df) + self.batch_size - 1) // self.batch_size
                
                for i in range(0, len(df), self.batch_size):
                    batch = df.iloc[i:i+self.batch_size]
                    batch_records = batch.to_dict('records')
                    
                    # Convert all datetime columns to string for SQLite compatibility
                    for record in batch_records:
                        for col, value in record.items():
                            if isinstance(value, (pd.Timestamp, datetime, datetime_time)):
                                record[col] = str(value)
                    
                    # Insert batch with column mapping
                    cursor.executemany("""
                        INSERT INTO gspread_analytics (
                            "CUSIP", cusip_standardized, "Security", "GSpread", "DATE",
                            universe_match_status, universe_match_date, source_file, loaded_timestamp
                        ) VALUES (
                            :cusip_original, :cusip_standardized, :Security, :GSpread, :DATE,
                            'matched', NULL, :source_file, :loaded_timestamp
                        )
                    """, batch_records)
                    
                    batch_num = (i // self.batch_size) + 1
                    self._log_pipeline_event(f"Inserted batch {batch_num}/{total_batches}", {
                        'batch_size': len(batch),
                        'total_inserted': i + len(batch)
                    })
                    
                    # Garbage collection if low memory mode
                    if self.low_memory and batch_num % 10 == 0:  # Every 10 batches
                        gc.collect()
                
                conn.commit()
                
                # Update pipeline statistics
                self.pipeline_stats['total_records_processed'] += len(df)
                self.pipeline_stats['cusips_matched'] += len(df) - unmatched_count  # 1 CUSIP per record
                self.pipeline_stats['cusips_unmatched'] += unmatched_count
                self.pipeline_stats['tables_updated'].append('gspread_analytics')
                
                self._log_pipeline_event("G-spread analytics data loading completed successfully", {
                    'records_processed': len(df),
                    'cusips_matched': len(df) - unmatched_count,
                    'cusips_unmatched': unmatched_count,
                    'update_strategy': 'full_refresh'
                })
                
                return True
                
        except Exception as e:
            self._log_pipeline_error("Failed to load G-spread analytics data", e, {'file': gspread_file})
            return False
    
    def optimize_database(self) -> bool:
        """
        Optimize database performance after data loading.
        
        Returns:
            True if optimization successful, False otherwise
        """
        try:
            with self.logger.operation_context("database_optimization"):
                self._log_pipeline_event("Starting database optimization")
                
                conn = self.db_connection.connect()
                cursor = conn.cursor()
                
                # VACUUM to reclaim space and optimize storage
                self._log_pipeline_event("Running VACUUM operation")
                cursor.execute("VACUUM")
                
                # ANALYZE to update statistics for query optimization
                self._log_pipeline_event("Running ANALYZE operation")
                cursor.execute("ANALYZE")
                
                # Update database statistics
                cursor.execute("PRAGMA optimize")
                
                conn.commit()
                
                # Check database size after optimization
                health_results = self.db_connection.check_health()
                
                self._log_pipeline_event("Database optimization completed successfully", {
                    'database_size_mb': health_results['database_size_mb'],
                    'optimization_operations': ['VACUUM', 'ANALYZE', 'PRAGMA optimize']
                })
                
                return True
                
        except Exception as e:
            self._log_pipeline_error("Database optimization failed", e)
            return False
    
    def run_full_pipeline(self, data_sources: Dict[str, str], force_full_refresh: bool = False) -> bool:
        """
        Run complete pipeline for all data sources.
        
        Args:
            data_sources: Dictionary mapping table names to file paths
            force_full_refresh: Whether to force full refresh for all tables
            
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        try:
            self.pipeline_stats['start_time'] = datetime.now()
            
            with self.logger.operation_context("full_pipeline_execution"):
                
                self._log_pipeline_event("Starting full pipeline execution", {
                    'data_sources': data_sources,
                    'force_full_refresh': force_full_refresh
                })
                
                # Initialize database if needed
                if not self.initialize_database():
                    raise Exception("Database initialization failed")
                
                # Load data in dependency order (universe first, then others)
                success_count = 0
                total_sources = len(data_sources)
                
                # 1. Load universe data first (required for CUSIP validation)
                if 'universe' in data_sources:
                    if self.load_universe_data(data_sources['universe'], force_full_refresh):
                        success_count += 1
                    else:
                        raise Exception("Universe data loading failed - cannot continue")
                
                # 2. Load portfolio data (requires universe for CUSIP validation)
                if 'portfolio' in data_sources:
                    if self.load_portfolio_data(data_sources['portfolio'], force_full_refresh):
                        success_count += 1
                
                # 3. Load combined runs data (requires universe for CUSIP validation)
                if 'runs' in data_sources:
                    if self.load_combined_runs_data(data_sources['runs'], force_full_refresh):
                        success_count += 1
                
                # 4. Load run monitor data (requires universe for CUSIP validation)
                if 'run_monitor' in data_sources:
                    if self.load_run_monitor_data(data_sources['run_monitor'], force_full_refresh):
                        success_count += 1
                
                # 5. Load G-spread analytics data (requires universe for CUSIP validation)
                if 'gspread_analytics' in data_sources:
                    if self.load_gspread_analytics_data(data_sources['gspread_analytics'], force_full_refresh):
                        success_count += 1
                
                # Generate final statistics and health check
                final_stats = self._generate_pipeline_summary()
                
                self.pipeline_stats['end_time'] = datetime.now()
                duration = (self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']).total_seconds()
                
                # Run database optimization if enabled
                if self.optimize_db and success_count == total_sources:
                    self._log_pipeline_event("Starting post-pipeline database optimization")
                    optimization_success = self.optimize_database()
                    if optimization_success:
                        self._log_pipeline_event("Database optimization completed successfully")
                    else:
                        self._log_pipeline_event("Database optimization failed, but pipeline completed")
                
                self._log_pipeline_event("Full pipeline execution completed", {
                    'duration_seconds': duration,
                    'sources_processed': success_count,
                    'total_sources': total_sources,
                    'success_rate': (success_count / total_sources) * 100,
                    'final_statistics': final_stats,
                    'optimization_enabled': self.optimize_db
                })
                
                return success_count == total_sources
                
        except Exception as e:
            self.pipeline_stats['errors_encountered'] += 1
            self.pipeline_stats['end_time'] = datetime.now()
            self._log_pipeline_error("Full pipeline execution failed", e)
            return False
    
    def create_backup(self, backup_path: Optional[str] = None) -> bool:
        """
        Create database backup with timestamp.
        
        Args:
            backup_path: Custom backup path, or None for auto-generated path
            
        Returns:
            True if backup successful, False otherwise
        """
        try:
            if backup_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"backups/trading_analytics_backup_{timestamp}.db"
            
            success = self.db_connection.backup_database(backup_path)
            
            if success:
                self._log_pipeline_event("Database backup created successfully", {
                    'backup_path': backup_path,
                    'original_size_mb': self.db_connection._get_database_file_size()
                })
            
            return success
            
        except Exception as e:
            self._log_pipeline_error("Database backup failed", e)
            return False
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get comprehensive pipeline status and statistics"""
        
        # Get database health
        health_results = self.db_connection.check_health()
        
        # Get connection statistics
        conn_stats = self.db_connection.get_connection_statistics()
        
        # Get CUSIP standardization statistics
        cusip_stats = self.cusip_standardizer.get_standardization_statistics()
        
        # Get table row counts
        table_counts = self._get_table_row_counts()
        
        # Get unmatched CUSIP summary
        unmatched_summary = self._get_unmatched_cusip_summary()
        
        return {
            'pipeline_statistics': self.pipeline_stats,
            'database_health': health_results,
            'connection_statistics': conn_stats,
            'cusip_statistics': cusip_stats,
            'table_row_counts': table_counts,
            'unmatched_cusip_summary': unmatched_summary,
            'last_updated': datetime.now().isoformat()
        }
    
    # ============================================
    # HELPER METHODS
    # ============================================
    
    def _decide_update_strategy(self, table_name: str, source_file: str, 
                               new_data_df, force_full_refresh: bool) -> Dict[str, Any]:
        """Decide between incremental update vs full refresh"""
        
        decision = {
            'update_type': 'full_refresh',
            'reason': 'default_full_refresh',
            'file_date': None,
            'file_size_mb': 0,
            'existing_dates': [],
            'new_dates': [],
            'overlapping_dates': []
        }
        
        try:
            # Get file info
            file_path = Path(source_file)
            if file_path.exists():
                decision['file_size_mb'] = file_path.stat().st_size / 1024 / 1024
            
            # Check if table exists and has data
            table_count = self.db_connection.execute_query(
                f"SELECT COUNT(*) FROM {table_name}"
            )
            
            if table_count and table_count[0][0] == 0:
                decision['reason'] = 'empty_table_requires_full_refresh'
            elif force_full_refresh:
                decision['reason'] = 'forced_full_refresh'
            else:
                # Check for date-based incremental logic
                if 'date' in new_data_df.columns:
                    new_dates = sorted(new_data_df['date'].unique())
                    decision['new_dates'] = [str(d) for d in new_dates]
                    
                    # Get existing dates
                    existing_dates_result = self.db_connection.execute_query(
                        f"SELECT DISTINCT date FROM {table_name} ORDER BY date"
                    )
                    existing_dates = [row[0] for row in existing_dates_result] if existing_dates_result else []
                    decision['existing_dates'] = [str(d) for d in existing_dates]
                    
                    # Check for overlap
                    overlapping = set(existing_dates) & set(new_data_df['date'].unique())
                    decision['overlapping_dates'] = [str(d) for d in overlapping]
                    
                    # Decide strategy
                    if not existing_dates:
                        decision['reason'] = 'no_existing_data'
                    elif len(overlapping) == 0:
                        decision['update_type'] = 'incremental'
                        decision['reason'] = 'no_date_overlap_detected'
                    elif len(overlapping) < len(new_dates) * 0.5:  # Less than 50% overlap
                        decision['update_type'] = 'incremental'
                        decision['reason'] = 'minimal_date_overlap'
                    else:
                        decision['reason'] = 'significant_date_overlap_requires_full_refresh'
                
            # Log decision
            self.logger.log_incremental_decision(table_name, source_file, decision)
            
        except Exception as e:
            self._log_pipeline_error("Error in update strategy decision", e)
            decision['reason'] = 'error_defaulting_to_full_refresh'
        
        return decision
    
    def _get_current_universe_cusips(self) -> set:
        """Get set of current universe CUSIPs for validation"""
        try:
            results = self.db_connection.execute_query(
                "SELECT DISTINCT cusip_standardized FROM current_universe"
            )
            return {row[0] for row in results} if results else set()
        except Exception:
            return set()
    
    def _insert_unmatched_cusips(self, source_table: str, unmatched_list: List[Dict], source_file: str):
        """Insert unmatched CUSIPs into tracking tables"""
        try:
            current_date = datetime.now().date()
            
            # Insert into all dates table
            all_dates_records = []
            last_date_records = []
            
            for unmatched in unmatched_list:
                # All dates record
                all_dates_record = (
                    source_table,
                    unmatched.get('date'),
                    unmatched['cusip_original'],
                    unmatched['cusip_standardized'],
                    unmatched.get('security_name', ''),
                    current_date,
                    source_file
                )
                all_dates_records.append(all_dates_record)
                
                # Last date record
                last_date_record = (
                    source_table,
                    unmatched['cusip_original'],
                    unmatched['cusip_standardized'],
                    unmatched.get('security_name', ''),
                    current_date,
                    source_file
                )
                last_date_records.append(last_date_record)
            
            # Insert batch into all dates table
            if all_dates_records:
                self.db_connection.execute_many("""
                    INSERT INTO unmatched_cusips_all_dates 
                    (source_table, date, cusip_original, cusip_standardized, 
                     security_name, universe_match_attempted_date, source_file)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, all_dates_records)
            
            # Insert batch into last date table (with REPLACE for updates)
            if last_date_records:
                self.db_connection.execute_many("""
                    INSERT OR REPLACE INTO unmatched_cusips_last_date 
                    (source_table, cusip_original, cusip_standardized, 
                     security_name, universe_match_attempted_date, source_file)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, last_date_records)
            
        except Exception as e:
            self._log_pipeline_error("Failed to insert unmatched CUSIPs", e)
    
    def _generate_pipeline_summary(self) -> Dict[str, Any]:
        """Generate comprehensive pipeline execution summary"""
        try:
            # Get CUSIP match summary from view
            match_summary = self.db_connection.execute_query(
                "SELECT * FROM cusip_match_summary"
            )
            
            # Get data quality summary
            quality_summary = self.db_connection.execute_query(
                "SELECT * FROM data_quality_dashboard"
            )
            
            return {
                'cusip_match_summary': [dict(zip(['table_name', 'total_cusips', 'matched_cusips', 'unmatched_cusips', 'match_rate_percent', 'reference_date'], row)) for row in match_summary] if match_summary else [],
                'data_quality_summary': [dict(zip(['table_name', 'total_checks', 'checks_passed', 'checks_failed', 'checks_warning', 'pass_rate_percent', 'last_check_time'], row)) for row in quality_summary] if quality_summary else [],
                'pipeline_performance': self.pipeline_stats
            }
            
        except Exception as e:
            self._log_pipeline_error("Error generating pipeline summary", e)
            return {'error': str(e)}
    
    def _get_table_row_counts(self) -> Dict[str, int]:
        """Get row counts for all main tables"""
        tables = ['universe_historical', 'portfolio_historical', 'combined_runs_historical', 
                 'run_monitor', 'gspread_analytics', 
                 'unmatched_cusips_all_dates', 'unmatched_cusips_last_date']
        
        counts = {}
        for table in tables:
            try:
                result = self.db_connection.execute_query(f"SELECT COUNT(*) FROM {table}")
                counts[table] = result[0][0] if result else 0
            except Exception:
                counts[table] = 0
        
        return counts
    
    def _get_unmatched_cusip_summary(self) -> Dict[str, Any]:
        """Get summary of unmatched CUSIPs"""
        try:
            # Get counts by source table
            results = self.db_connection.execute_query("""
                SELECT source_table, COUNT(*) as unmatched_count
                FROM unmatched_cusips_last_date
                GROUP BY source_table
                ORDER BY unmatched_count DESC
            """)
            
            by_table = {row[0]: row[1] for row in results} if results else {}
            
            # Get most recent unmatched examples
            examples = self.db_connection.execute_query("""
                SELECT source_table, cusip_original, cusip_standardized, security_name
                FROM unmatched_cusips_last_date
                ORDER BY loaded_timestamp DESC
                LIMIT 10
            """)
            
            return {
                'unmatched_by_table': by_table,
                'total_unmatched_last_date': sum(by_table.values()),
                'recent_examples': [
                    {
                        'source_table': row[0],
                        'cusip_original': row[1],
                        'cusip_standardized': row[2],
                        'security_name': row[3]
                    }
                    for row in examples
                ] if examples else []
            }
        except Exception as e:
            self._log_pipeline_error("Error getting unmatched CUSIP summary", e)
            return {
                'unmatched_by_table': {},
                'total_unmatched_last_date': 0,
                'recent_examples': [],
                'error': str(e)
            }
    
    def _log_pipeline_event(self, message: str, details: Dict[str, Any] = None):
        """Log pipeline event"""
        self.logger.log_pipeline_step("database_pipeline", {
            'message': message,
            'details': details or {}
        })
    
    def _log_pipeline_error(self, message: str, error: Exception, context: Dict[str, Any] = None):
        """Log pipeline error"""
        self.logger.log_database_error(error, context={
            'operation': 'database_pipeline',
            'message': message,
            **(context or {})
        })


def main():
    """Main entry point for database pipeline execution"""
    parser = argparse.ArgumentParser(description='Trading Analytics Database Pipeline')
    parser.add_argument('--database', '-d', default='trading_analytics.db', 
                       help='Path to SQLite database file')
    parser.add_argument('--config', '-c', default='config/config.yaml',
                       help='Path to configuration file')
    parser.add_argument('--init', action='store_true',
                       help='Initialize database schema only')
    parser.add_argument('--force-refresh', action='store_true',
                       help='Force full refresh instead of incremental updates')
    parser.add_argument('--backup', action='store_true',
                       help='Create database backup')
    parser.add_argument('--status', action='store_true',
                       help='Show pipeline status and statistics')
    parser.add_argument('--universe', type=str,
                       help='Path to universe parquet file')
    parser.add_argument('--portfolio', type=str,
                       help='Path to portfolio parquet file')
    parser.add_argument('--runs', type=str,
                       help='Path to combined runs parquet file')
    parser.add_argument('--run-monitor', type=str,
                       help='Path to run monitor parquet file')
    parser.add_argument('--gspread-analytics', type=str,
                       help='Path to G-spread analytics parquet file')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Batch size for database operations (default: 1000)')
    parser.add_argument('--parallel', action='store_true',
                       help='Enable parallel processing for CUSIP standardization')
    parser.add_argument('--low-memory', action='store_true',
                       help='Enable low memory mode with garbage collection')
    parser.add_argument('--optimize-db', action='store_true',
                       help='Optimize database after loading (VACUUM, ANALYZE)')
    parser.add_argument('--disable-logging', action='store_true',
                       help='Disable detailed logging for faster execution')
    
    args = parser.parse_args()
    
    # Initialize pipeline with optimization options
    pipeline = DatabasePipeline(
        database_path=args.database, 
        config_path=args.config,
        batch_size=args.batch_size,
        parallel=args.parallel,
        low_memory=args.low_memory,
        optimize_db=args.optimize_db,
        disable_logging=args.disable_logging
    )
    
    # Handle different operations
    if args.init:
        print("🔧 Initializing database...")
        success = pipeline.initialize_database(force_recreate=True)
        print("✅ Database initialized successfully!" if success else "❌ Database initialization failed!")
        return 0 if success else 1
    
    elif args.backup:
        print("💾 Creating database backup...")
        success = pipeline.create_backup()
        print("✅ Backup created successfully!" if success else "❌ Backup creation failed!")
        return 0 if success else 1
    
    elif args.status:
        print("📊 Getting pipeline status...")
        try:
            status = pipeline.get_pipeline_status()
        except Exception as e:
            print(f"⚠️  Error getting pipeline status: {e}")
            print("Continuing with basic status information...")
            return 1
        
        print("\n" + "="*80)
        print("📊 DATABASE PIPELINE STATUS REPORT")
        print("="*80)
        
        # Database Health Summary
        db_health = status['database_health']
        print(f"\n💾 DATABASE HEALTH:")
        print(f"   🟢 Status: {'Healthy' if db_health['connection_healthy'] else 'Unhealthy'}")
        print(f"   📦 Size: {db_health['database_size_mb']:.2f} MB")
        print(f"   🔗 Uptime: {db_health['connection_uptime_minutes']:.1f} minutes")
        
        # Connection Statistics
        conn_stats = status['connection_statistics']
        print(f"\n🔗 CONNECTION STATISTICS:")
        print(f"   📊 Total Queries: {conn_stats.get('total_queries', 0):,}")
        print(f"   ⏱️  Average Query Time: {conn_stats.get('avg_query_time_ms', 0):.2f} ms")
        print(f"   🔄 Active Connections: {conn_stats.get('active_connections', 0)}")
        
        # Table Row Counts with Analysis
        print(f"\n📋 TABLE STATISTICS:")
        table_counts = status['table_row_counts']
        total_records = sum(table_counts.values())
        
        # Core historical tables
        core_tables = ['universe_historical', 'portfolio_historical', 'combined_runs_historical']
        print(f"   📊 CORE HISTORICAL TABLES:")
        for table in core_tables:
            if table in table_counts:
                count = table_counts[table]
                percentage = (count / total_records * 100) if total_records > 0 else 0
                print(f"      • {table}: {count:,} rows ({percentage:.1f}%)")
        
        # Analytics tables
        analytics_tables = ['run_monitor', 'gspread_analytics']
        print(f"   📈 ANALYTICS TABLES:")
        for table in analytics_tables:
            if table in table_counts:
                count = table_counts[table]
                print(f"      • {table}: {count:,} rows")
        
        # Tracking tables
        tracking_tables = ['unmatched_cusips_all_dates', 'unmatched_cusips_last_date']
        print(f"   🔍 TRACKING TABLES:")
        for table in tracking_tables:
            if table in table_counts:
                count = table_counts[table]
                print(f"      • {table}: {count:,} rows")
        
        # CUSIP Analysis
        print(f"\n🎯 CUSIP ANALYSIS:")
        unmatched_summary = status['unmatched_cusip_summary']
        total_unmatched = unmatched_summary['total_unmatched_last_date']
        
        if total_unmatched == 0:
            print(f"   🟢 No unmatched CUSIPs detected")
        else:
            print(f"   🔴 {total_unmatched:,} unmatched CUSIPs in last load")
            
            # Show breakdown by table
            for table, count in unmatched_summary['unmatched_by_table'].items():
                print(f"      • {table}: {count:,} unmatched")
            
            # Show recent examples
            if unmatched_summary.get('recent_examples'):
                print(f"   📝 Recent unmatched examples:")
                for example in unmatched_summary['recent_examples'][:3]:  # Show first 3
                    print(f"      - {example['cusip_original']} ({example['security_name'][:30]}...)")
        
        # Last Universe Date Coverage Analysis
        print(f"\n🌍 LAST UNIVERSE DATE COVERAGE:")
        try:
            # Get the last universe date
            last_date_result = pipeline.db_connection.execute_query("SELECT MAX(date) FROM universe_historical")
            if last_date_result and last_date_result[0] and last_date_result[0][0]:
                last_universe_date = last_date_result[0][0]
                print(f"   📅 Last universe date: {last_universe_date}")
                
                # Get total CUSIPs on last universe date
                total_last_date_result = pipeline.db_connection.execute_query(
                    "SELECT COUNT(DISTINCT cusip_standardized) FROM universe_historical WHERE date = ?", 
                    (last_universe_date,)
                )
                total_last_date_cusips = total_last_date_result[0][0] if total_last_date_result else 0
                print(f"   🌍 Total CUSIPs on last date: {total_last_date_cusips:,}")
                
                # Get orphaned CUSIPs (in other tables but NOT in universe) by table
                orphaned_by_table = {}
                
                # Check portfolio for orphaned CUSIPs
                portfolio_orphaned_result = pipeline.db_connection.execute_query("""
                    SELECT COUNT(DISTINCT p.cusip_standardized) 
                    FROM portfolio_historical p 
                    LEFT JOIN universe_historical u ON p.cusip_standardized = u.cusip_standardized 
                        AND p.date = u.date
                    WHERE p.date = ? AND u.cusip_standardized IS NULL
                """, (last_universe_date,))
                portfolio_orphaned = portfolio_orphaned_result[0][0] if portfolio_orphaned_result else 0
                if portfolio_orphaned > 0:
                    orphaned_by_table['portfolio_historical'] = portfolio_orphaned
                
                # Check combined runs for orphaned CUSIPs
                runs_orphaned_result = pipeline.db_connection.execute_query("""
                    SELECT COUNT(DISTINCT r.cusip_standardized) 
                    FROM combined_runs_historical r 
                    LEFT JOIN universe_historical u ON r.cusip_standardized = u.cusip_standardized 
                        AND r.date = u.date
                    WHERE r.date = ? AND u.cusip_standardized IS NULL
                """, (last_universe_date,))
                runs_orphaned = runs_orphaned_result[0][0] if runs_orphaned_result else 0
                if runs_orphaned > 0:
                    orphaned_by_table['combined_runs_historical'] = runs_orphaned
                
                # Check run monitor for orphaned CUSIPs (current table, no date)
                monitor_orphaned_result = pipeline.db_connection.execute_query("""
                    SELECT COUNT(DISTINCT m.cusip_standardized) 
                    FROM run_monitor m 
                    LEFT JOIN universe_historical u ON m.cusip_standardized = u.cusip_standardized
                    WHERE u.cusip_standardized IS NULL
                """)
                monitor_orphaned = monitor_orphaned_result[0][0] if monitor_orphaned_result else 0
                if monitor_orphaned > 0:
                    orphaned_by_table['run_monitor'] = monitor_orphaned
                
                # Check gspread analytics for orphaned CUSIPs (current table, no date)
                gspread_orphaned_result = pipeline.db_connection.execute_query("""
                    SELECT COUNT(DISTINCT g.cusip_1_standardized) + COUNT(DISTINCT g.cusip_2_standardized) 
                    FROM gspread_analytics g 
                    LEFT JOIN universe_historical u1 ON g.cusip_1_standardized = u1.cusip_standardized
                    LEFT JOIN universe_historical u2 ON g.cusip_2_standardized = u2.cusip_standardized
                    WHERE u1.cusip_standardized IS NULL AND u2.cusip_standardized IS NULL
                """)
                gspread_orphaned = gspread_orphaned_result[0][0] if gspread_orphaned_result else 0
                if gspread_orphaned > 0:
                    orphaned_by_table['gspread_analytics'] = gspread_orphaned
                
                # Calculate total orphaned CUSIPs
                total_orphaned = sum(orphaned_by_table.values())
                
                if total_orphaned > 0:
                    print(f"   ⚠️  Orphaned CUSIPs (in other tables but NOT in universe): {total_orphaned:,}")
                    
                    # Show breakdown by table
                    print(f"   📊 Orphaned by table:")
                    for table, count in orphaned_by_table.items():
                        print(f"      • {table}: {count:,} orphaned")
                    
                    # Show all orphaned CUSIPs
                    print(f"   📝 All orphaned CUSIPs (not in universe):")
                    
                    # Get portfolio orphaned details
                    portfolio_orphaned_details_result = pipeline.db_connection.execute_query("""
                        SELECT p.cusip_standardized, p.security 
                        FROM portfolio_historical p 
                        LEFT JOIN universe_historical u ON p.cusip_standardized = u.cusip_standardized 
                            AND p.date = u.date
                        WHERE p.date = ? AND u.cusip_standardized IS NULL
                        ORDER BY p.cusip_standardized
                    """, (last_universe_date,))
                    
                    for row in portfolio_orphaned_details_result:
                        cusip, security = row
                        print(f"      - {cusip} ({security[:50]}...) - Orphaned in portfolio")
                    
                    # Get runs orphaned details
                    runs_orphaned_details_result = pipeline.db_connection.execute_query("""
                        SELECT r.cusip_standardized, r.security 
                        FROM combined_runs_historical r 
                        LEFT JOIN universe_historical u ON r.cusip_standardized = u.cusip_standardized 
                            AND r.date = u.date
                        WHERE r.date = ? AND u.cusip_standardized IS NULL
                        ORDER BY r.cusip_standardized
                    """, (last_universe_date,))
                    
                    for row in runs_orphaned_details_result:
                        cusip, security = row
                        print(f"      - {cusip} ({security[:50]}...) - Orphaned in runs")
                    
                    # Get monitor orphaned details
                    monitor_orphaned_details_result = pipeline.db_connection.execute_query("""
                        SELECT m.cusip_standardized, m.security 
                        FROM run_monitor m 
                        LEFT JOIN universe_historical u ON m.cusip_standardized = u.cusip_standardized
                        WHERE u.cusip_standardized IS NULL
                        ORDER BY m.cusip_standardized
                    """)
                    
                    for row in monitor_orphaned_details_result:
                        cusip, security = row
                        print(f"      - {cusip} ({security[:50]}...) - Orphaned in run monitor")
                    
                    # Get gspread orphaned details
                    gspread_orphaned_details_result = pipeline.db_connection.execute_query("""
                        SELECT g.cusip_1_standardized, g.cusip_2_standardized, g.bond_name_1, g.bond_name_2
                        FROM gspread_analytics g 
                        LEFT JOIN universe_historical u1 ON g.cusip_1_standardized = u1.cusip_standardized
                        LEFT JOIN universe_historical u2 ON g.cusip_2_standardized = u2.cusip_standardized
                        WHERE u1.cusip_standardized IS NULL OR u2.cusip_standardized IS NULL
                        ORDER BY g.cusip_1_standardized, g.cusip_2_standardized
                    """)
                    
                    for row in gspread_orphaned_details_result:
                        cusip1, cusip2, security1, security2 = row
                        if cusip1 and cusip1 not in [None, '']:
                            print(f"      - {cusip1} ({security1[:50]}...) - Orphaned in gspread (CUSIP_1)")
                        if cusip2 and cusip2 not in [None, '']:
                            print(f"      - {cusip2} ({security2[:50]}...) - Orphaned in gspread (CUSIP_2)")
                        
                else:
                    print(f"   🟢 No orphaned CUSIPs found - all CUSIPs in other tables exist in universe")
                    
            else:
                print(f"   ⚠️  Could not determine last universe date")
                
        except Exception as e:
            print(f"   ⚠️  Could not perform last universe date coverage analysis: {e}")
        
        # Data Quality Assessment
        print(f"\n✅ DATA QUALITY ASSESSMENT:")
        
        # Check for data completeness
        universe_count = table_counts.get('universe_historical', 0)
        portfolio_count = table_counts.get('portfolio_historical', 0)
        runs_count = table_counts.get('combined_runs_historical', 0)
        
        if universe_count > 0:
            print(f"   🟢 Universe data: {universe_count:,} records")
        else:
            print(f"   🔴 Universe data: MISSING")
            
        if portfolio_count > 0:
            print(f"   🟢 Portfolio data: {portfolio_count:,} records")
        else:
            print(f"   🔴 Portfolio data: MISSING")
            
        if runs_count > 0:
            print(f"   🟢 Trading runs data: {runs_count:,} records")
        else:
            print(f"   🔴 Trading runs data: MISSING")
        
        # Check for analytics completeness
        monitor_count = table_counts.get('run_monitor', 0)
        gspread_count = table_counts.get('gspread_analytics', 0)
        
        if monitor_count > 0:
            print(f"   🟢 Run monitor analytics: {monitor_count:,} records")
        else:
            print(f"   🔴 Run monitor analytics: MISSING")
            
        if gspread_count > 0:
            print(f"   🟢 G-spread analytics: {gspread_count:,} records")
        else:
            print(f"   🔴 G-spread analytics: MISSING")
        
        # Performance Metrics
        print(f"\n🚀 PERFORMANCE METRICS:")
        pipeline_stats = status['pipeline_statistics']
        if pipeline_stats.get('start_time') and pipeline_stats.get('end_time'):
            start_time = pipeline_stats['start_time']
            end_time = pipeline_stats['end_time']
            
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time)
            if isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)
                
            duration = (end_time - start_time).total_seconds()
            records_processed = pipeline_stats.get('total_records_processed', 0)
            
            print(f"   ⏱️  Last pipeline duration: {duration:.1f} seconds")
            print(f"   📈 Records processed: {records_processed:,}")
            if duration > 0:
                print(f"   ⚡ Processing rate: {records_processed/duration:.0f} records/second")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        
        if total_unmatched > 0:
            print(f"   • Review unmatched CUSIPs for data quality issues")
            print(f"   • Check universe data completeness and accuracy")
        
        if universe_count == 0:
            print(f"   • Load universe data first (required for CUSIP validation)")
        
        if portfolio_count == 0:
            print(f"   • Load portfolio data for position analysis")
        
        if runs_count == 0:
            print(f"   • Load trading runs data for performance analysis")
        
        if monitor_count == 0:
            print(f"   • Generate run monitor analytics for current market view")
        
        if gspread_count == 0:
            print(f"   • Generate G-spread analytics for relative value analysis")
        
        print(f"\n📖 For detailed logs, check: logs/db.log")
        print(f"📊 For database queries, check: logs/db_sql.log")
        print("="*80)
        
        return 0
    
    else:
        # Run data loading pipeline
        data_sources = {}
        
        if args.universe:
            data_sources['universe'] = args.universe
        
        if args.portfolio:
            data_sources['portfolio'] = args.portfolio
        
        if args.runs:
            data_sources['runs'] = args.runs
        
        if args.run_monitor:
            data_sources['run_monitor'] = args.run_monitor
        
        if args.gspread_analytics:
            data_sources['gspread_analytics'] = args.gspread_analytics
        
        if not data_sources:
            print("📁 No data sources specified. Using default file paths for full pipeline run...")
            # Default file paths for full pipeline run
            data_sources = {
                'universe': 'universe/universe.parquet',
                'portfolio': 'portfolio/portfolio.parquet', 
                'runs': 'runs/combined_runs.parquet',
                'run_monitor': 'runs/run_monitor.parquet',
                'gspread_analytics': 'historical g spread/bond_z.parquet'
            }
            print("🔍 Checking for default files...")
            missing_sources = []
            for source, path in data_sources.items():
                if os.path.exists(path):
                    print(f"  ✅ {source}: {path}")
                else:
                    print(f"  ❌ {source}: {path} (not found)")
                    missing_sources.append(source)
            
            # Remove missing files from data_sources
            for source in missing_sources:
                data_sources.pop(source)
            
            if not data_sources:
                print("❌ No default files found. Please specify data sources manually.")
                return 1
        
        print("🚀 Starting database pipeline...")
        success = pipeline.run_full_pipeline(data_sources, args.force_refresh)
        
        if success:
            print("✅ Pipeline completed successfully!")
            
            # Show final status
            status = pipeline.get_pipeline_status()
            stats = status['pipeline_statistics']
            if stats.get('end_time'):
                # Handle both string and datetime objects for timestamps
                end_time = stats['end_time']
                start_time = stats['start_time']
                
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time)
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time)
                    
                duration = (end_time - start_time).total_seconds()
                
                # ============================================
                # COMPREHENSIVE DATA ENGINEERING REPORT
                # ============================================
                print("\n" + "="*80)
                print("📊 DATA ENGINEERING PIPELINE REPORT")
                print("="*80)
                
                # Performance Metrics
                print(f"\n🚀 PERFORMANCE METRICS:")
                print(f"   ⏱️  Total Duration: {duration:.1f} seconds")
                print(f"   📈 Records Processed: {stats['total_records_processed']:,}")
                print(f"   ⚡ Processing Rate: {stats['total_records_processed']/duration:.0f} records/second")
                
                # Database Health
                db_health = status['database_health']
                print(f"\n💾 DATABASE HEALTH:")
                print(f"   🟢 Status: {'Healthy' if db_health['connection_healthy'] else 'Unhealthy'}")
                print(f"   📦 Size: {db_health['database_size_mb']:.2f} MB")
                print(f"   🔗 Uptime: {db_health['connection_uptime_minutes']:.1f} minutes")
                
                # Data Quality Metrics
                print(f"\n🔍 DATA QUALITY METRICS:")
                match_rate = 0  # Initialize match_rate
                if stats['cusips_matched'] + stats['cusips_unmatched'] > 0:
                    match_rate = (stats['cusips_matched'] / (stats['cusips_matched'] + stats['cusips_unmatched'])) * 100
                    print(f"   🎯 CUSIP Match Rate: {match_rate:.1f}%")
                    print(f"   ✅ Matched CUSIPs: {stats['cusips_matched']:,}")
                    print(f"   ❌ Unmatched CUSIPs: {stats['cusips_unmatched']:,}")
                    
                    # Match rate assessment
                    if match_rate >= 98:
                        print(f"   🟢 EXCELLENT: CUSIP matching is very healthy")
                    elif match_rate >= 95:
                        print(f"   🟡 GOOD: CUSIP matching is acceptable")
                    elif match_rate >= 90:
                        print(f"   🟠 WARNING: CUSIP matching needs attention")
                    else:
                        print(f"   🔴 CRITICAL: CUSIP matching has significant issues")
                else:
                    print(f"   ⚠️  No CUSIP matching data available")
                
                # Table Statistics
                print(f"\n📋 TABLE STATISTICS:")
                table_counts = status['table_row_counts']
                total_records = sum(table_counts.values())
                
                # Core tables
                core_tables = ['universe_historical', 'portfolio_historical', 'combined_runs_historical']
                for table in core_tables:
                    if table in table_counts:
                        count = table_counts[table]
                        percentage = (count / total_records * 100) if total_records > 0 else 0
                        print(f"   📊 {table}: {count:,} rows ({percentage:.1f}%)")
                
                # Analytics tables
                analytics_tables = ['run_monitor', 'gspread_analytics']
                for table in analytics_tables:
                    if table in table_counts:
                        count = table_counts[table]
                        print(f"   📈 {table}: {count:,} rows")
                
                # Data Quality Issues
                print(f"\n⚠️  DATA QUALITY ISSUES:")
                unmatched_summary = status['unmatched_cusip_summary']
                total_unmatched = unmatched_summary['total_unmatched_last_date']
                
                if total_unmatched == 0:
                    print(f"   🟢 No unmatched CUSIPs detected")
                else:
                    print(f"   🔴 {total_unmatched:,} unmatched CUSIPs in last load")
                    
                    # Show breakdown by table
                    for table, count in unmatched_summary['unmatched_by_table'].items():
                        print(f"      • {table}: {count:,} unmatched")
                
                # Data Freshness
                print(f"\n🕒 DATA FRESHNESS:")
                try:
                    # Get most recent dates from historical tables
                    for table in ['universe_historical', 'portfolio_historical', 'combined_runs_historical']:
                        try:
                            result = pipeline.db_connection.execute_query(f"SELECT MAX(date) FROM {table}")
                            if result and result[0] and result[0][0]:
                                print(f"   📅 {table}: Latest date = {result[0][0]}")
                            else:
                                print(f"   📅 {table}: No date data available")
                        except Exception:
                            print(f"   📅 {table}: Date query failed")
                except Exception as e:
                    print(f"   ⚠️  Could not determine data freshness: {e}")
                
                # Data Distribution Analysis
                print(f"\n📊 DATA DISTRIBUTION ANALYSIS:")
                try:
                    # Simple record counts and unique CUSIPs
                    universe_result = pipeline.db_connection.execute_query("SELECT COUNT(*) FROM universe_historical")
                    universe_count = universe_result[0][0] if universe_result else 0
                    
                    portfolio_result = pipeline.db_connection.execute_query("SELECT COUNT(*) FROM portfolio_historical")
                    portfolio_count = portfolio_result[0][0] if portfolio_result else 0
                    
                    runs_result = pipeline.db_connection.execute_query("SELECT COUNT(*) FROM combined_runs_historical")
                    runs_count = runs_result[0][0] if runs_result else 0
                    
                    print(f"   📊 Record Counts: Universe={universe_count:,}, Portfolio={portfolio_count:,}, Runs={runs_count:,}")
                    
                    # Unique CUSIPs analysis
                    universe_cusips_result = pipeline.db_connection.execute_query("SELECT COUNT(DISTINCT cusip_standardized) FROM universe_historical")
                    universe_cusips = universe_cusips_result[0][0] if universe_cusips_result else 0
                    
                    portfolio_cusips_result = pipeline.db_connection.execute_query("SELECT COUNT(DISTINCT cusip_standardized) FROM portfolio_historical")
                    portfolio_cusips = portfolio_cusips_result[0][0] if portfolio_cusips_result else 0
                    
                    runs_cusips_result = pipeline.db_connection.execute_query("SELECT COUNT(DISTINCT cusip_standardized) FROM combined_runs_historical")
                    runs_cusips = runs_cusips_result[0][0] if runs_cusips_result else 0
                    
                    print(f"   🎯 Unique CUSIPs: Universe={universe_cusips:,}, Portfolio={portfolio_cusips:,}, Runs={runs_cusips:,}")
                    
                    # Coverage analysis
                    if universe_cusips > 0:
                        portfolio_coverage = (portfolio_cusips / universe_cusips * 100) if universe_cusips > 0 else 0
                        runs_coverage = (runs_cusips / universe_cusips * 100) if universe_cusips > 0 else 0
                        print(f"   📈 Coverage: Portfolio={portfolio_coverage:.1f}%, Runs={runs_coverage:.1f}% of universe")
                    
                except Exception as e:
                    print(f"   ⚠️  Could not perform distribution analysis: {e}")
                
                # Last Universe Date Coverage Analysis
                print(f"\n🌍 LAST UNIVERSE DATE COVERAGE:")
                try:
                    # Get the last universe date
                    last_date_result = pipeline.db_connection.execute_query("SELECT MAX(date) FROM universe_historical")
                    if last_date_result and last_date_result[0] and last_date_result[0][0]:
                        last_universe_date = last_date_result[0][0]
                        print(f"   📅 Last universe date: {last_universe_date}")
                        
                        # Get total CUSIPs on last universe date
                        total_last_date_result = pipeline.db_connection.execute_query(
                            "SELECT COUNT(DISTINCT cusip_standardized) FROM universe_historical WHERE date = ?", 
                            (last_universe_date,)
                        )
                        total_last_date_cusips = total_last_date_result[0][0] if total_last_date_result else 0
                        print(f"   🌍 Total CUSIPs on last date: {total_last_date_cusips:,}")
                        
                        # Get orphaned CUSIPs (in other tables but NOT in universe) by table
                        orphaned_by_table = {}
                        
                        # Check portfolio for orphaned CUSIPs
                        portfolio_orphaned_result = pipeline.db_connection.execute_query("""
                            SELECT COUNT(DISTINCT p.cusip_standardized) 
                            FROM portfolio_historical p 
                            LEFT JOIN universe_historical u ON p.cusip_standardized = u.cusip_standardized 
                                AND p.date = u.date
                            WHERE p.date = ? AND u.cusip_standardized IS NULL
                        """, (last_universe_date,))
                        portfolio_orphaned = portfolio_orphaned_result[0][0] if portfolio_orphaned_result else 0
                        if portfolio_orphaned > 0:
                            orphaned_by_table['portfolio_historical'] = portfolio_orphaned
                        
                        # Check combined runs for orphaned CUSIPs
                        runs_orphaned_result = pipeline.db_connection.execute_query("""
                            SELECT COUNT(DISTINCT r.cusip_standardized) 
                            FROM combined_runs_historical r 
                            LEFT JOIN universe_historical u ON r.cusip_standardized = u.cusip_standardized 
                                AND r.date = u.date
                            WHERE r.date = ? AND u.cusip_standardized IS NULL
                        """, (last_universe_date,))
                        runs_orphaned = runs_orphaned_result[0][0] if runs_orphaned_result else 0
                        if runs_orphaned > 0:
                            orphaned_by_table['combined_runs_historical'] = runs_orphaned
                        
                        # Check run monitor for orphaned CUSIPs (current table, no date)
                        monitor_orphaned_result = pipeline.db_connection.execute_query("""
                            SELECT COUNT(DISTINCT m.cusip_standardized) 
                            FROM run_monitor m 
                            LEFT JOIN universe_historical u ON m.cusip_standardized = u.cusip_standardized
                            WHERE u.cusip_standardized IS NULL
                        """)
                        monitor_orphaned = monitor_orphaned_result[0][0] if monitor_orphaned_result else 0
                        if monitor_orphaned > 0:
                            orphaned_by_table['run_monitor'] = monitor_orphaned
                        
                        # Check gspread analytics for orphaned CUSIPs (current table, no date)
                        gspread_orphaned_result = pipeline.db_connection.execute_query("""
                            SELECT COUNT(DISTINCT g.cusip_1_standardized) + COUNT(DISTINCT g.cusip_2_standardized) 
                            FROM gspread_analytics g 
                            LEFT JOIN universe_historical u1 ON g.cusip_1_standardized = u1.cusip_standardized
                            LEFT JOIN universe_historical u2 ON g.cusip_2_standardized = u2.cusip_standardized
                            WHERE u1.cusip_standardized IS NULL AND u2.cusip_standardized IS NULL
                        """)
                        gspread_orphaned = gspread_orphaned_result[0][0] if gspread_orphaned_result else 0
                        if gspread_orphaned > 0:
                            orphaned_by_table['gspread_analytics'] = gspread_orphaned
                        
                        # Calculate total orphaned CUSIPs
                        total_orphaned = sum(orphaned_by_table.values())
                        
                        if total_orphaned > 0:
                            print(f"   ⚠️  Orphaned CUSIPs (in other tables but NOT in universe): {total_orphaned:,}")
                            
                            # Show breakdown by table
                            print(f"   📊 Orphaned by table:")
                            for table, count in orphaned_by_table.items():
                                print(f"      • {table}: {count:,} orphaned")
                            
                            # Show all orphaned CUSIPs
                            print(f"   📝 All orphaned CUSIPs (not in universe):")
                            
                            # Get portfolio orphaned details
                            portfolio_orphaned_details_result = pipeline.db_connection.execute_query("""
                                SELECT p.cusip_standardized, p.security 
                                FROM portfolio_historical p 
                                LEFT JOIN universe_historical u ON p.cusip_standardized = u.cusip_standardized 
                                    AND p.date = u.date
                                WHERE p.date = ? AND u.cusip_standardized IS NULL
                                ORDER BY p.cusip_standardized
                            """, (last_universe_date,))
                            
                            for row in portfolio_orphaned_details_result:
                                cusip, security = row
                                print(f"      - {cusip} ({security[:50]}...) - Orphaned in portfolio")
                            
                            # Get runs orphaned details
                            runs_orphaned_details_result = pipeline.db_connection.execute_query("""
                                SELECT r.cusip_standardized, r.security 
                                FROM combined_runs_historical r 
                                LEFT JOIN universe_historical u ON r.cusip_standardized = u.cusip_standardized 
                                    AND r.date = u.date
                                WHERE r.date = ? AND u.cusip_standardized IS NULL
                                ORDER BY r.cusip_standardized
                            """, (last_universe_date,))
                            
                            for row in runs_orphaned_details_result:
                                cusip, security = row
                                print(f"      - {cusip} ({security[:50]}...) - Orphaned in runs")
                            
                            # Get monitor orphaned details
                            monitor_orphaned_details_result = pipeline.db_connection.execute_query("""
                                SELECT m.cusip_standardized, m.security 
                                FROM run_monitor m 
                                LEFT JOIN universe_historical u ON m.cusip_standardized = u.cusip_standardized
                                WHERE u.cusip_standardized IS NULL
                                ORDER BY m.cusip_standardized
                            """)
                            
                            for row in monitor_orphaned_details_result:
                                cusip, security = row
                                print(f"      - {cusip} ({security[:50]}...) - Orphaned in run monitor")
                            
                            # Get gspread orphaned details
                            gspread_orphaned_details_result = pipeline.db_connection.execute_query("""
                                SELECT g.cusip_1_standardized, g.cusip_2_standardized, g.bond_name_1, g.bond_name_2
                                FROM gspread_analytics g 
                                LEFT JOIN universe_historical u1 ON g.cusip_1_standardized = u1.cusip_standardized
                                LEFT JOIN universe_historical u2 ON g.cusip_2_standardized = u2.cusip_standardized
                                WHERE u1.cusip_standardized IS NULL OR u2.cusip_standardized IS NULL
                                ORDER BY g.cusip_1_standardized, g.cusip_2_standardized
                            """)
                            
                            for row in gspread_orphaned_details_result:
                                cusip1, cusip2, security1, security2 = row
                                if cusip1 and cusip1 not in [None, '']:
                                    print(f"      - {cusip1} ({security1[:50]}...) - Orphaned in gspread (CUSIP_1)")
                                if cusip2 and cusip2 not in [None, '']:
                                    print(f"      - {cusip2} ({security2[:50]}...) - Orphaned in gspread (CUSIP_2)")
                                
                        else:
                            print(f"   🟢 No orphaned CUSIPs found - all CUSIPs in other tables exist in universe")
                            
                    else:
                        print(f"   ⚠️  Could not determine last universe date")
                        
                except Exception as e:
                    print(f"   ⚠️  Could not perform last universe date coverage analysis: {e}")
                
                # Validation Summary
                print(f"\n✅ VALIDATION SUMMARY:")
                print(f"   🟢 Pipeline execution: SUCCESS")
                print(f"   🟢 Database health: {'HEALTHY' if db_health['connection_healthy'] else 'UNHEALTHY'}")
                print(f"   🟢 Data integrity: {'GOOD' if stats['errors_encountered'] == 0 else 'ISSUES DETECTED'}")
                
                if stats['errors_encountered'] > 0:
                    print(f"   🔴 Errors encountered: {stats['errors_encountered']}")
                
                # Recommendations
                print(f"\n💡 RECOMMENDATIONS:")
                if total_unmatched > 0:
                    print(f"   • Review unmatched CUSIPs in unmatched_cusips_last_date table")
                    print(f"   • Check universe data completeness")
                
                if match_rate < 95:
                    print(f"   • Investigate CUSIP standardization issues")
                    print(f"   • Verify universe data quality")
                
                if duration > 300:  # 5 minutes
                    print(f"   • Consider optimizing pipeline performance")
                
                print(f"\n📖 For detailed logs, check: logs/db.log")
                print(f"📊 For database queries, check: logs/db_sql.log")
                print("="*80)
                
        else:
            print("❌ Pipeline execution failed!")
        
        return 0 if success else 1


if __name__ == "__main__":
    exit(main()) 