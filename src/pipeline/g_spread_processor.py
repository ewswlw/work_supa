import pandas as pd
import numpy as np
from pathlib import Path
from rapidfuzz import process, fuzz
import yaml
import io
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime


class GSpreadProcessor:
    """
    G Spread historical data processor with advanced fuzzy matching,
    comprehensive validation, and detailed logging.
    """
    
    def __init__(self, config_dict: dict, logger):
        self.logger = logger
        self.config = config_dict.get('g_spread_processor', {})
        
        # Initialize paths
        self.input_file = Path(self.config.get('input_file', ''))
        self.output_parquet = Path(self.config.get('output_parquet', ''))
        self.output_csv = Path(self.config.get('output_csv', ''))
        self.universe_reference = Path(self.config.get('universe_reference', ''))
        
        # Configuration sections
        self.fuzzy_config = self.config.get('fuzzy_matching', {})
        self.column_config = self.config.get('column_handling', {})
        self.validation_config = self.config.get('validation', {})
        self.error_config = self.config.get('error_handling', {})
        self.logging_config = self.config.get('logging', {})
        
    def process_g_spread_files(self) -> Optional[pd.DataFrame]:
        """
        Main processing function for G spread historical data.
        
        Returns:
            pd.DataFrame: Processed DataFrame or None if processing fails
        """
        try:
            self.logger.info("=" * 50)
            self.logger.info("STARTING G SPREAD PROCESSING PIPELINE")
            self.logger.info("=" * 50)
            
            # Step 1: Load and validate input data
            df = self._load_and_validate_data()
            if df is None:
                return None
                
            # Step 2: Load universe reference data
            universe_df = self._load_universe_reference()
            if universe_df is None:
                return None
                
            # Step 3: Perform fuzzy mapping
            df_mapped = self._perform_fuzzy_mapping(df, universe_df)
            if df_mapped is None:
                return None
                
            # Step 4: Handle duplicate columns
            df_clean = self._handle_duplicate_columns(df_mapped)
            if df_clean is None:
                return None
                
            # Step 5: Save wide format outputs
            self._save_outputs(df_clean, format_type='wide')
            
            # Step 6: Convert to long format
            df_long = self._to_long_format(df_clean)
            
            # Step 7: Add CUSIP column to long format
            df_long = self._add_cusip_column(df_long, universe_df)
            
            # Step 8: Save long format outputs (and overwrite main Parquet)
            self._save_outputs(df_long, format_type='long')
            
            # Step 9: Final validation and analysis (on long format)
            self._perform_final_analysis(df_long, universe_df)
            
            self.logger.info("=" * 50)
            self.logger.info("G SPREAD PROCESSING COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 50)
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"G Spread processing failed: {str(e)}")
            if self.error_config.get('fail_on_validation_errors', True):
                raise
            return None
    
    def _load_and_validate_data(self) -> Optional[pd.DataFrame]:
        """Load and validate G-spread data from CSV files."""
        self.logger.info("--- LOADING AND VALIDATING G-SPREAD DATA ---")
        
        if not self.input_file.exists():
            error_msg = f"Input file not found: {self.input_file}"
            self.logger.error(error_msg)
            if self.error_config.get('fail_on_missing_input', True):
                raise FileNotFoundError(error_msg)
            return None
        
        try:
            try:
                df = pd.read_csv(self.input_file, encoding='utf-8')
                self.logger.info(f"Raw data loaded: {df.shape} (utf-8)")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(self.input_file, encoding='utf-8-sig')
                    self.logger.info(f"Raw data loaded: {df.shape} (utf-8-sig)")
                except UnicodeDecodeError:
                    df = pd.read_csv(self.input_file, encoding='latin1')
                    self.logger.warning(f"Raw data loaded: {df.shape} (latin1 fallback)")
            
            # Convert DATE column to datetime immediately after loading
            date_col = self.column_config.get('date_column', 'DATE')
            if date_col in df.columns:
                self.logger.info(f"Converting {date_col} column from string to datetime...")
                original_dtype = df[date_col].dtype
                
                # Convert to datetime with error handling
                try:
                    df[date_col] = pd.to_datetime(df[date_col], format='%m/%d/%Y', errors='coerce')
                    converted_count = df[date_col].notna().sum()
                    total_count = len(df)
                    
                    self.logger.info(f"Date conversion successful:")
                    self.logger.info(f"  - Original dtype: {original_dtype}")
                    self.logger.info(f"  - New dtype: {df[date_col].dtype}")
                    self.logger.info(f"  - Converted: {converted_count}/{total_count} dates")
                    
                    if converted_count < total_count:
                        failed_count = total_count - converted_count
                        self.logger.warning(f"  - Failed conversions: {failed_count}")
                        
                        # Show examples of failed conversions
                        failed_dates = df[df[date_col].isna()][date_col].unique()[:5]
                        if len(failed_dates) > 0:
                            self.logger.warning(f"  - Example failed dates: {failed_dates}")
                    
                    # Log date range after conversion
                    if converted_count > 0:
                        min_date = df[date_col].min()
                        max_date = df[date_col].max()
                        self.logger.info(f"  - Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
                
                except Exception as e:
                    self.logger.error(f"Date conversion failed: {e}")
                    self.logger.warning("Proceeding with string dates, but this will limit analytics capabilities")
            else:
                self.logger.warning(f"Date column '{date_col}' not found in data")
            
            # Rest of validation logic
            if self.validation_config.get('validate_date_column', True):
                self._validate_date_column(df, date_col)
            
            if self.validation_config.get('validate_data_quality', True):
                self._log_data_quality_metrics(df)
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to load G-spread data: {str(e)}"
            self.logger.error(error_msg)
            if self.error_config.get('fail_on_data_errors', True):
                raise
            return None
    
    def _load_universe_reference(self) -> Optional[pd.DataFrame]:
        """Load and validate universe reference data."""
        self.logger.info("--- LOADING UNIVERSE REFERENCE DATA ---")
        
        if not self.universe_reference.exists():
            error_msg = f"Universe reference file not found: {self.universe_reference}"
            self.logger.error(error_msg)
            if self.error_config.get('fail_on_missing_universe', True):
                raise FileNotFoundError(error_msg)
            return None
        
        try:
            universe_df = pd.read_parquet(self.universe_reference, engine='pyarrow')
            self.logger.info(f"Universe reference loaded: {universe_df.shape}")
            
            # Validate Security column exists
            if 'Security' not in universe_df.columns:
                error_msg = "Universe reference missing 'Security' column"
                self.logger.error(error_msg)
                if self.error_config.get('fail_on_missing_universe', True):
                    raise ValueError(error_msg)
                return None
            
            # Check if universe data is time-series (has Date column)
            if 'Date' in universe_df.columns:
                self.logger.info("Time-series universe data detected - filtering to most recent date only")
                
                # Convert Date column to datetime
                universe_df['Date'] = pd.to_datetime(universe_df['Date'])
                
                # Find the most recent date across ALL records
                most_recent_date = universe_df['Date'].max()
                self.logger.info(f"Most recent date in universe: {most_recent_date.strftime('%Y-%m-%d')}")
                
                # Filter to only records from the most recent date
                universe_df = universe_df[universe_df['Date'] == most_recent_date].copy()
                
                self.logger.info(f"Universe data filtered to most recent date: {universe_df.shape}")
                
                # Check for duplicates in the filtered data
                duplicate_securities = universe_df['Security'].duplicated()
                if duplicate_securities.any():
                    dup_count = duplicate_securities.sum()
                    dup_list = universe_df.loc[duplicate_securities, 'Security'].unique().tolist()
                    self.logger.warning(
                        f"Found {dup_count} duplicate Security values on the most recent date "
                        f"({most_recent_date.strftime('%Y-%m-%d')}): {dup_list[:10]}... "
                        f"Will keep the first occurrence of each duplicate."
                    )
                    
                    # Instead of failing, keep only the first occurrence of each Security
                    universe_df = universe_df.drop_duplicates(subset=['Security'], keep='first')
                    self.logger.info(f"After deduplication: {len(universe_df)} unique securities")
                    
                    # Log which duplicates were removed
                    for security in dup_list[:5]:  # Show first 5 examples
                        original_count = (universe_df['Security'] == security).sum()
                        self.logger.info(f"  Deduplicated '{security}': kept 1 record")
                
                self.logger.info(f"[OK] Universe data successfully filtered to most recent date with {len(universe_df)} unique securities")
                
            else:
                # Check for duplicates in non-time-series data
                duplicate_securities = universe_df['Security'].duplicated()
                if duplicate_securities.any():
                    dup_count = duplicate_securities.sum()
                    dup_list = universe_df.loc[duplicate_securities, 'Security'].unique().tolist()
                    error_msg = (
                        f"Duplicate Security values found in universe reference: {dup_list[:10]}... "
                        f"({dup_count} total duplicates). "
                        f"All Security values must be unique for a valid merge."
                    )
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
            
            # Check universe integrity if configured
            if self.validation_config.get('data_quality', {}).get('check_universe_integrity', True):
                security_count = universe_df['Security'].dropna().nunique()
                self.logger.info(f"Universe contains {security_count} unique securities")
                
                if security_count == 0:
                    error_msg = "Universe reference contains no securities"
                    self.logger.error(error_msg)
                    if self.error_config.get('fail_on_missing_universe', True):
                        raise ValueError(error_msg)
                    return None
            
            return universe_df
            
        except Exception as e:
            self.logger.error(f"Failed to load universe reference: {str(e)}")
            if self.error_config.get('fail_on_missing_universe', True):
                raise
            return None
    
    def _perform_fuzzy_mapping(self, df: pd.DataFrame, universe_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Perform advanced fuzzy matching with multiple scoring methods."""
        self.logger.info("--- PERFORMING FUZZY MAPPING ---")
        
        # Get security names from universe
        security_names = set(universe_df['Security'].dropna().unique())
        self.logger.info(f"Available securities in universe: {len(security_names)}")
        
        # Identify bond columns (exclude date column)
        date_col = self.column_config.get('date_column', 'DATE')
        exclude_patterns = self.column_config.get('exclude_columns', ['Unnamed', '.1'])
        
        bond_columns = set(df.columns) - {date_col}
        
        # Filter out excluded columns
        for pattern in exclude_patterns:
            bond_columns = {col for col in bond_columns if pattern not in col}
        
        self.logger.info(f"Bond columns to process: {len(bond_columns)}")
        
        # Perform unique fuzzy mapping
        available_securities = set(security_names)
        rename_map = {}
        mapping_report = []
        unmapped_bonds = []
        
        # Get fuzzy matching configuration
        threshold = self.fuzzy_config.get('default_threshold', 85)
        scoring_methods = self.fuzzy_config.get('scoring_methods', ['ratio'])
        
        self.logger.info(f"Fuzzy matching threshold: {threshold}%")
        self.logger.info(f"Scoring methods: {scoring_methods}")
        
        for bond in sorted(bond_columns):
            if bond in available_securities:
                # Exact match
                available_securities.remove(bond)
                mapping_report.append((bond, bond, 100.0, 'exact'))
                if self.logging_config.get('log_mapping_details', True):
                    msg = f"KEPT: {bond}"
                    self.logger.info(msg)
                    if self.logging_config.get('log_console_and_file', True):
                        print(msg)
            else:
                # Fuzzy match
                best_match, best_score = self._find_best_fuzzy_match(
                    bond, available_securities, scoring_methods, threshold
                )
                
                if best_match and best_score >= threshold:
                    rename_map[bond] = best_match
                    available_securities.remove(best_match)
                    mapping_report.append((bond, best_match, best_score, 'fuzzy'))
                    bond_ascii = self.replace_unicode_fractions(bond)
                    best_match_ascii = self.replace_unicode_fractions(best_match) if best_match else best_match
                    msg = f"MAPPED: {bond_ascii} -> {best_match_ascii} (Similarity: {best_score:.1f}%)"
                    self.logger.info(msg)
                    if self.logging_config.get('log_console_and_file', True):
                        print(msg)
                else:
                    unmapped_bonds.append(bond)
                    mapping_report.append((bond, None, 0.0, 'unmapped'))
        
        # Log mapping summary
        exact_matches = len([r for r in mapping_report if r[3] == 'exact'])
        fuzzy_matches = len([r for r in mapping_report if r[3] == 'fuzzy'])
        unmapped_count = len(unmapped_bonds)
        
        self.logger.info(f"\\nMAPPING SUMMARY:")
        self.logger.info(f"- Exact matches: {exact_matches}")
        self.logger.info(f"- Fuzzy matches: {fuzzy_matches}")
        self.logger.info(f"- Unmapped bonds: {unmapped_count}")
        
        if unmapped_count > 0:
            self.logger.warning(f"Unmapped bonds: {unmapped_bonds[:10]}...")
            if self.error_config.get('fail_on_no_matches', True) and fuzzy_matches == 0:
                error_msg = "No fuzzy matches found and fail_on_no_matches is enabled"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Apply renaming
        df_mapped = df.rename(columns=rename_map)
        self.logger.info(f"Applied {len(rename_map)} column renames")
        
        return df_mapped
    
    def _find_best_fuzzy_match(self, bond: str, available_securities: set, 
                              scoring_methods: List[str], threshold: float) -> Tuple[Optional[str], float]:
        """Find the best fuzzy match using multiple scoring methods."""
        if not available_securities:
            return None, 0.0
        
        best_match = None
        best_score = 0.0
        
        for method in scoring_methods:
            if method == 'ratio':
                scorer = fuzz.ratio
            elif method == 'partial_ratio':
                scorer = fuzz.partial_ratio
            elif method == 'token_sort_ratio':
                scorer = fuzz.token_sort_ratio
            else:
                continue
            
            match, score, _ = process.extractOne(bond, available_securities, scorer=scorer)
            
            if score > best_score:
                best_score = score
                best_match = match
        
        return best_match, best_score
    
    def _handle_duplicate_columns(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Handle duplicate columns after renaming."""
        self.logger.info("--- HANDLING DUPLICATE COLUMNS ---")
        
        cols = list(df.columns)
        duplicates = set([col for col in cols if cols.count(col) > 1])
        
        if duplicates:
            self.logger.warning(f"Found {len(duplicates)} duplicate column names after renaming")
            
            if self.column_config.get('drop_duplicates', True):
                if self.column_config.get('keep_first_duplicate', True):
                    # Keep only first occurrence
                    _, first_indices = np.unique(cols, return_index=True)
                    df_clean = df.iloc[:, sorted(first_indices)]
                    
                    dropped_count = len(cols) - len(first_indices)
                    self.logger.info(f"Dropped {dropped_count} duplicate columns (kept first occurrence)")
                    
                    for dup in duplicates:
                        indices = [i for i, c in enumerate(cols) if c == dup]
                        self.logger.info(f"Duplicate '{dup}': kept index {indices[0]}, dropped {indices[1:]}")
                else:
                    # Drop all duplicates
                    unique_cols = [col for col in cols if cols.count(col) == 1]
                    df_clean = df[unique_cols]
                    self.logger.info(f"Dropped all {len(duplicates)} duplicate columns")
            else:
                error_msg = f"Duplicate columns found but drop_duplicates is disabled: {list(duplicates)}"
                self.logger.error(error_msg)
                if self.error_config.get('fail_on_all_duplicates', True):
                    raise ValueError(error_msg)
                return None
        else:
            df_clean = df
            self.logger.info("No duplicate columns found")
        
        self.logger.info(f"Final DataFrame shape after duplicate handling: {df_clean.shape}")
        return df_clean
    
    def _perform_final_analysis(self, df: pd.DataFrame, universe_df: pd.DataFrame):
        """Perform final analysis and logging."""
        self.logger.info("--- PERFORMING FINAL ANALYSIS ---")
        
        # Log final DataFrame info
        buf = io.StringIO()
        df.info(buf=buf)
        if self.logging_config.get('log_statistics', True):
            self.logger.info(f"Final DataFrame info:\\n{buf.getvalue()}")
        
        # Date coverage analysis
        if self.logging_config.get('log_date_coverage', True):
            self._log_date_coverage_analysis(df)
        
        # Security matching analysis (updated for long format)
        self._log_security_matching_analysis_long_format(df, universe_df)
        
        # Data quality metrics
        if self.logging_config.get('log_statistics', True):
            self._log_final_statistics(df)
    
    def _log_date_coverage_analysis(self, df: pd.DataFrame):
        """Log comprehensive date coverage analysis for datetime objects."""
        date_col = self.column_config.get('date_column', 'DATE')
        
        if date_col in df.columns:
            # Get unique dates
            unique_dates = df[date_col].dropna().unique()
            
            self.logger.info(f"\\n=========================")
            self.logger.info(f"=== DATE COVERAGE ANALYSIS ===")
            self.logger.info(f"=========================")
            self.logger.info(f"Total unique dates: {len(unique_dates)}")
            self.logger.info(f"Date column dtype: {df[date_col].dtype}")
            
            if len(unique_dates) > 0:
                # Check if we have datetime objects
                if pd.api.types.is_datetime64_any_dtype(df[date_col]):
                    # Enhanced analysis for datetime objects
                    sorted_dates = sorted(unique_dates)
                    min_date = sorted_dates[0]
                    max_date = sorted_dates[-1]
                    
                    # Convert to pandas datetime for easier formatting
                    min_dt = pd.to_datetime(min_date)
                    max_dt = pd.to_datetime(max_date)
                    
                    self.logger.info(f"Date range: {min_dt.strftime('%Y-%m-%d')} to {max_dt.strftime('%Y-%m-%d')}")
                    
                    # Calculate time span
                    time_span = max_dt - min_dt
                    self.logger.info(f"Time span: {time_span.days} days ({time_span.days/365.25:.1f} years)")
                    
                    # Year distribution analysis
                    df_temp = df[df[date_col].notna()].copy()
                    df_temp['year'] = df_temp[date_col].dt.year
                    df_temp['month'] = df_temp[date_col].dt.month
                    df_temp['quarter'] = df_temp[date_col].dt.quarter
                    df_temp['weekday'] = df_temp[date_col].dt.day_name()
                    
                    year_dist = df_temp['year'].value_counts().sort_index()
                    self.logger.info(f"\\nYear distribution:")
                    for year, count in year_dist.items():
                        self.logger.info(f"  - {year}: {count} dates")
                    
                    # Business day analysis
                    business_days = df_temp[~df_temp['weekday'].isin(['Saturday', 'Sunday'])]
                    weekend_days = df_temp[df_temp['weekday'].isin(['Saturday', 'Sunday'])]
                    
                    self.logger.info(f"\\nBusiness day analysis:")
                    self.logger.info(f"  - Business days: {len(business_days)} ({len(business_days)/len(df_temp)*100:.1f}%)")
                    self.logger.info(f"  - Weekend days: {len(weekend_days)} ({len(weekend_days)/len(df_temp)*100:.1f}%)")
                    
                    # Monthly distribution (recent 12 months)
                    recent_data = df_temp[df_temp[date_col] >= (max_dt - pd.DateOffset(months=12))]
                    if len(recent_data) > 0:
                        monthly_dist = recent_data.groupby([recent_data[date_col].dt.year, recent_data[date_col].dt.month]).size()
                        self.logger.info(f"\\nRecent 12 months distribution:")
                        for (year, month), count in monthly_dist.tail(12).items():
                            month_name = pd.to_datetime(f'{year}-{month}-01').strftime('%b %Y')
                            self.logger.info(f"  - {month_name}: {count} dates")
                    
                    # Sample of earliest and latest dates
                    self.logger.info(f"\\nSample dates:")
                    for i, date in enumerate(sorted_dates[:5]):
                        date_dt = pd.to_datetime(date)
                        date_count = (df[date_col] == date).sum()
                        self.logger.info(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
                    
                    if len(sorted_dates) > 10:
                        self.logger.info(f"  ... ({len(sorted_dates) - 10} more dates) ...")
                        for i, date in enumerate(sorted_dates[-5:]):
                            date_dt = pd.to_datetime(date)
                            date_count = (df[date_col] == date).sum()
                            self.logger.info(f"  - {date_dt.strftime('%Y-%m-%d (%a)')}: {date_count} records")
                            
                else:
                    # Fallback for string dates (legacy behavior)
                    self.logger.warning("DATE column is not datetime type - converting for analysis...")
                    try:
                        datetime_dates = pd.to_datetime(unique_dates)
                        sorted_datetime = sorted(datetime_dates)
                        
                        earliest_date = f"{sorted_datetime[0].month}/{sorted_datetime[0].day}/{sorted_datetime[0].year}"
                        latest_date = f"{sorted_datetime[-1].month}/{sorted_datetime[-1].day}/{sorted_datetime[-1].year}"
                        
                        self.logger.info(f"Raw date range: {earliest_date} to {latest_date}")
                        self.logger.info(f"Formatted date range: {sorted_datetime[0].strftime('%Y-%m-%d')} to {sorted_datetime[-1].strftime('%Y-%m-%d')}")
                        
                    except Exception as e:
                        self.logger.warning(f"Could not parse dates for analysis: {e}")
                        sorted_dates = sorted(unique_dates)
                        self.logger.info(f"Raw date range (string sorted): {sorted_dates[0]} to {sorted_dates[-1]}")
                        
            self.logger.info(f"\\n=== END DATE ANALYSIS ===\\n")
    
    def _log_security_matching_analysis_long_format(self, df: pd.DataFrame, universe_df: pd.DataFrame):
        """Log analysis of security matching for long format data."""
        self.logger.info(f"\\n=========================")
        self.logger.info(f"=== SECURITY MATCHING ANALYSIS (LONG FORMAT) ===")
        self.logger.info(f"=========================")
        
        if 'Security' in df.columns:
            # Analyze securities in the long format data
            securities_in_data = set(df['Security'].dropna().unique())
            securities_in_universe = set(universe_df['Security'].dropna().unique())
            
            matched_securities = securities_in_data.intersection(securities_in_universe)
            unmatched_securities = securities_in_data - securities_in_universe
            
            self.logger.info(f"Total unique securities in data: {len(securities_in_data)}")
            self.logger.info(f"Matched to universe: {len(matched_securities)}")
            self.logger.info(f"Unmatched: {len(unmatched_securities)}")
            
            if len(securities_in_data) > 0:
                match_rate = len(matched_securities) / len(securities_in_data) * 100
                self.logger.info(f"Match rate: {match_rate:.1f}%")
            
            if unmatched_securities:
                unmatched_list = sorted(list(unmatched_securities))
                self.logger.warning(f"Example unmatched securities: {unmatched_list[:10]}")
                if len(unmatched_list) > 10:
                    self.logger.warning(f"... and {len(unmatched_list) - 10} more")
            
            # Additional long format statistics
            if 'CUSIP' in df.columns:
                cusip_coverage = df['CUSIP'].notna().sum() / len(df) * 100
                self.logger.info(f"CUSIP coverage: {cusip_coverage:.2f}%")
            
            if 'GSpread' in df.columns:
                gspread_coverage = df['GSpread'].notna().sum() / len(df) * 100
                self.logger.info(f"G-Spread data coverage: {gspread_coverage:.2f}%")
        else:
            self.logger.warning("No 'Security' column found for analysis")
    
    def _log_security_matching_analysis(self, df: pd.DataFrame, universe_df: pd.DataFrame):
        """Log analysis of security matching after processing (for wide format data)."""
        date_col = self.column_config.get('date_column', 'DATE')
        security_names = set(universe_df['Security'].dropna().unique())
        bond_columns = set(df.columns) - {date_col}
        
        matched_bonds = [col for col in bond_columns if col in security_names]
        unmatched_bonds = [col for col in bond_columns if col not in security_names]
        
        self.logger.info(f"\\n=========================")
        self.logger.info(f"=== SECURITY MATCHING ANALYSIS (WIDE FORMAT) ===")
        self.logger.info(f"=========================")
        self.logger.info(f"Total bond columns: {len(bond_columns)}")
        self.logger.info(f"Matched to universe: {len(matched_bonds)}")
        self.logger.info(f"Unmatched: {len(unmatched_bonds)}")
        
        if len(bond_columns) > 0:
            match_rate = len(matched_bonds) / len(bond_columns) * 100
            self.logger.info(f"Match rate: {match_rate:.1f}%")
        
        if unmatched_bonds:
            self.logger.warning(f"Unmatched bonds: {unmatched_bonds[:10]}")
            if len(unmatched_bonds) > 10:
                self.logger.warning(f"... and {len(unmatched_bonds) - 10} more")
    
    def _log_final_statistics(self, df: pd.DataFrame):
        """Log final DataFrame statistics."""
        self.logger.info(f"\\n=========================")
        self.logger.info(f"=== FINAL STATISTICS ===")
        self.logger.info(f"=========================")
        
        # Basic stats
        self.logger.info(f"Final shape: {df.shape}")
        self.logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        # Missing value analysis
        missing_stats = df.isnull().sum()
        high_missing = missing_stats[missing_stats > len(df) * 0.5]
        if len(high_missing) > 0:
            self.logger.warning(f"Columns with >50% missing values: {len(high_missing)}")
            for col, missing_count in high_missing.head().items():
                pct = missing_count / len(df) * 100
                self.logger.warning(f"  - {col}: {pct:.1f}% missing")
    
    def _validate_date_column(self, df: pd.DataFrame, date_col: str):
        """Validate date column format and content."""
        date_formats = self.validation_config.get('date_formats', ['%m/%d/%Y', '%Y-%m-%d'])
        
        # Try to parse dates
        parsed_dates = None
        for fmt in date_formats:
            try:
                parsed_dates = pd.to_datetime(df[date_col], format=fmt, errors='coerce')
                if parsed_dates.notna().sum() > len(df) * 0.8:  # 80% success rate
                    self.logger.info(f"Date column validated with format: {fmt}")
                    break
            except:
                continue
        
        if parsed_dates is None or parsed_dates.notna().sum() < len(df) * 0.5:
            self.logger.warning(f"Date column validation failed for {date_col}")
    
    def _log_data_quality_metrics(self, df: pd.DataFrame):
        """Log comprehensive data quality metrics."""
        self.logger.info("--- DATA QUALITY METRICS ---")
        
        # Missing data analysis
        missing_pct = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
        self.logger.info(f"Overall missing data: {missing_pct:.2f}%")
        
        # Check maximum missing percentage threshold
        max_missing = self.validation_config.get('data_quality', {}).get('max_missing_percentage', 95)
        if missing_pct > max_missing:
            error_msg = f"Data quality check failed: {missing_pct:.1f}% > {max_missing}% missing"
            self.logger.error(error_msg)
            if self.error_config.get('fail_on_validation_errors', True):
                raise ValueError(error_msg)
        
        # Numeric columns analysis
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        self.logger.info(f"Numeric columns: {len(numeric_cols)}")
        
        for col in numeric_cols[:5]:  # Log first 5 numeric columns
            valid_pct = df[col].notna().sum() / len(df) * 100
            self.logger.info(f"  - {col}: {valid_pct:.1f}% valid data")
    
    def _to_long_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert wide DataFrame to long format with columns: DATE, Security, GSpread."""
        date_col = self.column_config.get('date_column', 'DATE')
        bond_columns = [col for col in df.columns if col != date_col]
        long_df = pd.melt(df, id_vars=[date_col], value_vars=bond_columns,
                          var_name='Security', value_name='GSpread')
        return long_df
    
    def _add_cusip_column(self, df_long: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
        """Add CUSIP column as the first column by matching Security to universe reference from most recent date. Includes detailed logging."""
        self.logger.info("--- ADDING CUSIP COLUMN VIA UNIVERSE LOOKUP ---")
        
        # Log universe data details
        if 'Date' in universe_df.columns:
            universe_dates = universe_df['Date'].unique()
            if len(universe_dates) == 1:
                self.logger.info(f"Using universe data from date: {pd.to_datetime(universe_dates[0]).strftime('%Y-%m-%d')}")
            else:
                self.logger.warning(f"Universe data contains {len(universe_dates)} dates - expected only most recent date")
        else:
            self.logger.info("Using static universe data (no Date column)")
        
        self.logger.info(f"Universe contains {len(universe_df)} securities for CUSIP matching")
        
        # Ensure universe_df has required columns
        if 'Security' not in universe_df.columns or 'CUSIP' not in universe_df.columns:
            self.logger.error("Universe reference must contain 'Security' and 'CUSIP' columns.")
            raise ValueError("Universe reference missing required columns.")
        # Merge to add CUSIP
        merged = df_long.merge(
            universe_df[['Security', 'CUSIP']],
            on='Security',
            how='left',
            validate='many_to_one',
            indicator=True
        )
        # Move CUSIP to first column
        cols = merged.columns.tolist()
        cols.insert(0, cols.pop(cols.index('CUSIP')))
        merged = merged[cols]
        # Logging before filtering
        total_before = len(merged)
        matched = merged['CUSIP'].notna().sum()
        unmatched = total_before - matched
        pct_matched = matched / total_before * 100 if total_before else 0
        pct_unmatched = unmatched / total_before * 100 if total_before else 0
        self.logger.info(f"Total records before filtering: {total_before}")
        self.logger.info(f"Matched CUSIP: {matched} ({pct_matched:.2f}%)")
        self.logger.info(f"Unmatched CUSIP: {unmatched} ({pct_unmatched:.2f}%)")
        
        if unmatched > 0:
            unmatched_securities = merged.loc[merged['CUSIP'].isna(), 'Security'].unique()
            self.logger.warning(f"Example unmatched securities: {unmatched_securities[:10]}")
            
            # Filter out records with missing CUSIP
            self.logger.info(f"Removing {unmatched} records with missing CUSIP...")
            merged_filtered = merged[merged['CUSIP'].notna()].copy()
            
            # Log results after filtering
            total_after = len(merged_filtered)
            self.logger.info(f"Total records after filtering: {total_after}")
            self.logger.info(f"Records removed: {total_before - total_after}")
            self.logger.info(f"Final match rate: 100.00% (all remaining records have CUSIP)")
        else:
            merged_filtered = merged.copy()
            self.logger.info("No unmatched records to remove - all records have CUSIP")
        
        # Example matches
        if len(merged_filtered) > 0:
            example_matches = merged_filtered[['Security', 'CUSIP']].drop_duplicates().head(10)
            self.logger.info(f"Example matches:\n{example_matches}")
        
        # Remove merge indicator column
        merged_filtered = merged_filtered.drop(columns=['_merge'])
        return merged_filtered
    
    def _save_outputs(self, df: pd.DataFrame, format_type: str = 'wide'):
        """Save processed data to CSV for both wide and long formats. For long format, also overwrite the main Parquet file."""
        self.logger.info(f"--- SAVING OUTPUTS ({format_type.upper()} FORMAT) ---")
        try:
            # Determine output paths
            processed_data_dir = Path('historical g spread/processed data')
            processed_data_dir.mkdir(parents=True, exist_ok=True)
            if format_type == 'wide':
                csv_path = processed_data_dir / 'bond_g_sprd_wide.csv'
                # Save to CSV only
                df.to_csv(csv_path, index=False)
                self.logger.info(f"Saved CSV (wide): {csv_path}")
                csv_size = csv_path.stat().st_size / 1024**2
                self.logger.info(f"CSV file size (wide): {csv_size:.1f} MB")
            else:
                csv_path = processed_data_dir / 'bond_g_sprd_long.csv'
                # Save to CSV
                df.to_csv(csv_path, index=False)
                self.logger.info(f"Saved CSV (long): {csv_path}")
                csv_size = csv_path.stat().st_size / 1024**2
                self.logger.info(f"CSV file size (long): {csv_size:.1f} MB")
                # Overwrite the main Parquet file with long format
                main_parquet_path = Path('historical g spread/bond_g_sprd_time_series.parquet')
                df.to_parquet(main_parquet_path, index=False)
                parquet_size = main_parquet_path.stat().st_size / 1024**2
                self.logger.info(f"Overwrote main Parquet file with long format: {main_parquet_path}")
                self.logger.info(f"Main Parquet file size (long): {parquet_size:.1f} MB")
        except Exception as e:
            self.logger.error(f"Failed to save outputs: {str(e)}")
            raise
    
    def filter_by_date_range(self, df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
        """Filter DataFrame by date range. Expects datetime DATE column.
        
        Args:
            df: DataFrame with datetime DATE column
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Filtered DataFrame
        """
        date_col = self.column_config.get('date_column', 'DATE')
        
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            raise ValueError(f"{date_col} column must be datetime type for date filtering")
        
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        mask = (df[date_col] >= start_dt) & (df[date_col] <= end_dt)
        filtered_df = df[mask].copy()
        
        self.logger.info(f"Filtered by date range {start_date} to {end_date}: {len(filtered_df)} records")
        return filtered_df
    
    def get_year_data(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Extract data for a specific year.
        
        Args:
            df: DataFrame with datetime DATE column
            year: Year to extract (e.g., 2024)
            
        Returns:
            DataFrame filtered to the specified year
        """
        date_col = self.column_config.get('date_column', 'DATE')
        
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            raise ValueError(f"{date_col} column must be datetime type for year filtering")
        
        mask = df[date_col].dt.year == year
        year_df = df[mask].copy()
        
        self.logger.info(f"Extracted year {year} data: {len(year_df)} records")
        return year_df
    
    def get_business_days_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to business days only (Monday-Friday).
        
        Args:
            df: DataFrame with datetime DATE column
            
        Returns:
            DataFrame filtered to business days only
        """
        date_col = self.column_config.get('date_column', 'DATE')
        
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            raise ValueError(f"{date_col} column must be datetime type for business day filtering")
        
        # Monday=0, Sunday=6, so weekdays 0-4 are business days
        mask = df[date_col].dt.weekday < 5
        business_df = df[mask].copy()
        
        self.logger.info(f"Business days only: {len(business_df)} records ({len(business_df)/len(df)*100:.1f}%)")
        return business_df
    
    def resample_to_monthly(self, df: pd.DataFrame, agg_method: str = 'last') -> pd.DataFrame:
        """Resample daily data to monthly frequency.
        
        Args:
            df: DataFrame with datetime DATE column
            agg_method: Aggregation method ('last', 'first', 'mean', 'median')
            
        Returns:
            DataFrame resampled to monthly frequency
        """
        date_col = self.column_config.get('date_column', 'DATE')
        
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            raise ValueError(f"{date_col} column must be datetime type for resampling")
        
        # Set date as index for resampling
        df_resample = df.set_index(date_col)
        
        # Resample to month-end frequency
        if agg_method == 'last':
            monthly_df = df_resample.groupby(pd.Grouper(freq='M')).last()
        elif agg_method == 'first':
            monthly_df = df_resample.groupby(pd.Grouper(freq='M')).first()
        elif agg_method == 'mean':
            monthly_df = df_resample.groupby(pd.Grouper(freq='M')).mean()
        elif agg_method == 'median':
            monthly_df = df_resample.groupby(pd.Grouper(freq='M')).median()
        else:
            raise ValueError(f"Unsupported aggregation method: {agg_method}")
        
        # Reset index to get DATE back as column
        monthly_df = monthly_df.reset_index()
        
        self.logger.info(f"Resampled to monthly using '{agg_method}': {len(monthly_df)} periods")
        return monthly_df
    
    def add_date_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add useful date-based features for analytics.
        
        Args:
            df: DataFrame with datetime DATE column
            
        Returns:
            DataFrame with additional date features
        """
        date_col = self.column_config.get('date_column', 'DATE')
        
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            raise ValueError(f"{date_col} column must be datetime type for feature extraction")
        
        df_enhanced = df.copy()
        
        # Add date features
        df_enhanced['Year'] = df_enhanced[date_col].dt.year
        df_enhanced['Month'] = df_enhanced[date_col].dt.month
        df_enhanced['Quarter'] = df_enhanced[date_col].dt.quarter
        df_enhanced['DayOfWeek'] = df_enhanced[date_col].dt.dayofweek  # Monday=0
        df_enhanced['DayName'] = df_enhanced[date_col].dt.day_name()
        df_enhanced['MonthName'] = df_enhanced[date_col].dt.month_name()
        df_enhanced['IsBusinessDay'] = df_enhanced[date_col].dt.weekday < 5
        df_enhanced['IsMonthEnd'] = df_enhanced[date_col].dt.is_month_end
        df_enhanced['IsQuarterEnd'] = df_enhanced[date_col].dt.is_quarter_end
        df_enhanced['IsYearEnd'] = df_enhanced[date_col].dt.is_year_end
        
        self.logger.info(f"Added {9} date features for analytics")
        return df_enhanced

    def replace_unicode_fractions(self, text):
        if not isinstance(text, str):
            return text
        return (
            text.replace('⅛', '1/8')
                .replace('⅜', '3/8')
                .replace('⅝', '5/8')
                .replace('⅞', '7/8')
                .replace('⅓', '1/3')
                .replace('⅔', '2/3')
                .replace('¼', '1/4')
                .replace('¾', '3/4')
                .replace('½', '1/2')
        )


def process_g_spread_files(logger=None) -> Optional[pd.DataFrame]:
    """
    Main entry point for G spread processing.
    
    Args:
        logger: Logger instance (optional, will create one if not provided)
        
    Returns:
        pd.DataFrame: Processed long format DataFrame or None if failed
    """
    try:
        # Load configuration directly from YAML
        config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
        try:
            with open(config_path, 'r', encoding='utf-8-sig') as f:
                config_dict = yaml.safe_load(f)
        except UnicodeDecodeError:
            with open(config_path, 'r', encoding='latin1') as f:
                config_dict = yaml.safe_load(f)
        
        # Create logger if not provided
        if logger is None:
            from src.utils.logging import LogManager
            log_manager = LogManager("logs/g_spread_processor.log", "INFO")
            logger = log_manager.logger
        
        # Initialize processor
        processor = GSpreadProcessor(config_dict, logger)
        
        # Process files - this already does all the analysis internally
        result_df = processor.process_g_spread_files()
        
        # Return the wide format result - the long format has already been saved and analyzed
        return result_df
        
    except Exception as e:
        if logger:
            logger.error(f"G spread processing failed: {str(e)}")
        else:
            print(f"G spread processing failed: {str(e)}")
        raise 