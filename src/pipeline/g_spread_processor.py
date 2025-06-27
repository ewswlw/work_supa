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
                
            # Step 5: Final validation and analysis
            self._perform_final_analysis(df_clean, universe_df)
            
            # Step 6: Save outputs
            self._save_outputs(df_clean)
            
            self.logger.info("=" * 50)
            self.logger.info("G SPREAD PROCESSING COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 50)
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"G Spread processing failed: {str(e)}", exc=e)
            if self.error_config.get('fail_on_validation_errors', True):
                raise
            return None
    
    def _load_and_validate_data(self) -> Optional[pd.DataFrame]:
        """Load CSV data and perform comprehensive validation."""
        self.logger.info("--- LOADING AND VALIDATING INPUT DATA ---")
        
        # Check if input file exists
        if not self.input_file.exists():
            error_msg = f"Input file not found: {self.input_file}"
            self.logger.error(error_msg)
            if self.error_config.get('fail_on_validation_errors', True):
                raise FileNotFoundError(error_msg)
            return None
        
        try:
            # Load CSV
            self.logger.info(f"Loading CSV from: {self.input_file}")
            df = pd.read_csv(self.input_file)
            
            # Log initial DataFrame info
            self.logger.info(f"Initial DataFrame shape: {df.shape}")
            buf = io.StringIO()
            df.info(buf=buf)
            if self.logging_config.get('log_console_and_file', True):
                self.logger.info(f"DataFrame info:\\n{buf.getvalue()}")
            
            # Validate required columns
            required_cols = self.validation_config.get('required_columns', ['DATE'])
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                error_msg = f"Missing required columns: {missing_cols}"
                self.logger.error(error_msg)
                if self.error_config.get('fail_on_validation_errors', True):
                    raise ValueError(error_msg)
                return None
            
            # Validate minimum rows
            min_rows = self.validation_config.get('data_quality', {}).get('min_rows', 100)
            if len(df) < min_rows:
                error_msg = f"Insufficient data: {len(df)} rows < {min_rows} minimum"
                self.logger.error(error_msg)
                if self.error_config.get('fail_on_validation_errors', True):
                    raise ValueError(error_msg)
                return None
            
            # Validate date column
            date_col = self.column_config.get('date_column', 'DATE')
            if date_col in df.columns:
                self._validate_date_column(df, date_col)
            
            # Log data quality metrics
            self._log_data_quality_metrics(df)
            
            self.logger.info(f"Data validation completed successfully: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load/validate data: {str(e)}", exc=e)
            if self.error_config.get('fail_on_validation_errors', True):
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
            self.logger.error(f"Failed to load universe reference: {str(e)}", exc=e)
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
                    msg = f"MAPPED: {bond} -> {best_match} (Similarity: {best_score:.1f}%)"
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
        
        # Security matching analysis
        self._log_security_matching_analysis(df, universe_df)
        
        # Data quality metrics
        if self.logging_config.get('log_statistics', True):
            self._log_final_statistics(df)
    
    def _log_date_coverage_analysis(self, df: pd.DataFrame):
        """Log comprehensive date coverage analysis."""
        date_col = self.column_config.get('date_column', 'DATE')
        
        if date_col in df.columns:
            unique_dates = sorted(df[date_col].dropna().unique())
            self.logger.info(f"\\n=========================")
            self.logger.info(f"=== DATE COVERAGE ANALYSIS ===")
            self.logger.info(f"=========================")
            self.logger.info(f"Total unique dates: {len(unique_dates)}")
            
            if unique_dates:
                # Convert numpy datetime64 to pandas datetime for proper formatting
                start_date = pd.to_datetime(unique_dates[0]).strftime('%Y-%m-%d')
                end_date = pd.to_datetime(unique_dates[-1]).strftime('%Y-%m-%d')
                self.logger.info(f"Date range: {start_date} to {end_date}")
                
                self.logger.info(f"Date details:")
                for date in unique_dates[:10]:  # First 10 dates
                    date_count = (df[date_col] == date).sum()
                    date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
                    self.logger.info(f"  - {date_str}: {date_count} records")
                
                if len(unique_dates) > 10:
                    self.logger.info(f"  ... and {len(unique_dates) - 10} more dates")
    
    def _log_security_matching_analysis(self, df: pd.DataFrame, universe_df: pd.DataFrame):
        """Log analysis of security matching after processing."""
        date_col = self.column_config.get('date_column', 'DATE')
        security_names = set(universe_df['Security'].dropna().unique())
        bond_columns = set(df.columns) - {date_col}
        
        matched_bonds = [col for col in bond_columns if col in security_names]
        unmatched_bonds = [col for col in bond_columns if col not in security_names]
        
        self.logger.info(f"\\n=========================")
        self.logger.info(f"=== SECURITY MATCHING ANALYSIS ===")
        self.logger.info(f"=========================")
        self.logger.info(f"Total bond columns: {len(bond_columns)}")
        self.logger.info(f"Matched to universe: {len(matched_bonds)}")
        self.logger.info(f"Unmatched: {len(unmatched_bonds)}")
        self.logger.info(f"Match rate: {len(matched_bonds)/len(bond_columns)*100:.1f}%")
        
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
    
    def _save_outputs(self, df: pd.DataFrame):
        """Save processed data to Parquet and CSV."""
        self.logger.info("--- SAVING OUTPUTS ---")
        
        try:
            # Save to Parquet
            df.to_parquet(self.output_parquet, index=False)
            self.logger.info(f"Saved Parquet: {self.output_parquet}")
            
            # Save to CSV
            self.output_csv.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(self.output_csv, index=False)
            self.logger.info(f"Saved CSV: {self.output_csv}")
            
            # Log file sizes
            parquet_size = self.output_parquet.stat().st_size / 1024**2
            csv_size = self.output_csv.stat().st_size / 1024**2
            compression_ratio = csv_size / parquet_size if parquet_size > 0 else 0
            
            self.logger.info(f"File sizes - Parquet: {parquet_size:.1f} MB, CSV: {csv_size:.1f} MB")
            self.logger.info(f"Compression ratio: {compression_ratio:.1f}:1")
            
        except Exception as e:
            self.logger.error(f"Failed to save outputs: {str(e)}", exc=e)
            raise


def process_g_spread_files(logger) -> Optional[pd.DataFrame]:
    """
    Main entry point for G spread processing.
    
    Args:
        logger: Logger instance
        
    Returns:
        pd.DataFrame: Processed DataFrame or None if failed
    """
    try:
        # Load configuration directly from YAML
        config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        # Initialize processor
        processor = GSpreadProcessor(config_dict, logger)
        
        # Process files
        result_df = processor.process_g_spread_files()
        
        return result_df
        
    except Exception as e:
        logger.error(f"G spread processing failed: {str(e)}", exc=e)
        raise 