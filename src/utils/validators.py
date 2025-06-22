"""
Data validation utilities.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import re
from datetime import datetime


class DataValidator:
    """
    Performs comprehensive data validation and quality checks on a DataFrame.
    Returns structured dictionaries of results and errors.
    """

    def __init__(self, df: pd.DataFrame, numeric_cols: List[str] = None, categorical_cols: List[str] = None):
        """
        Initializes the validator with the DataFrame and optional column lists.
        
        Args:
            df (pd.DataFrame): The DataFrame to validate.
            numeric_cols (List[str], optional): List of columns to treat as numeric.
            categorical_cols (List[str], optional): List of columns for categorical analysis.
        """
        self.df = df
        self.numeric_cols = numeric_cols if numeric_cols else df.select_dtypes(include=np.number).columns.tolist()
        self.categorical_cols = categorical_cols if categorical_cols else df.select_dtypes(include=['object', 'category']).columns.tolist()
        self.results: Dict[str, Any] = {}
        self.errors: Dict[str, Any] = {}

    def run_all_checks(self) -> None:
        """Runs all available validation checks."""
        self.check_non_numeric_in_numeric_cols()
        self.analyze_nulls()
        self.summarize_statistics()
        self.analyze_categorical_distribution()

    def check_non_numeric_in_numeric_cols(self) -> None:
        """
        Identifies rows where supposedly numeric columns contain non-numeric data.
        This is a common issue after loading data from sources like Excel.
        """
        non_numeric_rows = {}
        for col in self.numeric_cols:
            if col in self.df.columns:
                # Attempt to convert to numeric, coercing errors to NaN
                numeric_series = pd.to_numeric(self.df[col], errors='coerce')
                # Find rows that became NaN, but were not NaN originally
                offenders = self.df[numeric_series.isna() & self.df[col].notna()]
                if not offenders.empty:
                    non_numeric_rows[col] = offenders.index.tolist()
        
        if non_numeric_rows:
            self.errors['non_numeric_rows'] = non_numeric_rows

    def analyze_nulls(self) -> None:
        """Performs a detailed analysis of null values across all columns."""
        null_counts = self.df.isnull().sum()
        total_rows = len(self.df)
        if total_rows == 0:
            self.results['null_analysis'] = {}
            return

        null_analysis = {}
        for col, count in null_counts.items():
            if count > 0:
                null_analysis[col] = {
                    'count': int(count),
                    'percentage': (count / total_rows) * 100
                }
        self.results['null_analysis'] = null_analysis

    def summarize_statistics(self) -> None:
        """Calculates descriptive statistics for all numeric columns."""
        # Ensure we only work with columns that are actually numeric
        numeric_df = self.df[self.numeric_cols].apply(pd.to_numeric, errors='coerce')
        if not numeric_df.empty:
            self.results['statistical_summary'] = numeric_df.describe().to_dict()

    def analyze_categorical_distribution(self) -> None:
        """Analyzes the distribution of values in categorical columns."""
        categorical_distribution = {}
        for col in self.categorical_cols:
            if col in self.df.columns:
                counts = self.df[col].value_counts()
                total_rows = len(self.df)
                if total_rows > 0:
                    percentages = (counts / total_rows) * 100
                    categorical_distribution[col] = {
                        'value_counts': counts.to_dict(),
                        'percentages': percentages.to_dict(),
                        'unique_count': len(counts)
                    }
        self.results['categorical_distribution'] = categorical_distribution

    @staticmethod
    def validate_numeric_ranges(df: pd.DataFrame, logger=None) -> bool:
        """Validate numeric columns are within expected ranges"""
        try:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                if col in ['Bid Price', 'Ask Price']:
                    negative_count = (df[col] < 0).sum()
                    if negative_count > 0:
                        msg = f"Found {negative_count} negative values in {col}"
                        if logger:
                            logger.warning(msg)
                        return False
                
                if col in ['Bid Size', 'Ask Size']:
                    negative_count = (df[col] < 0).sum()
                    if negative_count > 0:
                        msg = f"Found {negative_count} negative values in {col}"
                        if logger:
                            logger.warning(msg)
                        return False
            
            return True
            
        except Exception as e:
            if logger:
                logger.error("Error validating numeric ranges", e)
            return False

    @staticmethod
    def validate_date_time(df: pd.DataFrame, logger=None) -> bool:
        """Validate date and time columns"""
        try:
            if 'Date' in df.columns:
                # Check for null dates
                null_dates = df['Date'].isna().sum()
                if null_dates > 0:
                    msg = f"Found {null_dates} null values in Date column"
                    if logger:
                        logger.warning(msg)
                
                # Check for future dates
                try:
                    date_col = pd.to_datetime(df['Date'], errors='coerce')
                    today = pd.Timestamp(datetime.now().date())
                    future_dates = (date_col > today).sum()
                    if future_dates > 0:
                        msg = f"Found {future_dates} future dates"
                        if logger:
                            logger.warning(msg)
                except Exception:
                    pass
            
            if 'Time' in df.columns:
                null_times = df['Time'].isna().sum()
                if null_times > 0:
                    msg = f"Found {null_times} null values in Time column"
                    if logger:
                        logger.warning(msg)
                
                # Validate time format for string times
                try:
                    invalid_times = df['Time'].apply(
                        lambda t: isinstance(t, str) and not re.match(r'^([01]?\d|2[0-3]):[0-5]\d$', t)
                    ).sum()
                    if invalid_times > 0:
                        msg = f"Found {invalid_times} invalid time strings"
                        if logger:
                            logger.warning(msg)
                except Exception:
                    pass
            
            return True
            
        except Exception as e:
            if logger:
                logger.error("Error validating date/time columns", e)
            return False

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_cols: List[str], logger=None) -> bool:
        """Validate that required columns are present"""
        try:
            missing_cols = set(required_cols) - set(df.columns)
            if missing_cols:
                msg = f"Missing required columns: {missing_cols}"
                if logger:
                    logger.error(msg)
                return False
            return True
            
        except Exception as e:
            if logger:
                logger.error("Error validating required columns", e)
            return False

    @staticmethod
    def validate_data_quality(df: pd.DataFrame, logger=None) -> Dict[str, Any]:
        """Perform comprehensive data quality validation"""
        quality_report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'null_counts': {},
            'duplicate_rows': 0,
            'data_types': {},
            'numeric_stats': {},
            'validation_passed': True,
            'warnings': []
        }
        
        try:
            # Null value analysis
            null_counts = df.isnull().sum()
            for col in df.columns:
                null_count = null_counts[col]
                null_pct = (null_count / len(df)) * 100
                quality_report['null_counts'][col] = {
                    'count': int(null_count),
                    'percentage': round(null_pct, 2)
                }
                
                if null_pct > 50:  # More than 50% nulls
                    warning = f"Column '{col}' has {null_pct:.2f}% null values"
                    quality_report['warnings'].append(warning)
                    if logger:
                        logger.warning(warning)
            
            # Duplicate analysis
            quality_report['duplicate_rows'] = int(df.duplicated().sum())
            
            # Data type analysis
            for col in df.columns:
                quality_report['data_types'][col] = str(df[col].dtype)
            
            # Numeric statistics
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                quality_report['numeric_stats'][col] = {
                    'min': float(df[col].min()),
                    'max': float(df[col].max()),
                    'mean': float(df[col].mean()),
                    'std': float(df[col].std()),
                    'unique_count': int(df[col].nunique())
                }
            
            # Run specific validations
            if not DataValidator.validate_numeric_ranges(df, logger):
                quality_report['validation_passed'] = False
            
            if not DataValidator.validate_date_time(df, logger):
                quality_report['validation_passed'] = False
            
            return quality_report
            
        except Exception as e:
            if logger:
                logger.error("Error during data quality validation", e)
            quality_report['validation_passed'] = False
            return quality_report

    @staticmethod
    def log_quality_report(quality_report: Dict[str, Any], logger):
        """Log the data quality report"""
        logger.info(f"Data Quality Report:")
        logger.info(f"  Total rows: {quality_report['total_rows']}")
        logger.info(f"  Total columns: {quality_report['total_columns']}")
        logger.info(f"  Duplicate rows: {quality_report['duplicate_rows']}")
        logger.info(f"  Validation passed: {quality_report['validation_passed']}")
        
        if quality_report['warnings']:
            logger.warning(f"  Warnings: {len(quality_report['warnings'])}")
            for warning in quality_report['warnings']:
                logger.warning(f"    - {warning}") 