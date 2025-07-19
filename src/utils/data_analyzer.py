"""
📊 DATA ANALYZER
===============
Comprehensive data analysis and reporting for pipeline tables.
Provides detailed analysis of dataframes including info, head, tail, describe,
time series date ranges, and CUSIP validation.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
from io import StringIO
import sys


class DataAnalyzer:
    """
    Comprehensive data analyzer for pipeline tables.
    
    Features:
    - DataFrame analysis (info, head, tail, describe)
    - Time series date range detection
    - CUSIP validation and orphaned CUSIP detection
    - Nice formatting for console and logging output
    - Progress indicators for large tables
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize data analyzer.
        
        Args:
            logger: Optional logger for output
        """
        self.logger = logger
        self.analysis_results = {}
        
        # Define time series columns that indicate time series data
        self.time_series_columns = ['Date', 'DATE', 'date', 'timestamp', 'Timestamp']
        
        # Define CUSIP columns across different tables
        self.cusip_columns = {
            'universe': 'CUSIP',
            'portfolio': 'CUSIP',
            'runs': 'CUSIP',
            'g_spread': 'CUSIP',
            'gspread_analytics': 'CUSIP'
        }
        
        # Define security name columns
        self.security_columns = {
            'universe': 'Security',
            'portfolio': 'SECURITY',
            'runs': 'SECURITY',
            'g_spread': 'Security',
            'gspread_analytics': 'Security'
        }
    
    def analyze_dataframe(self, df: pd.DataFrame, table_name: str, 
                         max_rows: int = 10) -> Dict[str, Any]:
        """
        Analyze a single dataframe with comprehensive statistics.
        
        Args:
            df: DataFrame to analyze
            table_name: Name of the table for reporting
            max_rows: Maximum rows to show in head/tail
            
        Returns:
            Dictionary with analysis results
        """
        if df is None or df.empty:
            return {
                'table_name': table_name,
                'empty': True,
                'message': f"Table {table_name} is empty or None"
            }
        
        # Basic info
        info_buffer = StringIO()
        df.info(buf=info_buffer, max_cols=None, memory_usage=True)
        df_info = info_buffer.getvalue()
        
        # Head and tail (limit rows for large tables)
        head_rows = min(max_rows, len(df))
        tail_rows = min(max_rows, len(df))
        
        df_head = df.head(head_rows)
        df_tail = df.tail(tail_rows)
        
        # Describe (only for numeric columns)
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            df_describe = df[numeric_cols].describe()
        else:
            df_describe = pd.DataFrame()
        
        # Time series analysis
        time_series_info = self._analyze_time_series(df, table_name)
        
        # Memory usage
        memory_usage = df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
        
        return {
            'table_name': table_name,
            'empty': False,
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'info': df_info,
            'head': df_head,
            'tail': df_tail,
            'describe': df_describe,
            'time_series': time_series_info,
            'memory_usage_mb': memory_usage,
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum()
        }
    
    def _analyze_time_series(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Analyze time series aspects of the dataframe."""
        time_series_info = {
            'is_time_series': False,
            'date_columns': [],
            'date_ranges': {},
            'total_dates': 0
        }
        
        # Find date columns
        date_columns = []
        for col in df.columns:
            if any(ts_col in col for ts_col in self.time_series_columns):
                date_columns.append(col)
        
        if date_columns:
            time_series_info['is_time_series'] = True
            time_series_info['date_columns'] = date_columns
            
            # Analyze each date column
            for date_col in date_columns:
                try:
                    # Try to parse as datetime
                    if df[date_col].dtype == 'object':
                        # Try different date formats
                        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d %H:%M:%S']:
                            try:
                                parsed_dates = pd.to_datetime(df[date_col], format=fmt, errors='coerce')
                                if parsed_dates.notna().sum() > 0:
                                    df[date_col] = parsed_dates
                                    break
                            except:
                                continue
                    
                    if pd.api.types.is_datetime64_any_dtype(df[date_col]):
                        valid_dates = df[date_col].dropna()
                        if len(valid_dates) > 0:
                            time_series_info['date_ranges'][date_col] = {
                                'start_date': valid_dates.min(),
                                'end_date': valid_dates.max(),
                                'total_dates': len(valid_dates),
                                'unique_dates': valid_dates.nunique()
                            }
                            time_series_info['total_dates'] = len(valid_dates)
                except Exception as e:
                    time_series_info['date_ranges'][date_col] = {
                        'error': f"Could not parse dates: {str(e)}"
                    }
        
        return time_series_info
    
    def format_analysis_output(self, analysis: Dict[str, Any], 
                              show_details: bool = True) -> str:
        """
        Format analysis results into a nice string output.
        
        Args:
            analysis: Analysis results from analyze_dataframe
            show_details: Whether to show detailed info/describe
            
        Returns:
            Formatted string output
        """
        if analysis.get('empty', False):
            return f"📊 {analysis['table_name'].upper()}: {analysis['message']}\n"
        
        output = []
        output.append(f"📊 {analysis['table_name'].upper()} ANALYSIS")
        output.append("=" * 60)
        
        # Basic stats
        output.append(f"Shape: {analysis['shape'][0]:,} rows × {analysis['shape'][1]} columns")
        output.append(f"Memory Usage: {analysis['memory_usage_mb']:.2f} MB")
        output.append(f"Duplicate Rows: {analysis['duplicate_rows']:,}")
        
        # Time series info
        if analysis['time_series']['is_time_series']:
            output.append(f"Time Series: YES")
            for date_col, date_info in analysis['time_series']['date_ranges'].items():
                if 'error' not in date_info:
                    start_date = date_info['start_date'].strftime('%Y-%m-%d')
                    end_date = date_info['end_date'].strftime('%Y-%m-%d')
                    output.append(f"  {date_col}: {start_date} to {end_date} ({date_info['unique_dates']:,} unique dates)")
                else:
                    output.append(f"  {date_col}: {date_info['error']}")
        else:
            output.append("Time Series: NO")
        
        # Null values summary
        null_summary = {k: v for k, v in analysis['null_counts'].items() if v > 0}
        if null_summary:
            output.append(f"Columns with nulls: {len(null_summary)}")
            for col, count in sorted(null_summary.items(), key=lambda x: x[1], reverse=True)[:5]:
                pct = (count / analysis['shape'][0]) * 100
                output.append(f"  {col}: {count:,} ({pct:.1f}%)")
        
        if show_details:
            # DataFrame info
            output.append("\n📋 DATAFRAME INFO:")
            output.append("-" * 40)
            output.append(analysis['info'])
            
            # Head
            output.append(f"\n🔝 FIRST {len(analysis['head'])} ROWS:")
            output.append("-" * 40)
            output.append(analysis['head'].to_string())
            
            # Tail
            output.append(f"\n🔚 LAST {len(analysis['tail'])} ROWS:")
            output.append("-" * 40)
            output.append(analysis['tail'].to_string())
            
            # Describe (if numeric columns exist)
            if not analysis['describe'].empty:
                output.append(f"\n📈 NUMERIC COLUMNS SUMMARY:")
                output.append("-" * 40)
                output.append(analysis['describe'].to_string())
        
        output.append("=" * 60)
        return "\n".join(output)
    
    def analyze_all_tables(self, table_data: Dict[str, pd.DataFrame],
                          show_details: bool = True) -> str:
        """
        Analyze all tables and return formatted output.
        
        Args:
            table_data: Dictionary of {table_name: dataframe}
            show_details: Whether to show detailed analysis
            
        Returns:
            Formatted string with all analysis results
        """
        output = []
        output.append("🎯 COMPREHENSIVE DATA ANALYSIS")
        output.append("=" * 80)
        output.append(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append(f"Tables Analyzed: {len(table_data)}")
        output.append("=" * 80)
        
        for i, (table_name, df) in enumerate(table_data.items(), 1):
            output.append(f"\n[{i}/{len(table_data)}] Analyzing {table_name}...")
            
            # Analyze the dataframe
            analysis = self.analyze_dataframe(df, table_name)
            self.analysis_results[table_name] = analysis
            
            # Format and add to output
            formatted_analysis = self.format_analysis_output(analysis, show_details)
            output.append(formatted_analysis)
        
        # Summary
        output.append("\n📊 ANALYSIS SUMMARY")
        output.append("=" * 80)
        total_rows = sum(analysis['shape'][0] for analysis in self.analysis_results.values() if not analysis.get('empty', False))
        total_memory = sum(analysis['memory_usage_mb'] for analysis in self.analysis_results.values() if not analysis.get('empty', False))
        time_series_tables = sum(1 for analysis in self.analysis_results.values() if analysis['time_series']['is_time_series'])
        
        output.append(f"Total Rows Across All Tables: {total_rows:,}")
        output.append(f"Total Memory Usage: {total_memory:.2f} MB")
        output.append(f"Time Series Tables: {time_series_tables}")
        output.append(f"Tables Analyzed: {len(self.analysis_results)}")
        
        return "\n".join(output)


class CUSIPValidator:
    """
    Validates CUSIPs across all tables and detects orphaned CUSIPs.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize CUSIP validator.
        
        Args:
            logger: Optional logger for output
        """
        self.logger = logger
        self.orphaned_cusips = {}
        
    def validate_cusips(self, table_data: Dict[str, pd.DataFrame], 
                       universe_table: str = 'universe') -> Dict[str, Any]:
        """
        Validate CUSIPs across all tables against universe.
        
        Args:
            table_data: Dictionary of {table_name: dataframe}
            universe_table: Name of the universe table
            
        Returns:
            Dictionary with validation results
        """
        if universe_table not in table_data:
            return {
                'error': f"Universe table '{universe_table}' not found in data",
                'orphaned_cusips': {},
                'summary': {}
            }
        
        universe_df = table_data[universe_table]
        if universe_df is None or universe_df.empty:
            return {
                'error': "Universe table is empty",
                'orphaned_cusips': {},
                'summary': {}
            }
        
        # Get universe CUSIPs
        universe_cusips = set()
        if 'CUSIP' in universe_df.columns:
            universe_cusips = set(universe_df['CUSIP'].dropna().unique())
        
        # Find orphaned CUSIPs in other tables
        orphaned_results = {}
        summary = {
            'universe_cusips': len(universe_cusips),
            'tables_checked': 0,
            'total_orphaned_instances': 0,
            'unique_orphaned_cusips': set()
        }
        
        for table_name, df in table_data.items():
            if table_name == universe_table or df is None or df.empty:
                continue
            
            # Find CUSIP columns (handle variations like CUSIP_1, CUSIP_2)
            cusip_columns = [col for col in df.columns if 'CUSIP' in col.upper()]
            if not cusip_columns:
                continue
            
            summary['tables_checked'] += 1
            
            # Get all CUSIPs from all CUSIP columns in this table
            table_cusips = set()
            for cusip_col in cusip_columns:
                table_cusips.update(df[cusip_col].dropna().unique())
            
            orphaned_in_table = [cusip for cusip in table_cusips if cusip not in universe_cusips]
            
            if orphaned_in_table:
                # Get security names for orphaned CUSIPs
                orphaned_details = []
                for cusip in orphaned_in_table:
                    # Find which CUSIP column(s) contain this CUSIP
                    cusip_instances = 0
                    security_name = "Unknown"
                    
                    for cusip_col in cusip_columns:
                        cusip_data = df[df[cusip_col] == cusip]
                        if len(cusip_data) > 0:
                            cusip_instances += len(cusip_data)
                            
                            # Get security name if available
                            if security_name == "Unknown":
                                security_col = None
                                # Check for standard security columns first
                                for col in ['Security', 'SECURITY', 'security']:
                                    if col in df.columns:
                                        security_col = col
                                        break
                                
                                # For g_spread table, check Security_1 and Security_2
                                if security_col is None and table_name == 'g_spread':
                                    # Check which CUSIP column this CUSIP is in
                                    if cusip_col == 'CUSIP_1' and 'Security_1' in df.columns:
                                        security_col = 'Security_1'
                                    elif cusip_col == 'CUSIP_2' and 'Security_2' in df.columns:
                                        security_col = 'Security_2'
                                
                                if security_col:
                                    security_name = cusip_data[security_col].iloc[0]
                    
                    orphaned_details.append({
                        'cusip': cusip,
                        'security_name': security_name,
                        'count': cusip_instances
                    })
                
                orphaned_results[table_name] = {
                    'orphaned_cusips': orphaned_details,
                    'total_instances': sum(detail['count'] for detail in orphaned_details),
                    'unique_cusips': len(orphaned_in_table)
                }
                
                summary['total_orphaned_instances'] += orphaned_results[table_name]['total_instances']
                summary['unique_orphaned_cusips'].update(orphaned_in_table)
        
        return {
            'orphaned_cusips': orphaned_results,
            'summary': summary,
            'universe_cusips': universe_cusips
        }
    
    def validate_cusips_latest_universe(self, table_data: Dict[str, pd.DataFrame], 
                                       universe_table: str = 'universe') -> Dict[str, Any]:
        """
        Validate CUSIPs across all tables against only the most recent date in the universe table.
        For time series tables, also check only their latest date against universe's latest date.
        
        Args:
            table_data: Dictionary of {table_name: dataframe}
            universe_table: Name of the universe table
            
        Returns:
            Dictionary with validation results against latest universe
        """
        if universe_table not in table_data:
            return {
                'error': f"Universe table '{universe_table}' not found in data",
                'orphaned_cusips': {},
                'summary': {}
            }
        
        universe_df = table_data[universe_table]
        if universe_df is None or universe_df.empty:
            return {
                'error': "Universe table is empty",
                'orphaned_cusips': {},
                'summary': {}
            }
        
        # Find the most recent date in universe
        latest_date = None
        if 'Date' in universe_df.columns:
            latest_date = universe_df['Date'].max()
            latest_universe_df = universe_df[universe_df['Date'] == latest_date]
        else:
            # If no Date column, use all universe data
            latest_universe_df = universe_df
        
        # Get CUSIPs from the most recent universe
        latest_universe_cusips = set()
        if 'CUSIP' in latest_universe_df.columns:
            latest_universe_cusips = set(latest_universe_df['CUSIP'].dropna().unique())
        
        # Find orphaned CUSIPs in other tables
        orphaned_results = {}
        table_latest_dates = {}  # Store latest dates for each table
        summary = {
            'universe_cusips': len(latest_universe_cusips),
            'latest_date': str(latest_date) if latest_date is not None else 'N/A',
            'tables_checked': 0,
            'total_orphaned_instances': 0,
            'unique_orphaned_cusips': set()
        }
        
        for table_name, df in table_data.items():
            if table_name == universe_table or df is None or df.empty:
                continue
            
            # Find CUSIP columns (handle variations like CUSIP_1, CUSIP_2)
            cusip_columns = [col for col in df.columns if 'CUSIP' in col.upper()]
            if not cusip_columns:
                continue
            
            summary['tables_checked'] += 1
            
            # For time series tables, get only the latest date data
            if 'Date' in df.columns:
                table_latest_date = df['Date'].max()
                latest_df = df[df['Date'] == table_latest_date]
                table_latest_dates[table_name] = table_latest_date
                if self.logger:
                    self.logger.info(f"Table {table_name}: Using latest date {table_latest_date} (vs universe latest {latest_date})")
            else:
                # For non-time series tables, use all data
                latest_df = df
                table_latest_dates[table_name] = "All Data (Non-Time Series)"
                if self.logger:
                    self.logger.info(f"Table {table_name}: Using all data (non-time series)")
            
            # Get all CUSIPs from all CUSIP columns in this table (latest date only for time series)
            table_cusips = set()
            for cusip_col in cusip_columns:
                table_cusips.update(latest_df[cusip_col].dropna().unique())
            
            orphaned_in_table = [cusip for cusip in table_cusips if cusip not in latest_universe_cusips]
            
            if orphaned_in_table:
                # Get security names for orphaned CUSIPs
                orphaned_details = []
                for cusip in orphaned_in_table:
                    # Find which CUSIP column(s) contain this CUSIP
                    cusip_instances = 0
                    security_name = "Unknown"
                    
                    for cusip_col in cusip_columns:
                        cusip_data = latest_df[latest_df[cusip_col] == cusip]
                        if len(cusip_data) > 0:
                            cusip_instances += len(cusip_data)
                            
                            # Get security name if available
                            if security_name == "Unknown":
                                security_col = None
                                # Check for standard security columns first
                                for col in ['Security', 'SECURITY', 'security']:
                                    if col in df.columns:
                                        security_col = col
                                        break
                                
                                # For g_spread table, check Security_1 and Security_2
                                if security_col is None and table_name == 'g_spread':
                                    # Check which CUSIP column this CUSIP is in
                                    if cusip_col == 'CUSIP_1' and 'Security_1' in df.columns:
                                        security_col = 'Security_1'
                                    elif cusip_col == 'CUSIP_2' and 'Security_2' in df.columns:
                                        security_col = 'Security_2'
                                
                                if security_col:
                                    security_name = cusip_data[security_col].iloc[0]
                    
                    orphaned_details.append({
                        'cusip': cusip,
                        'security_name': security_name,
                        'count': cusip_instances
                    })
                
                orphaned_results[table_name] = {
                    'orphaned_cusips': orphaned_details,
                    'total_instances': sum(detail['count'] for detail in orphaned_details),
                    'unique_cusips': len(orphaned_in_table)
                }
                
                summary['total_orphaned_instances'] += orphaned_results[table_name]['total_instances']
                summary['unique_orphaned_cusips'].update(orphaned_in_table)
        
        return {
            'orphaned_cusips': orphaned_results,
            'summary': summary,
            'universe_cusips': latest_universe_cusips,
            'table_latest_dates': table_latest_dates
        }
    
    def format_validation_output(self, validation_results: Dict[str, Any]) -> str:
        """
        Format CUSIP validation results into a nice string output.
        
        Args:
            validation_results: Results from validate_cusips
            
        Returns:
            Formatted string output
        """
        if 'error' in validation_results:
            return f"❌ CUSIP VALIDATION ERROR: {validation_results['error']}\n"
        
        output = []
        output.append("🔍 CUSIP VALIDATION RESULTS")
        output.append("=" * 60)
        
        summary = validation_results['summary']
        output.append(f"Universe CUSIPs: {summary['universe_cusips']:,}")
        output.append(f"Tables Checked: {summary['tables_checked']}")
        output.append(f"Unique Orphaned CUSIPs: {len(summary['unique_orphaned_cusips'])}")
        output.append(f"Total Orphaned Instances: {summary['total_orphaned_instances']:,}")
        
        if validation_results['orphaned_cusips']:
            output.append("\n📋 ORPHANED CUSIPS BY TABLE:")
            output.append("-" * 60)
            
            for table_name, table_results in validation_results['orphaned_cusips'].items():
                output.append(f"\n🔴 {table_name.upper()}:")
                output.append(f"   Unique Orphaned CUSIPs: {table_results['unique_cusips']}")
                output.append(f"   Total Instances: {table_results['total_instances']:,}")
                
                # Show details for each orphaned CUSIP
                for detail in table_results['orphaned_cusips']:
                    output.append(f"   • {detail['cusip']} - {detail['security_name']} ({detail['count']} instances)")
        else:
            output.append("\n✅ No orphaned CUSIPs found!")
        
        output.append("=" * 60)
        return "\n".join(output)
    
    def format_latest_universe_validation_output(self, validation_results: Dict[str, Any]) -> str:
        """
        Format CUSIP validation results against latest universe into a nice string output.
        
        Args:
            validation_results: Results from validate_cusips_latest_universe
            
        Returns:
            Formatted string output
        """
        if 'error' in validation_results:
            return f"❌ LATEST UNIVERSE CUSIP VALIDATION ERROR: {validation_results['error']}\n"
        
        output = []
        output.append("🔍 CUSIP VALIDATION RESULTS (LATEST UNIVERSE DATE)")
        output.append("=" * 60)
        
        summary = validation_results['summary']
        output.append(f"Universe CUSIPs (Latest Date): {summary['universe_cusips']:,}")
        output.append(f"Latest Universe Date: {summary['latest_date']}")
        output.append(f"Tables Checked: {summary['tables_checked']}")
        output.append(f"Unique Orphaned CUSIPs: {len(summary['unique_orphaned_cusips'])}")
        output.append(f"Total Orphaned Instances: {summary['total_orphaned_instances']:,}")
        
        if validation_results['orphaned_cusips']:
            output.append("\n📋 ORPHANED CUSIPS BY TABLE:")
            output.append("-" * 60)
            
            for table_name, table_results in validation_results['orphaned_cusips'].items():
                # Get the latest date for this table
                table_latest_date = "N/A"
                if 'table_latest_dates' in validation_results and table_name in validation_results['table_latest_dates']:
                    table_latest_date = str(validation_results['table_latest_dates'][table_name])
                
                output.append(f"\n🔴 {table_name.upper()}:")
                output.append(f"   Latest Table Date: {table_latest_date}")
                output.append(f"   Unique Orphaned CUSIPs: {table_results['unique_cusips']}")
                output.append(f"   Total Instances: {table_results['total_instances']:,}")
                
                # Show details for each orphaned CUSIP
                for detail in table_results['orphaned_cusips']:
                    output.append(f"   • {detail['cusip']} - {detail['security_name']} ({detail['count']} instances)")
        else:
            output.append("\n✅ No orphaned CUSIPs found against latest universe!")
        
        output.append("=" * 60)
        return "\n".join(output)


def analyze_pipeline_data(table_data: Dict[str, pd.DataFrame], 
                         logger: Optional[logging.Logger] = None,
                         show_details: bool = True) -> str:
    """
    Convenience function to run complete data analysis and CUSIP validation.
    
    Args:
        table_data: Dictionary of {table_name: dataframe}
        logger: Optional logger for output
        show_details: Whether to show detailed analysis
        
    Returns:
        Complete formatted analysis output
    """
    output = []
    
    # Data analysis
    analyzer = DataAnalyzer(logger)
    analysis_output = analyzer.analyze_all_tables(table_data, show_details)
    output.append(analysis_output)
    
    # CUSIP validation (all universe dates)
    validator = CUSIPValidator(logger)
    validation_results = validator.validate_cusips(table_data)
    validation_output = validator.format_validation_output(validation_results)
    output.append(validation_output)
    
    # CUSIP validation (latest universe date only)
    latest_validation_results = validator.validate_cusips_latest_universe(table_data)
    latest_validation_output = validator.format_latest_universe_validation_output(latest_validation_results)
    output.append(latest_validation_output)
    
    return "\n\n".join(output) 