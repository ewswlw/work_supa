"""
dtale_manager.py - Utility module for managing dtale instances

This module provides helper functions and utilities for the dtale application,
including instance management, data optimization, and performance monitoring.
"""

import pandas as pd
import numpy as np
import dtale
from typing import Dict, List, Optional, Tuple, Any
import logging
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class DtaleInstanceManager:
    """
    Manages multiple dtale instances for different data views.
    
    Features:
    - Instance lifecycle management
    - Port management
    - Memory optimization
    - Performance monitoring
    """
    
    def __init__(self, base_port: int = 40000):
        """
        Initialize the dtale instance manager.
        
        Args:
            base_port: Base port number for dtale instances
        """
        self.base_port = base_port
        self.instances: Dict[str, dtale.app.DtaleData] = {}
        self.port_counter = 0
        self.stats: Dict[str, Dict[str, Any]] = {}
    
    def create_instance(self, name: str, df: pd.DataFrame, **kwargs) -> Optional[dtale.app.DtaleData]:
        """
        Create a new dtale instance.
        
        Args:
            name: Unique name for the instance
            df: DataFrame to display
            **kwargs: Additional arguments for dtale.show()
            
        Returns:
            dtale.app.DtaleData: dtale instance or None if failed
        """
        try:
            # Close existing instance if it exists
            if name in self.instances:
                self.close_instance(name)
            
            # Set default parameters
            default_params = {
                'host': 'localhost',
                'port': self.base_port + self.port_counter,
                'subprocess': False,
                'open_browser': False,
                'allow_cell_edits': False,
                'hide_shutdown': True,
                'hide_header_editor': True
            }
            default_params.update(kwargs)
            
            # Create instance
            instance = dtale.show(df, **default_params)
            
            # Store instance and stats
            self.instances[name] = instance
            self.stats[name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_mb': df.memory_usage(deep=True).sum() / 1024**2,
                'port': default_params['port'],
                'url': instance._url
            }
            
            self.port_counter += 1
            
            logger.info(f"Created dtale instance '{name}' at {instance._url}")
            return instance
            
        except Exception as e:
            logger.error(f"Error creating dtale instance '{name}': {e}")
            return None
    
    def close_instance(self, name: str) -> bool:
        """
        Close a dtale instance.
        
        Args:
            name: Name of the instance to close
            
        Returns:
            bool: True if closed successfully
        """
        try:
            if name in self.instances:
                self.instances[name].kill()
                del self.instances[name]
                if name in self.stats:
                    del self.stats[name]
                logger.info(f"Closed dtale instance '{name}'")
                return True
            else:
                logger.warning(f"Instance '{name}' not found")
                return False
                
        except Exception as e:
            logger.error(f"Error closing dtale instance '{name}': {e}")
            return False
    
    def close_all_instances(self):
        """Close all dtale instances."""
        for name in list(self.instances.keys()):
            self.close_instance(name)
    
    def get_instance_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a dtale instance.
        
        Args:
            name: Name of the instance
            
        Returns:
            Dict with instance information or None if not found
        """
        return self.stats.get(name)
    
    def list_instances(self) -> Dict[str, Dict[str, Any]]:
        """
        List all active dtale instances.
        
        Returns:
            Dict with instance names and their information
        """
        return self.stats.copy()

class DataOptimizer:
    """
    Optimizes pandas DataFrames for better performance with dtale.
    """
    
    @staticmethod
    def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame data types for better performance.
        
        Args:
            df: DataFrame to optimize
            
        Returns:
            Optimized DataFrame
        """
        df_opt = df.copy()
        
        # Convert object columns to category where appropriate
        for col in df_opt.columns:
            if df_opt[col].dtype == 'object':
                nunique = df_opt[col].nunique()
                if nunique < len(df_opt) * 0.5:  # Less than 50% unique values
                    try:
                        df_opt[col] = df_opt[col].astype('category')
                    except Exception:
                        pass  # Keep original dtype if conversion fails
        
        # Optimize numeric columns
        for col in df_opt.select_dtypes(include=['float64']).columns:
            try:
                if (df_opt[col].min() >= np.finfo(np.float32).min and 
                    df_opt[col].max() <= np.finfo(np.float32).max):
                    df_opt[col] = df_opt[col].astype('float32')
            except Exception:
                pass  # Keep original dtype if conversion fails
        
        # Optimize integer columns
        for col in df_opt.select_dtypes(include=['int64']).columns:
            try:
                if (df_opt[col].min() >= np.iinfo(np.int32).min and 
                    df_opt[col].max() <= np.iinfo(np.int32).max):
                    df_opt[col] = df_opt[col].astype('int32')
            except Exception:
                pass  # Keep original dtype if conversion fails
        
        return df_opt
    
    @staticmethod
    def create_smart_sample(df: pd.DataFrame, sample_size: int, 
                           priority_columns: List[str] = None) -> pd.DataFrame:
        """
        Create an intelligent sample of the data.
        
        Args:
            df: DataFrame to sample
            sample_size: Target sample size
            priority_columns: Columns to prioritize for sampling
            
        Returns:
            Sampled DataFrame
        """
        if len(df) <= sample_size:
            return df.copy()
        
        priority_columns = priority_columns or []
        sample_parts = []
        remaining_sample = sample_size
        
        # Include priority data
        for col in priority_columns:
            if col in df.columns and remaining_sample > 0:
                if df[col].dtype == 'object' or df[col].dtype.name == 'category':
                    # For categorical columns, include all unique values
                    unique_values = df[col].unique()
                    for value in unique_values[:min(len(unique_values), remaining_sample // 10)]:
                        priority_data = df[df[col] == value].head(remaining_sample // 20)
                        if not priority_data.empty:
                            sample_parts.append(priority_data)
                            remaining_sample -= len(priority_data)
                elif pd.api.types.is_numeric_dtype(df[col]):
                    # For numeric columns, include extreme values
                    high_threshold = df[col].quantile(0.95)
                    low_threshold = df[col].quantile(0.05)
                    extreme_data = df[
                        (df[col] >= high_threshold) | (df[col] <= low_threshold)
                    ]
                    if not extreme_data.empty:
                        sample_parts.append(extreme_data.head(remaining_sample // 2))
                        remaining_sample -= len(sample_parts[-1])
        
        # Random sample of remaining data
        if remaining_sample > 0:
            sampled_indices = set()
            for part in sample_parts:
                sampled_indices.update(part.index)
            
            remaining_data = df[~df.index.isin(sampled_indices)]
            if not remaining_data.empty:
                random_sample = remaining_data.sample(
                    n=min(remaining_sample, len(remaining_data)), 
                    random_state=42
                )
                sample_parts.append(random_sample)
        
        # Combine all parts
        if sample_parts:
            return pd.concat(sample_parts, ignore_index=True)
        else:
            return df.sample(n=sample_size, random_state=42)

class PerformanceMonitor:
    """
    Monitors performance metrics for dtale applications.
    """
    
    def __init__(self):
        self.metrics = {}
    
    def record_load_time(self, name: str, load_time: float):
        """Record data loading time."""
        if name not in self.metrics:
            self.metrics[name] = {}
        self.metrics[name]['load_time'] = load_time
    
    def record_memory_usage(self, name: str, memory_mb: float):
        """Record memory usage."""
        if name not in self.metrics:
            self.metrics[name] = {}
        self.metrics[name]['memory_mb'] = memory_mb
    
    def record_row_count(self, name: str, rows: int):
        """Record row count."""
        if name not in self.metrics:
            self.metrics[name] = {}
        self.metrics[name]['rows'] = rows
    
    def get_metrics(self, name: str) -> Dict[str, Any]:
        """Get performance metrics for a dataset."""
        return self.metrics.get(name, {})
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get all performance metrics."""
        return self.metrics.copy()
    
    def print_summary(self):
        """Print performance summary."""
        print("\n📊 PERFORMANCE SUMMARY")
        print("=" * 50)
        for name, metrics in self.metrics.items():
            print(f"\n🎯 {name.upper()}")
            for key, value in metrics.items():
                if key == 'load_time':
                    print(f"   ⏱️  Load Time: {value:.2f}s")
                elif key == 'memory_mb':
                    print(f"   💾 Memory: {value:.1f} MB")
                elif key == 'rows':
                    print(f"   📈 Rows: {value:,}")

def validate_data_file(file_path: str) -> Tuple[bool, str]:
    """
    Validate that a data file exists and is readable.
    
    Args:
        file_path: Path to the data file
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        path = Path(file_path)
        
        if not path.exists():
            return False, f"File not found: {file_path}"
        
        if not path.is_file():
            return False, f"Path is not a file: {file_path}"
        
        if path.suffix.lower() not in ['.parquet', '.csv', '.xlsx']:
            return False, f"Unsupported file format: {path.suffix}"
        
        # Try to read a small sample
        if path.suffix.lower() == '.parquet':
            df = pd.read_parquet(file_path, nrows=10)
        elif path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path, nrows=10)
        elif path.suffix.lower() == '.xlsx':
            df = pd.read_excel(file_path, nrows=10)
        
        if df.empty:
            return False, "File appears to be empty"
        
        return True, "File is valid"
        
    except Exception as e:
        return False, f"Error reading file: {str(e)}"

def get_column_info(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Get detailed information about DataFrame columns.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dict with column information
    """
    column_info = {}
    
    for col in df.columns:
        info = {
            'dtype': str(df[col].dtype),
            'non_null_count': df[col].count(),
            'null_count': df[col].isnull().sum(),
            'unique_count': df[col].nunique(),
            'memory_usage': df[col].memory_usage(deep=True)
        }
        
        if pd.api.types.is_numeric_dtype(df[col]):
            info.update({
                'min': df[col].min(),
                'max': df[col].max(),
                'mean': df[col].mean(),
                'std': df[col].std()
            })
        elif df[col].dtype == 'object':
            info.update({
                'sample_values': df[col].dropna().head(5).tolist()
            })
        
        column_info[col] = info
    
    return column_info

def format_memory_usage(bytes_value: int) -> str:
    """
    Format memory usage in human-readable format.
    
    Args:
        bytes_value: Memory usage in bytes
        
    Returns:
        Formatted string (e.g., "1.2 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_value < 1024:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024
    return f"{bytes_value:.1f} TB" 