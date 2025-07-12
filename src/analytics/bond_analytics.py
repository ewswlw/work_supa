"""
Bond Analytics Module
===================

Specialized analytics for bond g-spread data analysis using dtale.
This module consolidates bond-specific filtering and analysis functionality.

Features:
- Bond-specific data filtering (CAD-only, same-sector, portfolio, etc.)
- Integration with dtale_manager utilities
- Smart sampling for performance optimization
- Comprehensive view management
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
import logging
from pathlib import Path

try:
    from ..utils.dtale_manager import DtaleInstanceManager, DataOptimizer, PerformanceMonitor
except ImportError:
    # For direct execution, add parent directory to path
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from utils.dtale_manager import DtaleInstanceManager, DataOptimizer, PerformanceMonitor

logger = logging.getLogger(__name__)

class BondAnalytics:
    """
    Bond-specific analytics using dtale for interactive data exploration.
    
    This class provides specialized filtering and analysis capabilities
    for bond g-spread data, leveraging the existing dtale infrastructure.
    """
    
    def __init__(self, data_path: str = "historical g spread/bond_z.parquet", 
                 sample_size: int = 25000, base_port: int = 40000):
        """
        Initialize bond analytics.
        
        Args:
            data_path: Path to bond_z.parquet file
            sample_size: Number of rows to sample for performance
            base_port: Base port for dtale instances
        """
        self.data_path = data_path
        self.sample_size = sample_size
        self.base_port = base_port
        
        # Core components
        self.dtale_manager = DtaleInstanceManager(base_port)
        self.performance_monitor = PerformanceMonitor()
        
        # Data storage
        self.df_full: Optional[pd.DataFrame] = None
        self.df_sample: Optional[pd.DataFrame] = None
        
        # Analytics configuration
        self.view_definitions = {
            'all': {
                'name': 'All Data',
                'description': 'Full sample of bond g-spread data',
                'filter_func': self._filter_all
            },
            'cad-only': {
                'name': 'CAD Only',
                'description': 'CAD-denominated bonds only',
                'filter_func': self._filter_cad_only
            },
            'same-sector': {
                'name': 'Same Sector',
                'description': 'Bonds in the same sector',
                'filter_func': self._filter_same_sector
            },
            'same-ticker': {
                'name': 'Same Ticker',
                'description': 'Bonds with the same ticker',
                'filter_func': self._filter_same_ticker
            },
            'tradeable': {
                'name': 'Tradeable Bonds',
                'description': 'Bonds with available trading data (bid/offer)',
                'filter_func': self._filter_tradeable
            },
            'liquid': {
                'name': 'Liquid Markets',
                'description': 'Bonds with active bid/offer spreads and size',
                'filter_func': self._filter_liquid
            },
            'cross-currency': {
                'name': 'Cross-Currency',
                'description': 'Bonds with cross-currency exposure',
                'filter_func': self._filter_cross_currency
            }
        }
    
    @property
    def stats(self) -> Dict:
        """Get current data statistics."""
        if self.df_full is None:
            return {'total_rows': 0, 'total_columns': 0, 'memory_usage_mb': 0.0}
        
        return {
            'total_rows': len(self.df_full),
            'total_columns': len(self.df_full.columns),
            'memory_usage_mb': self.df_full.memory_usage(deep=True).sum() / 1024 ** 2,
            'sample_size': len(self.df_sample) if self.df_sample is not None else 0
        }
    
    def load_data(self) -> bool:
        """Load and prepare bond data for analysis."""
        try:
            logger.info(f"Loading bond data from: {self.data_path}")
            
            # Load full dataset
            self.df_full = pd.read_parquet(self.data_path)
            
            # Record performance metrics
            self.performance_monitor.record_row_count('full_dataset', len(self.df_full))
            self.performance_monitor.record_memory_usage('full_dataset', 
                                                       self.df_full.memory_usage(deep=True).sum() / 1024**2)
            
            # Create optimized sample
            logger.info("Creating optimized sample...")
            priority_columns = ['Z_Score', 'Best Bid_runs1', 'Currency_1', 'Currency_2']
            self.df_sample = DataOptimizer.create_smart_sample(
                self.df_full, 
                self.sample_size, 
                priority_columns
            )
            
            # Optimize data types
            self.df_sample = DataOptimizer.optimize_dtypes(self.df_sample)
            
            logger.info(f"✅ Data loaded successfully: {self.stats}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            self.df_full = None
            self.df_sample = None
            return False
    
    def create_view(self, view_name: str, **kwargs) -> Optional:
        """
        Create a dtale view for the specified bond filter.
        
        Args:
            view_name: Name of the view to create
            **kwargs: Additional arguments for dtale.show()
            
        Returns:
            dtale instance or None if failed
        """
        if self.df_sample is None:
            logger.error("Data not loaded. Call load_data() first.")
            return None
        
        if view_name not in self.view_definitions:
            logger.error(f"Unknown view: {view_name}")
            return None
        
        try:
            # Apply the filter
            view_config = self.view_definitions[view_name]
            df_filtered = view_config['filter_func'](self.df_sample)
            
            if df_filtered.empty:
                logger.warning(f"Filter '{view_name}' returned empty dataset")
                return None
            
            # Create dtale instance
            instance = self.dtale_manager.create_instance(
                name=view_name,
                df=df_filtered,
                **kwargs
            )
            
            logger.info(f"✅ Created view '{view_name}' with {len(df_filtered)} rows")
            return instance
            
        except Exception as e:
            logger.error(f"Failed to create view '{view_name}': {e}")
            return None
    
    def create_all_views(self) -> Dict[str, bool]:
        """Create all available views."""
        results = {}
        
        for view_name in self.view_definitions:
            logger.info(f"Creating view: {view_name}")
            instance = self.create_view(view_name)
            results[view_name] = instance is not None
            
        return results
    
    def get_view_info(self, view_name: str) -> Optional[Dict]:
        """Get information about a specific view."""
        if view_name not in self.view_definitions:
            return None
        
        view_config = self.view_definitions[view_name].copy()
        
        # Add runtime information if available
        instance_info = self.dtale_manager.get_instance_info(view_name)
        if instance_info:
            view_config.update(instance_info)
        
        return view_config
    
    def list_views(self) -> List[Dict]:
        """List all available views with their configurations."""
        return [
            {
                'name': view_name,
                'display_name': config['name'],
                'description': config['description'],
                'active': view_name in self.dtale_manager.instances
            }
            for view_name, config in self.view_definitions.items()
        ]
    
    def close_view(self, view_name: str) -> bool:
        """Close a specific view."""
        return self.dtale_manager.close_instance(view_name)
    
    def close_all_views(self):
        """Close all views."""
        self.dtale_manager.close_all_instances()
    
    def get_performance_summary(self) -> Dict:
        """Get performance metrics summary."""
        return self.performance_monitor.get_all_metrics()
    
    # Bond-specific filter methods
    def _filter_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return all data (no filtering)."""
        return df
    
    def _filter_cad_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for CAD-denominated bonds only."""
        return df[(df['Currency_1'] == 'CAD') & (df['Currency_2'] == 'CAD')]
    
    def _filter_same_sector(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for bonds in the same sector."""
        return df[df['Custom_Sector_1'] == df['Custom_Sector_2']]
    
    def _filter_same_ticker(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for bonds with the same ticker."""
        return df[
            (df['Currency_1'] == 'CAD') & 
            (df['Currency_2'] == 'CAD') & 
            (df['Ticker_1'] == df['Ticker_2'])
        ]
    
    def _filter_tradeable(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for bonds with available trading data."""
        # Bonds that have either runs1 or runs2 data (bid or offer available)
        tradeable_mask = (
            df['Best Bid_runs1'].notna() | 
            df['Best Offer_runs1'].notna() |
            df['Best Bid_runs2'].notna() | 
            df['Best Offer_runs2'].notna()
        )
        return df[tradeable_mask]
    
    def _filter_liquid(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for bonds with active liquid markets."""
        # Bonds that have both bid and offer data with size information
        liquid_mask = (
            (df['Best Bid_runs1'].notna() & df['Best Offer_runs1'].notna() & df['Size @ Best Bid_runs1'].notna()) |
            (df['Best Bid_runs2'].notna() & df['Best Offer_runs2'].notna() & df['Size @ Best Bid_runs2'].notna())
        )
        return df[liquid_mask]
    
    def _filter_cross_currency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for cross-currency bonds."""
        return df[df['Currency_1'] != df['Currency_2']]

# Legacy compatibility - alias to old name
BondDtaleApp = BondAnalytics 