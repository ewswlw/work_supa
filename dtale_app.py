#!/usr/bin/env python3
"""
High-Performance dtale App for Bond G-Spread Analytics
=====================================================

MVP dtale application for exploring bond_z.parquet data with:
- Smart sampling for 2M+ row datasets
- Multiple preset views (CAD bonds, same sector, portfolio, etc.)
- Performance optimizations for team collaboration
- Excel-like interface with advanced filtering

Usage:
    poetry run python dtale_app.py
    poetry run python dtale_app.py --sample-size 50000
    poetry run python dtale_app.py --view cad-only
    poetry run python dtale_app.py --port 40000

Author: Trading Analytics Team
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
import dtale
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, Optional, Tuple, List
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BondDtaleApp:
    """
    High-performance dtale application for bond G-spread analytics.
    
    Features:
    - Smart sampling for large datasets (2M+ rows)
    - Multiple preset views for different analysis needs
    - Performance optimizations for team collaboration
    - Comprehensive data validation and error handling
    """
    
    def __init__(self, data_path: str = "historical g spread/bond_z.parquet", 
                 sample_size: int = 100000, port: int = 40000):
        """
        Initialize the Bond dtale application.
        
        Args:
            data_path: Path to bond_z.parquet file
            sample_size: Number of rows to sample for performance
            port: Port number for dtale server
        """
        self.data_path = Path(data_path)
        self.sample_size = sample_size
        self.port = port
        self.df_full = None
        self.df_sample = None
        self.dtale_instances = {}
        self.stats = {}
        
        # Preset views configuration
        self.views = {
            'all': {
                'name': 'All Data (Sampled)',
                'description': 'Complete dataset with smart sampling',
                'filter_func': None
            },
            'cad-only': {
                'name': 'CAD Bonds Only',
                'description': 'Canadian dollar bonds only',
                'filter_func': self._filter_cad_only
            },
            'same-sector': {
                'name': 'Same Sector Pairs',
                'description': 'Bond pairs within same sector',
                'filter_func': self._filter_same_sector
            },
            'same-ticker': {
                'name': 'Same Ticker Pairs',
                'description': 'Bond pairs from same issuer',
                'filter_func': self._filter_same_ticker
            },
            'portfolio': {
                'name': 'Portfolio Holdings',
                'description': 'Bonds we own (Own? = 1)',
                'filter_func': self._filter_portfolio
            },
            'executable': {
                'name': 'Executable Trades',
                'description': 'Liquid bonds with tight spreads',
                'filter_func': self._filter_executable
            },
            'cross-currency': {
                'name': 'CAD/USD Cross-Currency',
                'description': 'CAD vs USD bond comparisons',
                'filter_func': self._filter_cross_currency
            }
        }
    
    def load_data(self) -> bool:
        """
        Load and prepare bond data with performance optimizations.
        
        Returns:
            bool: True if data loaded successfully
        """
        try:
            logger.info(f"Loading data from {self.data_path}")
            
            # Check if file exists
            if not self.data_path.exists():
                logger.error(f"Data file not found: {self.data_path}")
                return False
            
            # Load full dataset
            start_time = datetime.now()
            self.df_full = pd.read_parquet(self.data_path)
            load_time = (datetime.now() - start_time).total_seconds()
            
            # Calculate statistics
            self.stats = {
                'total_rows': len(self.df_full),
                'total_columns': len(self.df_full.columns),
                'memory_usage_mb': self.df_full.memory_usage(deep=True).sum() / 1024**2,
                'load_time_seconds': load_time
            }
            
            logger.info(f"✅ Data loaded: {self.stats['total_rows']:,} rows × {self.stats['total_columns']} columns")
            logger.info(f"📊 Memory usage: {self.stats['memory_usage_mb']:.1f} MB")
            logger.info(f"⏱️ Load time: {self.stats['load_time_seconds']:.2f} seconds")
            
            # Create smart sample
            self._create_smart_sample()
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def _create_smart_sample(self):
        """Create an intelligent sample of the data for performance."""
        if len(self.df_full) <= self.sample_size:
            self.df_sample = self.df_full.copy()
            logger.info("📊 Using full dataset (under sample size limit)")
            return
        
        # Smart sampling strategy:
        # 1. Include all extreme Z-scores (top/bottom 5%)
        # 2. Include all portfolio holdings (Own? = 1)
        # 3. Random sample of the rest
        
        logger.info(f"🎯 Creating smart sample of {self.sample_size:,} rows from {len(self.df_full):,} total")
        
        # Get extreme Z-scores
        z_threshold_high = self.df_full['Z_Score'].quantile(0.95)
        z_threshold_low = self.df_full['Z_Score'].quantile(0.05)
        extreme_z = self.df_full[
            (self.df_full['Z_Score'] >= z_threshold_high) | 
            (self.df_full['Z_Score'] <= z_threshold_low)
        ]
        
        # Get portfolio holdings
        portfolio_holdings = self.df_full[self.df_full['Own?'] == 1] if 'Own?' in self.df_full.columns else pd.DataFrame()
        
        # Calculate remaining sample size
        extreme_count = len(extreme_z)
        portfolio_count = len(portfolio_holdings)
        remaining_sample = max(0, self.sample_size - extreme_count - portfolio_count)
        
        # Random sample of remaining data
        remaining_data = self.df_full[
            ~self.df_full.index.isin(extreme_z.index) & 
            ~self.df_full.index.isin(portfolio_holdings.index)
        ]
        
        if len(remaining_data) > 0 and remaining_sample > 0:
            random_sample = remaining_data.sample(n=min(remaining_sample, len(remaining_data)), random_state=42)
        else:
            random_sample = pd.DataFrame()
        
        # Combine samples
        sample_parts = [df for df in [extreme_z, portfolio_holdings, random_sample] if not df.empty]
        if sample_parts:
            self.df_sample = pd.concat(sample_parts, ignore_index=True)
        else:
            self.df_sample = self.df_full.sample(n=self.sample_size, random_state=42)
        
        # Sort by absolute Z-score for better initial view
        self.df_sample['abs_z_score'] = abs(self.df_sample['Z_Score'])
        self.df_sample = self.df_sample.sort_values('abs_z_score', ascending=False).drop('abs_z_score', axis=1)
        
        logger.info(f"✅ Smart sample created:")
        logger.info(f"   • Extreme Z-scores: {extreme_count:,} rows")
        logger.info(f"   • Portfolio holdings: {portfolio_count:,} rows")
        logger.info(f"   • Random sample: {len(random_sample):,} rows")
        logger.info(f"   • Total sample: {len(self.df_sample):,} rows")
    
    def _filter_cad_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for CAD bonds only."""
        return df[
            (df['Currency_1'] == 'CAD') & 
            (df['Currency_2'] == 'CAD')
        ].copy()
    
    def _filter_same_sector(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for same sector pairs."""
        return df[
            df['Custom_Sector_1'] == df['Custom_Sector_2']
        ].copy()
    
    def _filter_same_ticker(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for same ticker pairs."""
        return df[
            (df['Currency_1'] == 'CAD') & 
            (df['Currency_2'] == 'CAD') &
            (df['Ticker_1'] == df['Ticker_2'])
        ].copy()
    
    def _filter_portfolio(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for portfolio holdings."""
        if 'Own?' not in df.columns:
            logger.warning("'Own?' column not found, returning empty DataFrame")
            return pd.DataFrame()
        return df[df['Own?'] == 1].copy()
    
    def _filter_executable(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for executable trades (liquid bonds with tight spreads)."""
        required_cols = ['Size @ Best Offer_runs1', 'Size @ Best Bid_runs2', 'Bid/Offer_runs1', 'Bid/Offer_runs2']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing columns for executable filter: {missing_cols}")
            return df.copy()
        
        return df[
            (df['Currency_1'] == 'CAD') & 
            (df['Currency_2'] == 'CAD') &
            (df['Own?'] == 1) &
            (df['Size @ Best Offer_runs1'] > 2000000) &
            (df['Size @ Best Bid_runs2'] > 2000000) &
            (df['Bid/Offer_runs1'] < 3) &
            (df['Bid/Offer_runs2'] < 3)
        ].copy()
    
    def _filter_cross_currency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter for CAD/USD cross-currency pairs."""
        return df[
            (df['Currency_1'] == 'CAD') & 
            (df['Currency_2'] == 'USD') &
            (df['Custom_Sector_1'] == df['Custom_Sector_2']) &
            (df['Ticker_1'] == df['Ticker_2']) &
            (df['Yrs_Mat_Bucket_1'] == df['Yrs_Mat_Bucket_2'])
        ].copy()
    
    def create_view(self, view_name: str) -> Optional[dtale.app.DtaleData]:
        """
        Create a dtale view for the specified view type.
        
        Args:
            view_name: Name of the view to create
            
        Returns:
            dtale.app.DtaleData: dtale instance or None if failed
        """
        if view_name not in self.views:
            logger.error(f"Unknown view: {view_name}")
            return None
        
        try:
            view_config = self.views[view_name]
            logger.info(f"🎨 Creating view: {view_config['name']}")
            
            # Apply filter if specified
            if view_config['filter_func']:
                df_view = view_config['filter_func'](self.df_sample)
                if df_view.empty:
                    logger.warning(f"Filter resulted in empty dataset for view: {view_name}")
                    return None
            else:
                df_view = self.df_sample.copy()
            
            # Optimize data types for performance
            df_view = self._optimize_dtypes(df_view)
            
            # Create dtale instance
            dtale_instance = dtale.show(
                df_view,
                host='localhost',
                port=self.port,
                subprocess=False,
                open_browser=False,
                allow_cell_edits=False,  # Read-only as requested
                hide_shutdown=True,
                hide_header_editor=True
            )
            
            self.dtale_instances[view_name] = dtale_instance
            
            logger.info(f"✅ View created: {view_config['name']}")
            logger.info(f"   • Rows: {len(df_view):,}")
            logger.info(f"   • URL: {dtale_instance._url}")
            
            return dtale_instance
            
        except Exception as e:
            logger.error(f"Error creating view {view_name}: {e}")
            return None
    
    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for better performance."""
        df_opt = df.copy()
        
        # Convert object columns to category where appropriate
        for col in df_opt.columns:
            if df_opt[col].dtype == 'object':
                nunique = df_opt[col].nunique()
                if nunique < len(df_opt) * 0.5:  # Less than 50% unique values
                    df_opt[col] = df_opt[col].astype('category')
        
        # Optimize numeric columns
        for col in df_opt.select_dtypes(include=['float64']).columns:
            if df_opt[col].min() >= np.finfo(np.float32).min and df_opt[col].max() <= np.finfo(np.float32).max:
                df_opt[col] = df_opt[col].astype('float32')
        
        return df_opt
    
    def launch_dashboard(self, view_name: str = 'all') -> bool:
        """
        Launch the dtale dashboard with the specified view.
        
        Args:
            view_name: Name of the view to launch
            
        Returns:
            bool: True if launched successfully
        """
        if not self.df_sample is not None:
            logger.error("Data not loaded. Call load_data() first.")
            return False
        
        try:
            # Create the requested view
            dtale_instance = self.create_view(view_name)
            if not dtale_instance:
                return False
            
            # Print dashboard information
            self._print_dashboard_info(view_name)
            
            return True
            
        except Exception as e:
            logger.error(f"Error launching dashboard: {e}")
            return False
    
    def _print_dashboard_info(self, view_name: str):
        """Print dashboard information and instructions."""
        view_config = self.views[view_name]
        dtale_instance = self.dtale_instances[view_name]
        
        print("\n" + "="*80)
        print("🚀 BOND G-SPREAD ANALYTICS DASHBOARD")
        print("="*80)
        print(f"📊 View: {view_config['name']}")
        print(f"📝 Description: {view_config['description']}")
        print(f"🔗 URL: {dtale_instance._url}")
        print(f"🎯 Rows: {len(self.df_sample):,} (sampled from {self.stats['total_rows']:,} total)")
        print(f"📈 Columns: {self.stats['total_columns']}")
        print(f"💾 Memory: {self.stats['memory_usage_mb']:.1f} MB")
        print("\n📋 AVAILABLE VIEWS:")
        for key, config in self.views.items():
            status = "🟢 ACTIVE" if key == view_name else "⚪ Available"
            print(f"   {status} {key}: {config['name']}")
        
        print("\n🎯 QUICK ACTIONS:")
        print("   • Sort by Z_Score (desc) to see most extreme spreads")
        print("   • Filter by Custom_Sector_1/2 to focus on specific sectors")
        print("   • Filter by Own? = 1 to see portfolio holdings")
        print("   • Use Size @ Best Offer/Bid columns for liquidity analysis")
        print("   • Export filtered data using dtale's export features")
        
        print("\n🔄 TO CHANGE VIEWS:")
        print("   • Stop this app (Ctrl+C)")
        print("   • Run with --view option: python dtale_app.py --view cad-only")
        
        print("\n⚠️  PERFORMANCE NOTES:")
        print(f"   • Data is sampled to {self.sample_size:,} rows for performance")
        print("   • Includes all extreme Z-scores and portfolio holdings")
        print("   • Use filters to focus on specific analysis areas")
        print("="*80)
    
    def list_views(self):
        """List all available views."""
        print("\n📋 AVAILABLE VIEWS:")
        print("="*60)
        for key, config in self.views.items():
            print(f"🎯 {key.upper()}")
            print(f"   Name: {config['name']}")
            print(f"   Description: {config['description']}")
            print()

def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(
        description="High-Performance dtale App for Bond G-Spread Analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    poetry run python dtale_app.py                     # Launch with default settings
    poetry run python dtale_app.py --view cad-only     # Launch CAD bonds view
    poetry run python dtale_app.py --sample-size 50000 # Use smaller sample
    poetry run python dtale_app.py --port 40001        # Use different port
    poetry run python dtale_app.py --list-views        # List all available views
        """
    )
    
    parser.add_argument(
        '--data-path', 
        default='historical g spread/bond_z.parquet',
        help='Path to bond_z.parquet file'
    )
    parser.add_argument(
        '--sample-size', 
        type=int, 
        default=100000,
        help='Number of rows to sample for performance (default: 100000)'
    )
    parser.add_argument(
        '--port', 
        type=int, 
        default=40000,
        help='Port number for dtale server (default: 40000)'
    )
    parser.add_argument(
        '--view', 
        choices=['all', 'cad-only', 'same-sector', 'same-ticker', 'portfolio', 'executable', 'cross-currency'],
        default='all',
        help='View to launch (default: all)'
    )
    parser.add_argument(
        '--list-views', 
        action='store_true',
        help='List all available views and exit'
    )
    
    args = parser.parse_args()
    
    # Initialize the app
    app = BondDtaleApp(
        data_path=args.data_path,
        sample_size=args.sample_size,
        port=args.port
    )
    
    # Handle list views
    if args.list_views:
        app.list_views()
        return
    
    # Load data
    if not app.load_data():
        logger.error("Failed to load data. Exiting.")
        sys.exit(1)
    
    # Launch dashboard
    if not app.launch_dashboard(args.view):
        logger.error("Failed to launch dashboard. Exiting.")
        sys.exit(1)
    
    # Keep the app running
    try:
        print(f"\n🚀 Dashboard running at: http://localhost:{args.port}")
        print("Press Ctrl+C to stop the server")
        
        # Keep the main thread alive
        import time
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down dtale app...")
        for instance in app.dtale_instances.values():
            try:
                instance.kill()
            except:
                pass
        print("✅ Goodbye!")

if __name__ == "__main__":
    main() 