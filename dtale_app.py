import pandas as pd
import numpy as np
import dtale
import logging
from typing import Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class BondDtaleApp:
    """
    Core dtale app for bond analytics. Handles data loading, sampling, and view creation.
    """
    def __init__(self, data_path: str = "historical g spread/bond_z.parquet", sample_size: int = 25000, port: int = 40000):
        self.data_path = data_path
        self.sample_size = sample_size
        self.port = port
        self.df_full: Optional[pd.DataFrame] = None
        self.df_sample: Optional[pd.DataFrame] = None
        self.dtale_instances: Dict[str, dtale.app.DtaleData] = {}
        self.stats = {
            'total_rows': 0,
            'total_columns': 0,
            'memory_usage_mb': 0.0
        }
        self.views = {
            'all': {
                'name': 'All Data',
                'description': 'Full sample of bond g-spread data.'
            },
            'cad-only': {
                'name': 'CAD Only',
                'description': 'CAD-denominated bonds only.'
            },
            'same-sector': {
                'name': 'Same Sector',
                'description': 'Bonds in the same sector.'
            },
            'same-ticker': {
                'name': 'Same Ticker',
                'description': 'Bonds with the same ticker.'
            },
            'portfolio': {
                'name': 'Portfolio',
                'description': 'Bonds in the portfolio (Own? == 1).'
            },
            'executable': {
                'name': 'Executable',
                'description': 'Bonds with executable trades.'
            },
            'cross-currency': {
                'name': 'Cross-Currency',
                'description': 'Bonds with cross-currency exposure.'
            },
        }

    def load_data(self) -> bool:
        """Load and sample the bond data."""
        try:
            df = pd.read_parquet(self.data_path)
            self.df_full = df
            self.df_sample = df.sample(n=min(self.sample_size, len(df)), random_state=42) if len(df) > self.sample_size else df.copy()
            self.stats = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 ** 2
            }
            return True
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            self.df_full = None
            self.df_sample = None
            return False

    def _filter_cad_only(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df['Currency_1'] == 'CAD') & (df['Currency_2'] == 'CAD')]

    def _filter_same_sector(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df['Custom_Sector_1'] == df['Custom_Sector_2']]

    def _filter_portfolio(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df['Own?'] == 1]

    def _filter_same_ticker(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df['Currency_1'] == 'CAD') & (df['Currency_2'] == 'CAD') & (df['Ticker_1'] == df['Ticker_2'])]

    def _filter_executable(self, df: pd.DataFrame) -> pd.DataFrame:
        # Placeholder: define your own logic for executable bonds
        return df[df.get('Executable', 1) == 1] if 'Executable' in df.columns else df.head(0)

    def _filter_cross_currency(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df['Currency_1'] != df['Currency_2']]

    def create_view(self, view_name: str) -> Optional[dtale.app.DtaleData]:
        if self.df_sample is None:
            logger.error("Data not loaded. Call load_data() first.")
            return None
        if view_name not in self.views:
            logger.error(f"Unknown view: {view_name}")
            return None
        # Select the appropriate filter
        if view_name == 'all':
            df_view = self.df_sample
        elif view_name == 'cad-only':
            df_view = self._filter_cad_only(self.df_sample)
        elif view_name == 'same-sector':
            df_view = self._filter_same_sector(self.df_sample)
        elif view_name == 'portfolio':
            df_view = self._filter_portfolio(self.df_sample)
        elif view_name == 'same-ticker':
            df_view = self._filter_same_ticker(self.df_sample)
        elif view_name == 'executable':
            df_view = self._filter_executable(self.df_sample)
        elif view_name == 'cross-currency':
            df_view = self._filter_cross_currency(self.df_sample)
        else:
            logger.error(f"No filter defined for view: {view_name}")
            return None
        try:
            instance = dtale.show(df_view, host='localhost', port=self.port, subprocess=False, open_browser=False)
            self.dtale_instances[view_name] = instance
            return instance
        except Exception as e:
            logger.error(f"Failed to create dtale view '{view_name}': {e}")
            return None

    def list_views(self):
        print("Available views:")
        for k, v in self.views.items():
            print(f"- {k}: {v['name']} - {v['description']}") 