"""
Analytics Module for Bond G-Spread Data Analysis
==============================================

This module provides specialized analytics tools for bond data analysis,
including dtale-based interactive dashboards and bond-specific filtering.

Components:
- bond_analytics.py: Bond-specific data analysis and filtering
- dtale_dashboard.py: Multi-tab interactive dashboard
"""

from .bond_analytics import BondAnalytics
from .dtale_dashboard import MultiTabDtaleApp

__all__ = ['BondAnalytics', 'MultiTabDtaleApp'] 