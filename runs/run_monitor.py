#!/usr/bin/env python3
"""
Advanced Run Monitor - Comprehensive Trading Analysis Tool

Calculates period-over-period changes, best bid/offer analysis, and dealer attribution
for trading data. Outputs both Parquet and CSV formats with detailed analytics.

Features:
- DoD, WoW, MTD, QTD, YTD, 1YR spread change calculations
- Best Bid/Offer analysis with size constraints (2M+)
- Dealer attribution for best levels
- Size change analysis (DoD, MTD)
- Comprehensive data validation and logging
- Sorted output by DoD (smallest to largest)

Author: AI Assistant
Date: 2025-06-30
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import sys
import warnings
warnings.filterwarnings('ignore')

# Add project root to path for imports
sys.path.append(os.path.dirname(__file__))

class RunMonitor:
    def __init__(self, source_file='runs/combined_runs.parquet', output_dir='runs'):
        """Initialize the Run Monitor with data source and output configuration."""
        self.source_file = source_file
        self.output_dir = output_dir
        self.df = None
        self.most_recent_date = None
        self.results = None
        
        # Period calculation settings
        self.min_size_threshold = 2_000_000  # 2M minimum size
        
        print("="*80)
        print("[INFO] ADVANCED RUN MONITOR - TRADING ANALYSIS SYSTEM")
        print("="*80)
        print(f"[FILE] Source File: {self.source_file}")
        print(f"📂 Output Directory: {self.output_dir}")
        print(f"💰 Size Threshold: {self.min_size_threshold:,}")
        print("="*80)

    def load_and_prepare_data(self):
        """Load and prepare the trading data for analysis."""
        print("\n🔄 LOADING AND PREPARING DATA")
        print("-" * 50)
        
        try:
            # Load the data
            print(f"[INFO] Loading data from {self.source_file}...")
            self.df = pd.read_parquet(self.source_file)
            print(f"✅ Loaded {len(self.df):,} records with {self.df.shape[1]} columns")
            
            # Ensure Date column is datetime
            if not pd.api.types.is_datetime64_any_dtype(self.df['Date']):
                self.df['Date'] = pd.to_datetime(self.df['Date'])
            
            # Get most recent date
            self.most_recent_date = self.df['Date'].max()
            print(f"📅 Most recent date: {self.most_recent_date.strftime('%Y-%m-%d')}")
            
            # Validate key columns exist
            required_cols = ['Date', 'CUSIP', 'Dealer', 'Security', 'Bid Spread', 'Ask Spread', 
                           'Bid Size', 'Ask Size', 'Bid Interpolated Spread to Government', 'Keyword']
            missing_cols = [col for col in required_cols if col not in self.df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Check for duplicates
            dupes = self.df.groupby(['Date', 'CUSIP', 'Dealer']).size()
            duplicate_count = (dupes > 1).sum()
            if duplicate_count > 0:
                print(f"⚠️  WARNING: Found {duplicate_count:,} duplicate groups by Date/CUSIP/Dealer")
                print(f"   Taking last occurrence for each group...")
                self.df = self.df.groupby(['Date', 'CUSIP', 'Dealer']).last().reset_index()
                print(f"✅ After deduplication: {len(self.df):,} records")
            else:
                print("✅ No duplicates found - data is properly unique by Date/CUSIP/Dealer")
            
            # Data quality summary
            recent_count = len(self.df[self.df['Date'] == self.most_recent_date])
            print(f"📊 Records on most recent date: {recent_count:,}")
            print(f"📈 Date range: {self.df['Date'].min().strftime('%Y-%m-%d')} to {self.df['Date'].max().strftime('%Y-%m-%d')}")
            print(f"🏢 Unique dealers: {self.df['Dealer'].nunique()}")
            print(f"🧾 Unique CUSIPs: {self.df['CUSIP'].nunique()}")
            
        except Exception as e:
            print(f"[FAIL] ERROR loading data: {e}")
            raise

    def calculate_period_changes(self):
        """Calculate period-over-period changes for Bid Spread and Size metrics."""
        print("\n📊 CALCULATING PERIOD-OVER-PERIOD CHANGES")
        print("-" * 50)
        
        # Get records for the most recent date
        latest_data = self.df[self.df['Date'] == self.most_recent_date].copy()
        print(f"🎯 Analyzing {len(latest_data):,} records from {self.most_recent_date.strftime('%Y-%m-%d')}")
        
        # Calculate reference dates
        periods = {
            'DoD': self.most_recent_date - timedelta(days=1),
            'WoW': self.most_recent_date - timedelta(weeks=1),
            'MTD': (self.most_recent_date.replace(day=1) - timedelta(days=1)),
            'QTD': self._get_quarter_start(self.most_recent_date) - timedelta(days=1),
            'YTD': datetime(self.most_recent_date.year - 1, 12, 31),
            '1YR': self.most_recent_date - relativedelta(years=1)
        }
        
        print("\n📅 Reference dates for calculations:")
        for period, ref_date in periods.items():
            available_date = self._find_nearest_available_date(ref_date)
            print(f"   {period:3s}: {ref_date.strftime('%Y-%m-%d')} → Available: {available_date.strftime('%Y-%m-%d') if available_date else 'None'}")
        
        # Initialize results DataFrame
        results = latest_data[['Security', 'CUSIP', 'Dealer', 'Bid Spread', 'Ask Spread', 
                              'Bid Size', 'Ask Size', 'Keyword']].copy()
        
        # Calculate changes for each period
        for period_name, ref_date in periods.items():
            available_date = self._find_nearest_available_date(ref_date)
            
            if available_date:
                # Get historical data for this date
                hist_data = self.df[self.df['Date'] == available_date][['CUSIP', 'Dealer', 'Bid Spread', 'Bid Size', 'Ask Size']].copy()
                
                # Rename columns to avoid conflicts
                hist_data = hist_data.rename(columns={
                    'Bid Spread': 'Bid Spread_hist',
                    'Bid Size': 'Bid Size_hist', 
                    'Ask Size': 'Ask Size_hist'
                })
                
                # Merge with current data to calculate changes
                merged = latest_data[['CUSIP', 'Dealer', 'Bid Spread', 'Bid Size', 'Ask Size']].merge(
                    hist_data, on=['CUSIP', 'Dealer'], how='left'
                )
                
                # Calculate Bid Spread change and assign to results using index alignment
                change_series = pd.Series(index=results.index, dtype=float)
                for i, (cusip, dealer) in enumerate(zip(results['CUSIP'], results['Dealer'])):
                    mask = (merged['CUSIP'] == cusip) & (merged['Dealer'] == dealer)
                    match = merged[mask]
                    if len(match) > 0:
                        change_series.iloc[i] = match['Bid Spread'].iloc[0] - match['Bid Spread_hist'].iloc[0]
                    else:
                        change_series.iloc[i] = np.nan
                
                results[period_name] = change_series
                
                # Calculate size changes for DoD and MTD
                if period_name in ['DoD', 'MTD']:
                    bid_size_change_series = pd.Series(index=results.index, dtype=float)
                    ask_size_change_series = pd.Series(index=results.index, dtype=float)
                    
                    for i, (cusip, dealer) in enumerate(zip(results['CUSIP'], results['Dealer'])):
                        mask = (merged['CUSIP'] == cusip) & (merged['Dealer'] == dealer)
                        match = merged[mask]
                        if len(match) > 0:
                            bid_size_change_series.iloc[i] = match['Bid Size'].iloc[0] - match['Bid Size_hist'].iloc[0]
                            ask_size_change_series.iloc[i] = match['Ask Size'].iloc[0] - match['Ask Size_hist'].iloc[0]
                        else:
                            bid_size_change_series.iloc[i] = np.nan
                            ask_size_change_series.iloc[i] = np.nan
                    
                    results[f'{period_name} Chg Bid Size'] = bid_size_change_series
                    results[f'{period_name} Chg Ask Size'] = ask_size_change_series
                
                valid_changes = results[period_name].notna().sum()
                print(f"   {period_name:3s}: {valid_changes:,} valid changes calculated")
            else:
                results[period_name] = np.nan
                if period_name in ['DoD', 'MTD']:
                    results[f'{period_name} Chg Bid Size'] = np.nan
                    results[f'{period_name} Chg Ask Size'] = np.nan
                print(f"   {period_name:3s}: No data available")
        
        # Add G Spread (Bid Interpolated Spread to Government)
        results['G Spread'] = latest_data['Bid Interpolated Spread to Government']
        
        return results

    def calculate_best_levels(self, results):
        """Calculate Best Bid, Best Offer, and dealer attribution."""
        print("\n🏆 CALCULATING BEST BID/OFFER LEVELS")
        print("-" * 50)
        
        # Get latest data with size constraints
        latest_data = self.df[self.df['Date'] == self.most_recent_date].copy()
        
        # Apply size constraints for best level calculations
        bid_eligible = latest_data[
            (latest_data['Bid Size'] >= self.min_size_threshold) & 
            (latest_data['Bid Spread'].notna())
        ]
        
        ask_eligible = latest_data[
            (latest_data['Ask Size'] >= self.min_size_threshold) & 
            (latest_data['Ask Spread'].notna())
        ]
        
        print(f"📊 Size constraint analysis:")
        print(f"   Total records: {len(latest_data):,}")
        print(f"   Bid Size >= {self.min_size_threshold:,}: {len(bid_eligible):,} ({len(bid_eligible)/len(latest_data)*100:.1f}%)")
        print(f"   Ask Size >= {self.min_size_threshold:,}: {len(ask_eligible):,} ({len(ask_eligible)/len(latest_data)*100:.1f}%)")
        
        # Calculate best levels for each CUSIP
        best_levels = {}
        
        print(f"\n🔍 Calculating best levels by CUSIP...")
        for cusip in results['CUSIP'].unique():
            cusip_bid_data = bid_eligible[bid_eligible['CUSIP'] == cusip]
            cusip_ask_data = ask_eligible[ask_eligible['CUSIP'] == cusip]
            
            best_levels[cusip] = {
                'Best Bid': np.nan,
                'Best Offer': np.nan,
                'Bid/Offer': np.nan,
                'Dealer @ Best Bid': None,
                'Dealer @ Best Offer': None,
                'Size @ Best Bid': np.nan,
                'Size @ Best Offer': np.nan
            }
            
            # Find best bid (minimum spread)
            if len(cusip_bid_data) > 0:
                min_bid_spread = cusip_bid_data['Bid Spread'].min()
                best_bid_records = cusip_bid_data[cusip_bid_data['Bid Spread'] == min_bid_spread]
                
                if len(best_bid_records) > 1:
                    # Tie-breaker: largest size
                    best_bid_record = best_bid_records.loc[best_bid_records['Bid Size'].idxmax()]
                else:
                    best_bid_record = best_bid_records.iloc[0]
                
                best_levels[cusip]['Best Bid'] = min_bid_spread
                best_levels[cusip]['Dealer @ Best Bid'] = best_bid_record['Dealer']
                best_levels[cusip]['Size @ Best Bid'] = best_bid_record['Bid Size']
            
            # Find best offer (minimum spread)
            if len(cusip_ask_data) > 0:
                min_ask_spread = cusip_ask_data['Ask Spread'].min()
                best_ask_records = cusip_ask_data[cusip_ask_data['Ask Spread'] == min_ask_spread]
                
                if len(best_ask_records) > 1:
                    # Tie-breaker: largest size
                    best_ask_record = best_ask_records.loc[best_ask_records['Ask Size'].idxmax()]
                else:
                    best_ask_record = best_ask_records.iloc[0]
                
                best_levels[cusip]['Best Offer'] = min_ask_spread
                best_levels[cusip]['Dealer @ Best Offer'] = best_ask_record['Dealer']
                best_levels[cusip]['Size @ Best Offer'] = best_ask_record['Ask Size']
            
            # Calculate Bid/Offer spread
            if not pd.isna(best_levels[cusip]['Best Bid']) and not pd.isna(best_levels[cusip]['Best Offer']):
                best_levels[cusip]['Bid/Offer'] = best_levels[cusip]['Best Bid'] - best_levels[cusip]['Best Offer']
        
        # Add best level data to results
        for col in ['Best Bid', 'Best Offer', 'Bid/Offer', 'Dealer @ Best Bid', 
                   'Dealer @ Best Offer', 'Size @ Best Bid', 'Size @ Best Offer']:
            results[col] = results['CUSIP'].map(lambda x: best_levels.get(x, {}).get(col, np.nan))
        
        # Summary statistics
        valid_best_bids = results['Best Bid'].notna().sum()
        valid_best_offers = results['Best Offer'].notna().sum()
        valid_spreads = results['Bid/Offer'].notna().sum()
        
        print(f"✅ Best level calculation complete:")
        print(f"   CUSIPs with Best Bid: {valid_best_bids:,}")
        print(f"   CUSIPs with Best Offer: {valid_best_offers:,}")
        print(f"   CUSIPs with Bid/Offer spread: {valid_spreads:,}")
        
        return results

    def generate_final_report(self):
        """Generate the comprehensive run monitor report."""
        print("\n📋 GENERATING COMPREHENSIVE REPORT")
        print("-" * 50)
        
        # Calculate period changes
        results = self.calculate_period_changes()
        
        # Calculate best levels
        results = self.calculate_best_levels(results)
        
        # Reorder columns to match specification
        column_order = [
            'Security', 'Bid Spread', 'Ask Spread', 'Bid Size', 'Ask Size',
            'DoD', 'WoW', 'MTD', 'QTD', 'YTD', '1YR',
            'DoD Chg Bid Size', 'DoD Chg Ask Size', 'MTD Chg Bid Size', 'MTD Chg Ask Size',
            'Best Bid', 'Best Offer', 'Bid/Offer',
            'Dealer @ Best Bid', 'Dealer @ Best Offer', 
            'Size @ Best Bid', 'Size @ Best Offer',
            'CUSIP', 'G Spread', 'Keyword'
        ]
        
        # Ensure all columns exist
        for col in column_order:
            if col not in results.columns:
                results[col] = np.nan
        
        # Select and reorder columns
        results = results[column_order]
        
        # Sort by DoD (smallest to largest)
        results = results.sort_values('DoD', na_position='last')
        
        # Summary statistics
        print(f"📊 FINAL REPORT SUMMARY")
        print(f"   Total securities: {len(results):,}")
        print(f"   Records with DoD data: {results['DoD'].notna().sum():,}")
        print(f"   Records with WoW data: {results['WoW'].notna().sum():,}")
        print(f"   Records with Best Bid: {results['Best Bid'].notna().sum():,}")
        print(f"   Records with Best Offer: {results['Best Offer'].notna().sum():,}")
        
        # DoD distribution
        dod_data = results['DoD'].dropna()
        if len(dod_data) > 0:
            print(f"\n📈 DoD Change Distribution:")
            print(f"   Min: {dod_data.min():.2f}")
            print(f"   Q1:  {dod_data.quantile(0.25):.2f}")
            print(f"   Med: {dod_data.median():.2f}")
            print(f"   Q3:  {dod_data.quantile(0.75):.2f}")
            print(f"   Max: {dod_data.max():.2f}")
        
        self.results = results
        return results

    def save_outputs(self):
        """Save the results to both Parquet and CSV formats."""
        print("\n💾 SAVING OUTPUT FILES")
        print("-" * 50)
        
        if self.results is None:
            print("[FAIL] No results to save. Run generate_final_report() first.")
            return
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Define output files
        parquet_file = os.path.join(self.output_dir, 'run_monitor.parquet')
        csv_file = os.path.join(self.output_dir, 'processed runs data', 'run_monitor.csv')
        
        # Ensure CSV directory exists
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        
        try:
            # Save Parquet file
            self.results.to_parquet(parquet_file, index=False)
            parquet_size = os.path.getsize(parquet_file)
            print(f"✅ Parquet saved: {parquet_file}")
            print(f"   Size: {parquet_size:,} bytes ({parquet_size/(1024*1024):.1f} MB)")
            
            # Save CSV file
            self.results.to_csv(csv_file, index=False)
            csv_size = os.path.getsize(csv_file)
            print(f"✅ CSV saved: {csv_file}")
            print(f"   Size: {csv_size:,} bytes ({csv_size/(1024*1024):.1f} MB)")
            
            print(f"\n📋 Output Summary:")
            print(f"   Records: {len(self.results):,}")
            print(f"   Columns: {len(self.results.columns)}")
            print(f"   Analysis Date: {self.most_recent_date.strftime('%Y-%m-%d')}")
            
        except Exception as e:
            print(f"[FAIL] ERROR saving files: {e}")
            raise

    def _get_quarter_start(self, date):
        """Get the start of the quarter for a given date."""
        quarter = (date.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        return datetime(date.year, quarter_start_month, 1)

    def _find_nearest_available_date(self, target_date):
        """Find the nearest available date in the dataset to the target date."""
        available_dates = self.df['Date'].unique()
        available_dates = pd.to_datetime(available_dates)
        
        # Find the closest date that is <= target_date
        valid_dates = available_dates[available_dates <= target_date]
        
        if len(valid_dates) == 0:
            return None
        
        return valid_dates.max()

    def run_full_analysis(self):
        """Execute the complete run monitor analysis."""
        print(f"\n🚀 STARTING FULL RUN MONITOR ANALYSIS")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Load and prepare data
            self.load_and_prepare_data()
            
            # Step 2: Generate comprehensive report
            results = self.generate_final_report()
            
            # Step 3: Save outputs
            self.save_outputs()
            
            print("\n" + "="*80)
            print("🎉 RUN MONITOR ANALYSIS COMPLETE!")
            print("="*80)
            print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"📊 Final dataset: {len(results):,} records × {len(results.columns)} columns")
            print(f"📅 Analysis date: {self.most_recent_date.strftime('%Y-%m-%d')}")
            print(f"[FILE] Files saved:")
            print(f"   • Parquet: runs/run_monitor.parquet")
            print(f"   • CSV: runs/processed runs data/run_monitor.csv")
            print("="*80)
            
            return results
            
        except Exception as e:
            print(f"\n[FAIL] ANALYSIS FAILED: {e}")
            print("="*80)
            raise

def main():
    """Main execution function."""
    try:
        # Initialize and run the monitor
        monitor = RunMonitor()
        results = monitor.run_full_analysis()
        
        # Display sample results
        print(f"\n📋 SAMPLE RESULTS (Top 10 by DoD):")
        print("-" * 80)
        sample_cols = ['Security', 'DoD', 'WoW', 'MTD', 'Best Bid', 'Best Offer', 'Dealer @ Best Bid']
        print(results[sample_cols].head(10).to_string(index=False))
        
        # ADD REQUESTED .info() OUTPUT
        print(f"\n📊 FINAL DATASET INFORMATION:")
        print("-" * 80)
        print(results.info())
        
        return results
        
    except Exception as e:
        print(f"[FAIL] FATAL ERROR: {e}")
        return None

if __name__ == "__main__":
    results = main() 