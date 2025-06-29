#!/usr/bin/env python
"""
Final upload script with proper column mapping.
This script correctly maps all Parquet column names to database column names.
"""
import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
import json

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.config import ConfigManager
from src.utils.logging import LogManager


def create_column_mapping():
    """Create comprehensive mapping from Parquet columns to DB columns."""
    return {
        'universe': {
            # From forensic output
            'Benchmark Cusip': 'benchmark_cusip',
            'Bloomberg Cusip': 'bloomberg_cusip',
            'Notes': 'issuer_name',  # Map Notes to issuer_name
            'Issue Date': 'issue_date',
            'Maturity Date': 'maturity_date',
            'Currency': 'currency',
            'Issue Size': 'issue_size',
            'Coupon': 'coupon',
            'Payment Frequency': 'payment_frequency',
            'Security Type': 'security_type',
            'Dated Date': 'dated_date',
            'First Pay Date': 'first_pay_date',
            'Last Pay Date': 'last_pay_date',
            'Day Count': 'day_count',
            'Ticker': 'ticker',
            'Exchange': 'exchange',
            'Country': 'country',
            'Industry': 'industry',
            'Sub-Industry': 'sub_industry',
            'SandP Rating': 'sp_rating',
            'Moodys Rating': 'moodys_rating',
            'Fitch Rating': 'fitch_rating',
            'Call': 'call_schedule',
            'Put': 'put_schedule',
            'Sink': 'sink_schedule',
            'Other': 'other_features',
            'Announcement Date': 'announcement_date',
            'Price': 'price',
            'Yield': 'yield_value',
            'Spread': 'spread',
            'Benchmark': 'benchmark',
            'Benchmark Yield': 'benchmark_yield',
            'Issuer': 'issuer',
            'Rank': 'rank',
            'Secured': 'secured',
            'Guarantor': 'guarantor',
            'Pricing Date': 'pricing_date',
            'Settlement Date': 'settlement_date',
            'Underwriter': 'underwriter',
            'Fees': 'fees',
            'Governing Law': 'governing_law',
            'Market of Issue': 'market_of_issue',
            'Min Piece/Increment': 'min_piece_increment',
            'Figi': 'figi',
            'Cfi Code': 'cfi_code',
            'Isin': 'isin',
            'Common Code': 'common_code',
            'Sedol': 'sedol',
            'Valoren': 'valoren',
            'Wpk': 'wpk',
            'Use of Proceeds': 'use_of_proceeds',
            'Tranche': 'tranche',
            'Issuer Description': 'issuer_description',
            'Security Description': 'security_description',
            'Series': 'series',
            'Tax Status': 'tax_status',
            'First Settle Date': 'first_settle_date',
            'Last Settle Date': 'last_settle_date',
            'Calculation Type': 'calculation_type',
            'Business Day Convention': 'business_day_convention',
            'Accrual Date Convention': 'accrual_date_convention',
            'Holidays': 'holidays',
            'Step': 'step_schedule',
            'Default': 'default_status',
            'Registration Type': 'registration_type',
            'Registration Rights': 'registration_rights',
            'Outstanding Amount': 'outstanding_amount',
            'Private Placement': 'private_placement',
            'Perpetual': 'perpetual',
            'Fungible': 'fungible',
            'Defeasance Type': 'defeasance_type',
            'Strippable': 'strippable',
            'Canadian Event': 'canadian_event',
            'Principal Factor': 'principal_factor',
            'Original Issue Price': 'original_issue_price',
            'Original Issue Yield': 'original_issue_yield',
            'Original Issue Spread': 'original_issue_spread',
            'Make Whole Benchmark': 'make_whole_benchmark',
            'Make Whole Spread': 'make_whole_spread',
            'Modified Make Whole': 'modified_make_whole',
            'Percent of Index': 'percent_of_index',
            'Issuer Industry': 'issuer_industry',
            'Issuer Sub-Industry': 'issuer_sub_industry',
            'Issuer SandP Rating': 'issuer_sp_rating',
            'Issuer Moodys Rating': 'issuer_moodys_rating',
            'Issuer Fitch Rating': 'issuer_fitch_rating',
            'Dual Currency': 'dual_currency',
            'Original Currency': 'original_currency',
            'Settlement Currency': 'settlement_currency',
            'Lead Underwriter': 'lead_underwriter',
            'Joint Lead Underwriter': 'joint_lead_underwriter',
            'Co-Manager': 'co_manager',
            'Bookrunner': 'bookrunner',
            'Selling Restrictions': 'selling_restrictions',
            'Minimum Denomination': 'minimum_denomination',
            'Calculation Agent': 'calculation_agent',
            'Paying Agent': 'paying_agent',
            'Trustee': 'trustee',
            'Depositary': 'depositary',
            'Clearing System': 'clearing_system',
            'Listing': 'listing',
            'Admission to Trading': 'admission_to_trading',
            'Method of Quotation': 'method_of_quotation',
            'Form': 'form',
            'Delivery': 'delivery',
            'Underlying Index': 'underlying_index',
            'Redemption': 'redemption',
            'Underlying Security': 'underlying_security',
            'Unit': 'unit',
            'Contract Size': 'contract_size',
            'Conversion Ratio': 'conversion_ratio',
            'Underlying Ticker': 'underlying_ticker',
            'Underlying Isin': 'underlying_isin',
            'Underlying Cusip': 'underlying_cusip',
            'Underlying Currency': 'underlying_currency',
            'Underlying Price': 'underlying_price',
            'Strike': 'strike',
            'Option Type': 'option_type',
            'Option Style': 'option_style',
            'Expiration Date': 'expiration_date',
            'Exercise Date': 'exercise_date',
            'Barrier': 'barrier',
            'Barrier Type': 'barrier_type',
            'Knock In': 'knock_in',
            'Knock Out': 'knock_out',
            'Touch': 'touch',
            'Underlying Bloomberg Ticker': 'underlying_bloomberg_ticker',
            'Underlying Bloomberg ID': 'underlying_bloomberg_id',
            'Underlying RIC': 'underlying_ric',
            'Underlying SEDOL': 'underlying_sedol',
            'Underlying Exchange': 'underlying_exchange',
            'Underlying Country': 'underlying_country',
            'Underlying Industry': 'underlying_industry',
            'Underlying Sub-Industry': 'underlying_sub_industry',
            'Underlying SandP Rating': 'underlying_sp_rating',
            'Underlying Moodys Rating': 'underlying_moodys_rating',
            'Underlying Fitch Rating': 'underlying_fitch_rating',
            'Seniority': 'seniority',
            'Restructuring': 'restructuring',
            'Collective Action Clause': 'collective_action_clause',
            'Bail In': 'bail_in',
            'Contingent Conversion': 'contingent_conversion',
            'Write Down': 'write_down',
            'Equity Clawback': 'equity_clawback',
            'Tax Call': 'tax_call',
            'Regulatory Call': 'regulatory_call',
            'Accounting Standard': 'accounting_standard',
            'Financial Reporting Standard': 'financial_reporting_standard',
            'Covered Bond': 'covered_bond',
            'Cover Pool': 'cover_pool',
            'Collateral Type': 'collateral_type',
            'Collateral Currency': 'collateral_currency',
            'Collateral Country': 'collateral_country',
            'Collateral Region': 'collateral_region',
            'Collateral Value': 'collateral_value',
            'Over Collateralization': 'over_collateralization',
            'Asset Coverage Test': 'asset_coverage_test',
            'Liquidity Reserve': 'liquidity_reserve',
            'Redemption Amount': 'redemption_amount',
            'Reference Rate': 'reference_rate',
            'Reference Rate Tenor': 'reference_rate_tenor',
            'Reference Rate Currency': 'reference_rate_currency',
            'Fallback Rate': 'fallback_rate',
            'Fallback Provisions': 'fallback_provisions',
            'Cap': 'cap',
            'Floor': 'floor',
            'Multiplier': 'multiplier',
            'Participation Rate': 'participation_rate',
            'Averaging': 'averaging',
            'Observation Dates': 'observation_dates',
            'Fixing Dates': 'fixing_dates',
            'Payment Dates': 'payment_dates',
            'Reset Dates': 'reset_dates',
            'Determination Dates': 'determination_dates',
            'Inflation Index': 'inflation_index',
            'Base CPI': 'base_cpi',
            'Reference CPI': 'reference_cpi',
            'Inflation Lag': 'inflation_lag',
            'Deflation Floor': 'deflation_floor',
            'Inflation Cap': 'inflation_cap',
            'Index Ratio': 'index_ratio',
            'Green Bond': 'green_bond',
            'Social Bond': 'social_bond',
            'Sustainability Bond': 'sustainability_bond',
            'Sustainability Linked': 'sustainability_linked',
            'Climate Bond': 'climate_bond',
            'Transition Bond': 'transition_bond',
            'Use of Proceeds Categories': 'use_of_proceeds_categories',
            'Target Population': 'target_population',
            'Environmental Objective': 'environmental_objective',
            'SPTs': 'spts',
            'KPIs': 'kpis',
            'External Review': 'external_review',
            'External Reviewer': 'external_reviewer',
            'Second Party Opinion': 'second_party_opinion',
            'Verification': 'verification',
            'Certification': 'certification',
            'Climate Bond Certified': 'climate_bond_certified',
            'EU GBS': 'eu_gbs',
            'ICMA Principles': 'icma_principles',
            'Reporting': 'reporting',
            'Reporting Frequency': 'reporting_frequency',
            'Impact Reporting': 'impact_reporting',
            'Allocation Reporting': 'allocation_reporting',
            'Eligibility Criteria': 'eligibility_criteria',
            'Project Categories': 'project_categories',
            'Exclusions': 'exclusions',
            'Process for Evaluation': 'process_for_evaluation',
            'Management of Proceeds': 'management_of_proceeds',
            'Unallocated Proceeds': 'unallocated_proceeds',
            'Refinancing Share': 'refinancing_share',
            'Look Back Period': 'look_back_period',
            'Project Locations': 'project_locations',
            'Reporting Indicators': 'reporting_indicators',
            'UN SDGs': 'un_sdgs',
            'EU Taxonomy': 'eu_taxonomy',
            'Do No Significant Harm': 'do_no_significant_harm',
            'Minimum Safeguards': 'minimum_safeguards',
            'Technical Screening Criteria': 'technical_screening_criteria',
            'Substantial Contribution': 'substantial_contribution',
            'Transition Plan': 'transition_plan',
            'Science Based Targets': 'science_based_targets',
            'Net Zero Target': 'net_zero_target',
            'Baseline Year': 'baseline_year',
            'Target Year': 'target_year',
            'Emissions Scope': 'emissions_scope',
            'Emissions Reduction': 'emissions_reduction',
            'Carbon Intensity': 'carbon_intensity',
            'Renewable Energy': 'renewable_energy',
            'Energy Efficiency': 'energy_efficiency',
            'Circular Economy': 'circular_economy',
            'Biodiversity': 'biodiversity',
            'Water Management': 'water_management',
            'Waste Management': 'waste_management',
            'Sustainable Agriculture': 'sustainable_agriculture',
            'Sustainable Forestry': 'sustainable_forestry',
            'Affordable Housing': 'affordable_housing',
            'Access to Essential Services': 'access_to_essential_services',
            'Employment Generation': 'employment_generation',
            'Gender Equality': 'gender_equality',
            'Financial Inclusion': 'financial_inclusion',
            'Food Security': 'food_security',
            'Socioeconomic Advancement': 'socioeconomic_advancement',
            'Pandemic Response': 'pandemic_response',
            'SME Financing': 'sme_financing',
            'Microfinance': 'microfinance',
            'Education': 'education',
            'Healthcare': 'healthcare',
            'Infrastructure': 'infrastructure',
            'Clean Transportation': 'clean_transportation',
            'Information and Communications Technology': 'information_and_communications_technology',
            'Cusip': 'cusip'  # Add direct mapping for Cusip
        },
        'runs': {
            # Map all column names with spaces to underscores
            'Date': 'date',
            'Time': 'time',
            'Dealer': 'dealer',
            'CUSIP': 'cusip',
            'Security': 'security',
            'Bid Price': 'bid_price',
            'Ask Price': 'ask_price',
            'Bid Yield To Convention': 'bid_yield_to_convention',
            'Ask Yield To Convention': 'ask_yield_to_convention',
            'Bid Spread': 'bid_spread',
            'Ask Spread': 'ask_spread',
            'Bid Size': 'bid_size',
            'Ask Size': 'ask_size',
            'Benchmark': 'benchmark',
            'Reference Benchmark': 'reference_benchmark',
            'Reference Security': 'reference_security',
            'Bid Z-spread': 'bid_z_spread',
            'Ask Z-spread': 'ask_z_spread',
            'Bid Discount Margin': 'bid_discount_margin',
            'Ask Discount Margin': 'ask_discount_margin',
            'Bid Workout Risk': 'bid_workout_risk',
            'Ask Workout Risk': 'ask_workout_risk',
            'Bid Contributed Yield': 'bid_contributed_yield',
            'Ask Contributed Yield': 'ask_contributed_yield',
            'Bid Interpolated Spread to Government': 'bid_interpolated_spread_to_government',
            'Ask Interpolated Spread to Government': 'ask_interpolated_spread_to_government',
            'Subject': 'subject',
            'Sender Name': 'sender_name',
            'Source': 'source',
            'Sector': 'sector',
            'Ticker': 'ticker',
            'Currency': 'currency',
            'Keyword': 'keyword'
        },
        'portfolio': {
            # Map all column names to lowercase with underscores
            'Date': 'date',
            'ACCOUNT': 'account',
            'PORTFOLIO': 'portfolio',
            'STRATEGY': 'strategy',
            'TRADE GROUP': 'trade_group',
            'TradeGroup Fixed': 'trade_group_fixed',
            'SECURITY CLASSIFICATION': 'security_classification',
            'SECURITY': 'security',
            'SECURITY TYPE': 'security_type',
            'CUSIP': 'cusip',
            'ISIN': 'isin',
            'UNDERLYING SECURITY': 'underlying_security',
            'UNDERLYING CUSIP': 'underlying_cusip',
            'UNDERLYING ISIN': 'underlying_isin',
            'COMPANY SYMBOL': 'company_symbol',
            'CURRENCY': 'currency',
            'QUANTITY': 'quantity',
            'PRICE': 'price',
            'VALUE': 'value',
            'VALUE PCT NAV': 'value_pct_nav',
            'FACE VALUE OUTSTANDING': 'face_value_outstanding',
            'PCT OF OUTSTANDING': 'pct_of_outstanding',
            'COUPON': 'coupon',
            'MATURITY DATE': 'maturity_date',
            'MATURITY BUCKET': 'maturity_bucket',
            'BENCHMARK MATURITY DATE': 'benchmark_maturity_date',
            'CREDIT RATING': 'credit_rating',
            'YIELD TO MAT': 'yield_to_mat',
            'Yield': 'yield_value',
            'Yield CAD Fee Included': 'yield_cad_fee_included',
            'Benchmark Yield': 'benchmark_yield',
            'Benchmark Yield Including Fee': 'benchmark_yield_including_fee',
            'Sprd Calculated': 'spread_calculated',
            'Sprd Funding Adjusted': 'spread_funding_adjusted',
            'Funding Cost': 'funding_cost',
            'Fuding Status': 'funding_status',
            'OAS (Bloomberg)': 'oas_bloomberg',
            'OAS Fund Adj(Bloomberg)': 'oas_fund_adj_bloomberg',
            'MODIFIED DURATION': 'modified_duration',
            'Duration Calc': 'duration_calc',
            'Duration Calc (Corps)': 'duration_calc_corps',
            'Duration Calc (Corps + CDX)': 'duration_calc_corps_cdx',
            'OAD Calc': 'oad_calc',
            'Yrs Calc': 'yrs_calc',
            'Yrs Calc (Corps)': 'yrs_calc_corps',
            'Yrs Calc (Corps + CDX)': 'yrs_calc_corps_cdx',
            'POSITION PVBP': 'position_pvbp',
            'SECURITY PVBP': 'security_pvbp',
            'POSITION CR01': 'position_cr01',
            'SECURITY CR01': 'security_cr01',
            'CDS Adj Size (Calc)': 'cds_adj_size_calc'
        }
    }


def map_dataframe_columns(df, table_name, column_mapping):
    """Map DataFrame columns using the mapping dictionary."""
    if table_name not in column_mapping:
        return df
    
    mapping = column_mapping[table_name]
    
    # Create reverse mapping for existing columns
    existing_columns = list(df.columns)
    mapped_df = df.copy()
    
    # Rename columns based on mapping
    rename_dict = {}
    for old_col, new_col in mapping.items():
        if old_col in existing_columns:
            rename_dict[old_col] = new_col
    
    if rename_dict:
        mapped_df = mapped_df.rename(columns=rename_dict)
        print(f"Mapped {len(rename_dict)} columns for {table_name}")
    
    return mapped_df


def clean_for_json(df):
    """Clean DataFrame for JSON serialization."""
    # Replace NaN and infinity with None
    df = df.replace([np.inf, -np.inf], None)
    
    # Convert all columns to ensure JSON serialization
    for col in df.columns:
        if df[col].dtype == 'object':
            # Ensure strings are properly encoded
            df[col] = df[col].astype(str).replace('nan', None)
        elif np.issubdtype(df[col].dtype, np.floating):
            # Handle float columns
            df[col] = df[col].where(pd.notna(df[col]), None)
        elif np.issubdtype(df[col].dtype, np.integer):
            # Handle integer columns
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    return df


def upload_with_proper_mapping():
    """Upload all data with proper column mapping."""
    # Setup
    load_dotenv()
    
    # Initialize Supabase client
    client = create_client(
        os.getenv('SUPABASE_URL'), 
        os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    )
    
    # Setup logging
    log_file = Path("logs/final_upload.log")
    log_file.parent.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    logger.info("="*80)
    logger.info("Starting final upload with proper column mapping")
    logger.info("="*80)
    
    # Get column mapping
    column_mapping = create_column_mapping()
    
    # Define tables to upload in order (respecting foreign keys)
    tables = [
        {
            'name': 'universe',
            'file': 'universe/universe.parquet',
            'batch_size': 100,
            'required_columns': ['cusip']  # Minimal required
        },
        {
            'name': 'runs',
            'file': 'runs/runs.parquet',
            'batch_size': 100,
            'required_columns': ['date', 'cusip', 'dealer']
        },
        {
            'name': 'portfolio',
            'file': 'portfolio/portfolio.parquet',
            'batch_size': 100,
            'required_columns': ['date', 'account', 'cusip']
        }
    ]
    
    results = {}
    
    for table_info in tables:
        table_name = table_info['name']
        file_path = table_info['file']
        batch_size = table_info['batch_size']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {table_name} table")
        logger.info(f"{'='*60}")
        
        try:
            # Load data
            df = pd.read_parquet(file_path)
            original_count = len(df)
            logger.info(f"Loaded {original_count} rows from {file_path}")
            
            # Map columns
            df = map_dataframe_columns(df, table_name, column_mapping)
            
            # Add created_at
            df['created_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            
            # Clean data for JSON
            df = clean_for_json(df)
            
            # Convert dates to strings
            for col in df.columns:
                if 'date' in col.lower() and df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    except:
                        pass
            
            # Upload in batches
            total_uploaded = 0
            failed_batches = []
            
            for i in range(0, len(df), batch_size):
                batch_num = i // batch_size + 1
                batch = df.iloc[i:i+batch_size]
                
                try:
                    # Convert to records
                    records = batch.to_dict('records')
                    
                    # Upload
                    response = client.table(table_name).insert(records).execute()
                    total_uploaded += len(batch)
                    
                    if batch_num % 10 == 0:
                        logger.info(f"Progress: {total_uploaded}/{original_count} rows uploaded")
                    
                except Exception as e:
                    logger.error(f"Failed batch {batch_num}: {str(e)}")
                    failed_batches.append(batch_num)
                    
                    # Log first record of failed batch for debugging
                    if len(records) > 0:
                        logger.debug(f"First record of failed batch: {json.dumps(records[0], indent=2, default=str)}")
            
            results[table_name] = {
                'total': original_count,
                'uploaded': total_uploaded,
                'failed_batches': failed_batches
            }
            
            logger.info(f"✅ {table_name}: Successfully uploaded {total_uploaded} out of {original_count} rows")
            
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {str(e)}")
            results[table_name] = {
                'total': 0,
                'uploaded': 0,
                'error': str(e)
            }
    
    # Final summary
    logger.info("\n" + "="*80)
    logger.info("UPLOAD SUMMARY")
    logger.info("="*80)
    
    for table, result in results.items():
        if 'error' in result:
            logger.info(f"{table}: ERROR - {result['error']}")
        else:
            success_rate = (result['uploaded'] / result['total'] * 100) if result['total'] > 0 else 0
            logger.info(f"{table}: {result['uploaded']}/{result['total']} rows ({success_rate:.1f}% success)")
            if result.get('failed_batches'):
                logger.info(f"  Failed batches: {len(result['failed_batches'])}")
    
    logger.info("\n✅ Upload process completed!")


if __name__ == "__main__":
    upload_with_proper_mapping() 