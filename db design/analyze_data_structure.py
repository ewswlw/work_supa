#!/usr/bin/env python3
"""
Data Structure Analysis Script for Supabase Database Design

This script analyzes all parquet and CSV files in the project to understand:
1. Table structures and column definitions
2. Data types and constraints
3. Relationships between tables (CUSIP linking)
4. Sample data for validation
5. Business logic and rules
"""

import pandas as pd
import os
from pathlib import Path
import numpy as np
from datetime import datetime
import json

def analyze_file_structure(file_path, file_type="parquet"):
    """Analyze a single file and return its structure info"""
    try:
        if file_type == "parquet":
            df = pd.read_parquet(file_path)
        else:  # CSV
            df = pd.read_csv(file_path)
        
        info = {
            "file_path": str(file_path),
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": {},
            "sample_data": {},
            "null_counts": {},
            "unique_counts": {},
            "date_range": None
        }
        
        # Analyze each column
        for col in df.columns:
            col_data = df[col]
            info["columns"][col] = {
                "dtype": str(col_data.dtype),
                "python_type": type(col_data.iloc[0]).__name__ if len(col_data) > 0 else "unknown",
                "non_null_count": col_data.count(),
                "null_count": col_data.isnull().sum(),
                "unique_count": col_data.nunique(),
            }
            
            # Sample data (first 3 non-null values)
            sample_values = col_data.dropna().head(3).tolist()
            info["sample_data"][col] = sample_values
            
            # Additional analysis for specific data types
            if col_data.dtype in ['datetime64[ns]', 'object']:
                if col.lower() in ['date', 'maturity_date', 'benchmark_maturity_date']:
                    try:
                        if col_data.dtype == 'object':
                            dates = pd.to_datetime(col_data, errors='coerce')
                        else:
                            dates = col_data
                        
                        if not dates.dropna().empty:
                            info["columns"][col]["date_min"] = str(dates.min())
                            info["columns"][col]["date_max"] = str(dates.max())
                    except:
                        pass
            
            # Check for CUSIP-like identifiers
            if any(x in col.lower() for x in ['cusip', 'security', 'underlying']):
                if col_data.dtype == 'object':
                    info["columns"][col]["is_identifier"] = True
                    info["columns"][col]["sample_identifiers"] = col_data.dropna().unique()[:5].tolist()
        
        # Check for date range in the data
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if date_cols:
            for date_col in date_cols:
                try:
                    dates = pd.to_datetime(df[date_col], errors='coerce')
                    if not dates.dropna().empty:
                        if info["date_range"] is None:
                            info["date_range"] = {}
                        info["date_range"][date_col] = {
                            "min": str(dates.min()),
                            "max": str(dates.max())
                        }
                except:
                    pass
        
        return info
    
    except Exception as e:
        return {"file_path": str(file_path), "error": str(e)}

def find_relationships(data_structures):
    """Analyze potential relationships between tables"""
    relationships = {}
    
    # Look for common columns across tables
    all_columns = {}
    for table_name, structure in data_structures.items():
        if "columns" in structure:
            for col_name in structure["columns"].keys():
                if col_name not in all_columns:
                    all_columns[col_name] = []
                all_columns[col_name].append(table_name)
    
    # Find columns that appear in multiple tables (potential relationships)
    relationships["common_columns"] = {
        col: tables for col, tables in all_columns.items() 
        if len(tables) > 1
    }
    
    # Look for CUSIP relationships specifically
    cusip_columns = {}
    for table_name, structure in data_structures.items():
        if "columns" in structure:
            for col_name, col_info in structure["columns"].items():
                if any(x in col_name.lower() for x in ['cusip', 'security']) and col_info.get("is_identifier"):
                    if table_name not in cusip_columns:
                        cusip_columns[table_name] = []
                    cusip_columns[table_name].append(col_name)
    
    relationships["cusip_relationships"] = cusip_columns
    
    return relationships

def main():
    """Main analysis function"""
    print("🔍 Starting comprehensive data structure analysis...")
    print("=" * 60)
    
    # Find all data files
    data_files = []
    
    # Parquet files
    parquet_files = [
        "../runs/combined_runs.parquet",
        "../universe/universe.parquet", 
        "../portfolio/portfolio.parquet",
        "../historical g spread/bond_g_sprd_time_series.parquet"
    ]
    
    # CSV files
    csv_files = [
        "../universe/processed data/universe_processed.csv",
        "../portfolio/processed data/portfolio.csv",
        "../historical g spread/processed data/bond_g_sprd_processed.csv"
    ]
    
    data_structures = {}
    
    # Analyze parquet files
    print("\n📊 ANALYZING PARQUET FILES:")
    print("-" * 40)
    for file_path in parquet_files:
        if os.path.exists(file_path):
            print(f"Analyzing: {file_path}")
            table_name = file_path.split('/')[-1].replace('.parquet', '')
            data_structures[table_name] = analyze_file_structure(file_path, "parquet")
            print(f"  ✅ {table_name}: {data_structures[table_name].get('total_rows', 0)} rows, {data_structures[table_name].get('total_columns', 0)} columns")
        else:
            print(f"  ❌ File not found: {file_path}")
    
    # Analyze CSV files
    print("\n📊 ANALYZING CSV FILES:")
    print("-" * 40)
    for file_path in csv_files:
        if os.path.exists(file_path):
            print(f"Analyzing: {file_path}")
            table_name = file_path.split('/')[-1].replace('.csv', '').replace('_processed', '')
            data_structures[f"{table_name}_csv"] = analyze_file_structure(file_path, "csv")
            print(f"  ✅ {table_name}_csv: {data_structures[f'{table_name}_csv'].get('total_rows', 0)} rows, {data_structures[f'{table_name}_csv'].get('total_columns', 0)} columns")
        else:
            print(f"  ❌ File not found: {file_path}")
    
    # Analyze relationships
    print("\n🔗 ANALYZING RELATIONSHIPS:")
    print("-" * 40)
    relationships = find_relationships(data_structures)
    
    # Generate comprehensive report
    print("\n📋 COMPREHENSIVE DATA STRUCTURE REPORT:")
    print("=" * 60)
    
    for table_name, structure in data_structures.items():
        if "error" in structure:
            print(f"\n❌ {table_name.upper()}: ERROR - {structure['error']}")
            continue
            
        print(f"\n📋 TABLE: {table_name.upper()}")
        print(f"   Rows: {structure['total_rows']:,}")
        print(f"   Columns: {structure['total_columns']}")
        
        if structure.get("date_range"):
            print(f"   Date Range: {structure['date_range']}")
        
        print("\n   COLUMNS:")
        for col_name, col_info in structure["columns"].items():
            null_pct = (col_info["null_count"] / structure["total_rows"]) * 100 if structure["total_rows"] > 0 else 0
            print(f"     • {col_name}")
            print(f"       Type: {col_info['dtype']} | Nulls: {null_pct:.1f}% | Unique: {col_info['unique_count']}")
            if col_info.get("is_identifier"):
                print(f"       🔑 IDENTIFIER COLUMN")
            if structure["sample_data"].get(col_name):
                sample_str = str(structure["sample_data"][col_name][:2])
                if len(sample_str) > 50:
                    sample_str = sample_str[:50] + "..."
                print(f"       Sample: {sample_str}")
        
        print()
    
    # Print relationships
    print("\n🔗 RELATIONSHIP ANALYSIS:")
    print("-" * 40)
    
    print("\nCommon Columns Across Tables:")
    for col, tables in relationships["common_columns"].items():
        if len(tables) > 1:
            print(f"  • {col}: {', '.join(tables)}")
    
    print("\nCUSIP/Security Identifier Columns:")
    for table, cusip_cols in relationships["cusip_relationships"].items():
        print(f"  • {table}: {', '.join(cusip_cols)}")
    
    # Save detailed analysis to JSON
    output_file = "data_structure_analysis.json"
    analysis_output = {
        "timestamp": datetime.now().isoformat(),
        "data_structures": data_structures,
        "relationships": relationships,
        "summary": {
            "total_tables": len(data_structures),
            "total_rows": sum(s.get("total_rows", 0) for s in data_structures.values()),
            "common_columns": len(relationships["common_columns"]),
            "tables_with_cusip": len(relationships["cusip_relationships"])
        }
    }
    
    with open(output_file, 'w') as f:
        json.dump(analysis_output, f, indent=2, default=str)
    
    print(f"\n💾 Detailed analysis saved to: {output_file}")
    
    # Generate database recommendations
    print("\n💡 DATABASE DESIGN RECOMMENDATIONS:")
    print("=" * 60)
    
    print("""
🏗️  RECOMMENDED TABLE STRUCTURE FOR SUPABASE:

1. 📊 UNIVERSE (Main Reference Table)
   • Primary Key: CUSIP
   • Contains: Security master data, static attributes
   • Central table that other tables reference

2. 📈 PORTFOLIO (Holdings Data)  
   • Primary Key: Composite (Date, CUSIP, Account/Portfolio)
   • Foreign Key: CUSIP → Universe.CUSIP
   • Contains: Position data, valuations, portfolio allocations

3. 📊 RUNS (Trading/Transaction Data)
   • Primary Key: Composite (Date, CUSIP, specific run identifier)
   • Foreign Key: CUSIP → Universe.CUSIP  
   • Contains: Trade execution data, transaction details

4. 📈 G_SPREAD (Historical Spread Data)
   • Primary Key: Composite (Date, Security)
   • Foreign Key: Security → Universe mapping (may need bridge table)
   • Contains: Time series spread data, market data

🔗 KEY RELATIONSHIPS:
   • Universe.CUSIP ← Portfolio.CUSIP (1:many)
   • Universe.CUSIP ← Runs.CUSIP (1:many)
   • Universe.Security ← G_Spread.Security (1:many, may need mapping)

⚠️  IMPORTANT CONSIDERATIONS:
   • G_Spread uses 'Security' field instead of CUSIP
   • May need mapping table: Security ↔ CUSIP
   • Time series data will be large - consider partitioning
   • Financial precision: Use DECIMAL for monetary values
   • Audit trails: Track all changes with timestamps
   • Data validation: Implement constraints and checks
    """)

if __name__ == "__main__":
    main() 