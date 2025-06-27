import pandas as pd
from pathlib import Path
from rapidfuzz import process, fuzz
import numpy as np

# Paths
csv_path = Path(__file__).parent / 'raw data' / 'bond_g_sprd_time_series.csv'
parquet_path = Path(__file__).parent / 'bond_g_sprd_time_series.parquet'
universe_parquet_path = Path(__file__).parent.parent / 'universe' / 'universe.parquet'
fuzzy_output_path = Path(__file__).parent / 'fuzzy_bond_security_mapping.csv'

# Load CSV
df = pd.read_csv(csv_path)
print("\n=== Loaded DataFrame from CSV ===")
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# Load universe Security names
universe = pd.read_parquet(universe_parquet_path, engine='pyarrow')
security_names = set(universe['Security'].dropna().unique())

# Identify bond columns
bond_columns = set(df.columns) - {'DATE'}

# Unique fuzzy mapping
available_securities = set(security_names)
rename_map = {}
mapping_report = []
unmapped_bonds = []
for bond in bond_columns:
    if bond in available_securities:
        # Already matches, keep as is and remove from available
        available_securities.remove(bond)
        mapping_report.append((bond, bond, 100.0))
    else:
        # Fuzzy match only among unused Security names
        if available_securities:
            match, score, _ = process.extractOne(bond, available_securities, scorer=fuzz.ratio)
            if score >= 85:
                rename_map[bond] = match
                available_securities.remove(match)
                mapping_report.append((bond, match, score))
                print(f"Renaming: {bond} -> {match} (Similarity: {score:.1f}%)")
            else:
                unmapped_bonds.append(bond)
        else:
            unmapped_bonds.append(bond)

# Apply renaming
df_renamed = df.rename(columns=rename_map)

# Check for duplicate columns after renaming
cols = list(df_renamed.columns)
duplicates = set([col for col in cols if cols.count(col) > 1])
dropped_columns = []
if duplicates:
    print("\n=== Duplicate Columns After Renaming (will drop all but first) ===")
    for dup in duplicates:
        # Find all indices of this column name except the first
        indices = [i for i, c in enumerate(cols) if c == dup]
        # Keep the first, drop the rest
        for idx in indices[1:]:
            dropped_columns.append((dup, idx))
    print(f"Dropping {len(dropped_columns)} duplicate columns:")
    for col, idx in dropped_columns:
        print(f"  - {col} (column index {idx})")
    # Actually drop the duplicates
    # Keep only the first occurrence of each column
    _, first_indices = np.unique(cols, return_index=True)
    df_renamed = df_renamed.iloc[:, sorted(first_indices)]
else:
    print("\nNo duplicate columns after renaming.")

# Print mapping summary
print("\n=== Mapping Summary ===")
for orig, new, score in mapping_report:
    if orig == new:
        print(f"Kept:    {orig}")
    else:
        print(f"Mapped:  {orig} -> {new} (Similarity: {score:.1f}%)")

if unmapped_bonds:
    print("\n=== Unmapped Bonds (no unique match >=85%) ===")
    for bond in unmapped_bonds:
        print(f"  - {bond}")
else:
    print("\nAll bond columns mapped to unique Security names.")

# Save to Parquet with renamed columns
df_renamed.to_parquet(parquet_path, index=False)
print(f"\nSaved DataFrame to Parquet: {parquet_path}")

# Also save to CSV in processed data folder
csv_out_path = Path(__file__).parent / 'processed data' / 'bond_g_sprd_processed.csv'
df_renamed.to_csv(csv_out_path, index=False)
print(f"Saved DataFrame to CSV: {csv_out_path}")

# Reload from Parquet for inspection
df_parquet = pd.read_parquet(parquet_path)
print("\n=== DataFrame Info (from Parquet) ===")
print(df_parquet.info())

print("\n=== Head (first 10 rows) ===")
print(df_parquet.head(10))

print("\n=== Tail (last 10 rows) ===")
print(df_parquet.tail(10))

print("\n=== Null Value Counts ===")
print(df_parquet.isnull().sum())

print("\n=== Descriptive Statistics (numeric columns) ===")
print(df_parquet.describe())

# Check for any columns still not matching Security names
after_bond_columns = set(df_parquet.columns) - {'DATE'}
still_missing = sorted([col for col in after_bond_columns if col not in security_names])
print("\n=== Bonds STILL NOT found in Security (after renaming) ===")
print(f"Count: {len(still_missing)}")
if still_missing:
    print("First 20 non-matching bonds:")
    for bond in still_missing[:20]:
        print(f"  - {bond}")
else:
    print("All bond columns now match Security names!")

# --- Bond name matching section ---
try:
    universe = pd.read_parquet(universe_parquet_path, engine='pyarrow')
    security_names = set(universe['Security'].dropna().unique())
    bond_columns = set(df_parquet.columns) - {'DATE'}
    missing_bonds = sorted([col for col in bond_columns if col not in security_names])
    print("\n=== Bonds NOT found in Security (universe.parquet) ===")
    print(f"Total bonds in G spread file: {len(bond_columns)}")
    print(f"Total unique Security names in universe: {len(security_names)}")
    print(f"Bonds not found in universe: {len(missing_bonds)}")
    print("First 20 missing bonds:")
    for bond in missing_bonds[:20]:
        print(f"  - {bond}")

    # --- Fuzzy matching section ---
    print("\n=== Fuzzy Matching Results (threshold: 85%) ===")
    fuzzy_results = []
    for bond in missing_bonds:
        match, score, _ = process.extractOne(
            bond, security_names, scorer=fuzz.ratio
        )
        if score >= 85:
            print(f"Bond: {bond}\n  Best Match: {match}\n  Similarity: {score:.1f}%\n")
        fuzzy_results.append({
            'bond_column': bond,
            'best_match': match,
            'similarity': score
        })
    # Summary
    good_matches = [r for r in fuzzy_results if r['similarity'] >= 85]
    print(f"\nTotal bonds with >=85% match: {len(good_matches)} / {len(missing_bonds)}")
except Exception as e:
    print(f"\n[ERROR] Could not compare bonds to universe.parquet: {e}") 