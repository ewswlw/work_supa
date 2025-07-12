# Project Changelog

## 2025-07-11 21:33 - Currency Correction Fix

### Problem
The bond_z.parquet file was missing CUSIP_1 and CUSIP_2 columns, and USD bonds were being incorrectly labeled as CAD due to fuzzy matching issues in the G-spread processor. The raw data included USD bonds like `HUSMID 4.1 12/02/29`, but the portfolio had `HUSMID 4.1 12/29 CA` (CAD), causing fuzzy matching to map USD bonds to CAD bonds and lose the original USD currency information.

### Root Cause
The `enrich_with_universe_data` function in `historical g spread/g_z.py` was:
1. Creating temporary CUSIP_1 and CUSIP_2 columns for universe matching
2. Mapping Currency information from universe data (mostly CAD)
3. **Removing the CUSIP columns** after enrichment
4. Not preserving original currency information from raw data

### Solution
Modified the `enrich_with_universe_data` function in `historical g spread/g_z.py` to:

1. **Keep CUSIP_1 and CUSIP_2 columns** instead of removing them
2. **Add currency detection logic** based on security name patterns:
   - USD patterns: `/29`, `/30`, `/31`, etc. (year patterns without CA suffix)
   - CAD patterns: ` CA`, ` CA)` (explicit CAD suffixes)
   - USD patterns: ` US` (explicit USD suffixes)
3. **Override universe currency** with detected currency when mismatches are found
4. **Log currency corrections** for transparency

### Technical Details
- **Currency detection patterns**: Extended year patterns (2029-2060) for USD bonds
- **Correction logic**: Compares detected currency vs universe currency, overrides when different
- **Performance**: Vectorized operations for efficiency
- **Logging**: Reports number of corrections for each security

### Results
- **343 currency corrections** for Security_1
- **167 currency corrections** for Security_2
- **CUSIP_1 and CUSIP_2 columns** now preserved in bond_z.parquet
- **Proper currency distribution**: 814 CAD + 47 USD for Security_1, 785 CAD + 76 USD for Security_2
- **Accurate currency matching**: USD bonds correctly identified as USD, CAD bonds as CAD

### Files Modified
- `historical g spread/g_z.py`: Enhanced `enrich_with_universe_data` function with currency correction logic

### Testing
- Verified bond_z.parquet now contains CUSIP_1 and CUSIP_2 columns
- Confirmed currency distribution shows both CAD and USD bonds
- Validated sample data shows proper currency matching (e.g., "C 5.07 04/28 US" → USD, "HOMEQU 6.552 10/27 C" → CAD)

### Impact
- **Data integrity**: Currency information now accurately reflects original bond characteristics
- **Analytics accuracy**: Cross-currency analysis now based on correct currency data
- **Downstream compatibility**: CUSIP columns available for additional universe matching
- **Audit trail**: Comprehensive logging of currency corrections for transparency 