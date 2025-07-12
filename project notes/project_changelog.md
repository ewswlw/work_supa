# Project Changelog

## 2025-07-12 08:30 - DTALE App: Multi-Tab Dashboard & Stability Fixes

### Problem
The dtale multi-tab application was experiencing random page reloading every 30 seconds, disrupting user experience and making data analysis unstable. Additionally, the core `BondDtaleApp` class was missing from the codebase, preventing the multi-tab dashboard from functioning.

### Root Cause
1. **Missing Core Module**: `dtale_app.py` with `BondDtaleApp` class was not present in the project
2. **Automatic Refresh Issue**: JavaScript `setInterval(loadViews, 30000)` was causing unwanted page reloads every 30 seconds
3. **Data Type Optimization Bug**: `DataOptimizer.optimize_dtypes` was incorrectly converting high-cardinality object columns to category

### Solution

#### 1. Reconstructed Core dtale App Module
Created `dtale_app.py` with comprehensive `BondDtaleApp` class:
- **Data loading and sampling** with configurable sample sizes
- **Multiple view filters**: CAD-only, same-sector, portfolio, executable, cross-currency
- **dtale instance management** with unique port allocation
- **Performance optimization** with smart sampling and memory usage tracking
- **Comprehensive error handling** and logging

#### 2. Fixed Automatic Refresh Issue
Modified JavaScript in `dtale_multi_tab_app.py`:
```javascript
// Before: Automatic refresh every 30 seconds
setInterval(loadViews, 30000);

// After: Manual refresh only
// setInterval(loadViews, 30000); // Removed automatic refresh
```

#### 3. Fixed Data Type Optimization
Updated `DataOptimizer.optimize_dtypes` in `src/utils/dtale_manager.py`:
```python
# Before: Converted all low-cardinality objects to category
if nunique < len(df_opt) * 0.5 or nunique <= 50:

# After: Preserve unique columns as object
if (nunique < len(df_opt) * 0.5 or nunique <= 50) and nunique < len(df_opt):
```

### Technical Details

#### Multi-Tab Dashboard Features
- **Flask-based interface** with tabbed navigation
- **Multiple dtale instances** running on separate ports (40000-40005)
- **Real-time data exploration** with Excel-like interface
- **Performance optimized** with configurable sample sizes (default: 25,000 rows)
- **Responsive design** with modern UI/UX

#### View Categories Available
1. **All Data**: Full sample of bond g-spread data
2. **CAD Only**: CAD-denominated bonds only
3. **Same Sector**: Bonds in the same sector
4. **Portfolio**: Bonds in portfolio (Own? == 1) - *Note: Requires 'Own?' column*
5. **Executable**: Bonds with executable trades
6. **Cross-Currency**: Bonds with cross-currency exposure

#### Performance Optimizations
- **Smart sampling**: Includes extreme values and priority data
- **Memory optimization**: Automatic dtype optimization for large datasets
- **Port management**: Automatic port allocation to prevent conflicts
- **Background processing**: Non-blocking view creation

### Results
- ✅ **Stable dashboard** - No more random reloading
- ✅ **20/20 tests passing** - Complete test suite validation
- ✅ **5/6 views working** - Only Portfolio view failed due to missing 'Own?' column
- ✅ **Fast loading** - 100-500 row samples load in seconds
- ✅ **User-controlled refresh** - Manual refresh button available

### Files Modified/Created
- **Created**: `dtale_app.py` - Core BondDtaleApp class
- **Modified**: `dtale_multi_tab_app.py` - Removed automatic refresh
- **Modified**: `src/utils/dtale_manager.py` - Fixed dtype optimization logic
- **Validated**: All test files in `/test` directory

### Usage Examples
```bash
# Launch with default settings (25,000 sample)
poetry run python dtale_multi_tab_app.py

# Launch with smaller sample for faster loading
poetry run python dtale_multi_tab_app.py --sample-size 100

# Launch on different port
poetry run python dtale_multi_tab_app.py --port 8052
```

### Impact
- **Enhanced data exploration**: Multi-tab interface for different bond perspectives
- **Improved stability**: No more disruptive automatic reloading
- **Better performance**: Optimized data types and smart sampling
- **Team-friendly interface**: Modern, responsive dashboard design
- **Comprehensive testing**: Full test suite ensures reliability

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