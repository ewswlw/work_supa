# Database Learning Notes

## 2025-01-27 - Database Safety & Pipeline Refresh Behavior

### 🔒 **Critical Learning: Custom Table Protection**

**Custom Tables Created**:
- `bid_spread_changes_analysis` - WoW/MoM bid spread analysis (125,235 records)

### 🛡️ **Pipeline Refresh Behavior - What I Learned**

**What Happens When You Re-run `@db_pipe.py`**:

**✅ SAFE Operations** (Normal Pipeline):
- **Custom tables are PRESERVED** - `bid_spread_changes_analysis` stays intact
- **Schema validation** only checks predefined tables, ignores custom tables
- **Incremental updates** to core tables (universe, portfolio, runs, etc.)
- **No data loss** for custom analysis tables

**❌ DANGEROUS Operations**:
- `--force-full-refresh` flag **DELETES ENTIRE DATABASE**
- **All custom tables lost** including `bid_spread_changes_analysis`
- **Complete recreation** of all tables from scratch

### 📋 **Schema Validation Logic - Technical Understanding**

**Expected Tables** (from `db/database/schema.py`):
```python
expected_tables = set(self.core_tables.keys()) | set(self.cusip_tracking_tables.keys()) | set(self.audit_tables.keys())
```

**Core Tables**: `universe_historical`, `portfolio_historical`, `combined_runs_historical`, `run_monitor`, `gspread_analytics`
**Tracking Tables**: `unmatched_cusips_all_dates`, `unmatched_cusips_last_date`
**Audit Tables**: `data_quality_log`, `processing_metadata`, `schema_version_history`

**Custom Tables** (NOT in schema - SAFE):
- `bid_spread_changes_analysis` ✅
- Any other custom analysis tables ✅

### 🛡️ **Best Practices for Database Safety**

**Before Running Pipeline**:
```bash
# Always backup before major operations
cp trading_analytics.db trading_analytics_backup_$(date +%Y%m%d_%H%M%S).db
```

**Safe Pipeline Operations**:
```bash
# Safe - preserves custom tables
poetry run python db_pipe.py

# Safe - incremental updates only
poetry run python db_pipe.py --incremental
```

**Dangerous Operations** (avoid unless necessary):
```bash
# DANGEROUS - deletes entire database including custom tables
poetry run python db_pipe.py --force-full-refresh
```

### 🔍 **Verification Commands - How to Check**

**Check Custom Tables Exist**:
```sql
SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN (
    'universe_historical', 'portfolio_historical', 'combined_runs_historical', 
    'run_monitor', 'gspread_analytics', 'unmatched_cusips_all_dates', 
    'unmatched_cusips_last_date', 'data_quality_log', 'processing_metadata', 
    'schema_version_history', 'sqlite_sequence'
);
```

**Check Analysis Table Data**:
```sql
SELECT COUNT(*) as total_records FROM bid_spread_changes_analysis;
```

### 💡 **Key Learnings**

1. **Custom tables are SAFE** during normal pipeline operations
2. **Schema validation ignores** tables not in the predefined schema
3. **Only force refresh** deletes custom tables
4. **Always backup** before major operations
5. **Incremental updates** are the safest approach

### 🎯 **Why This Matters**

**Database Safety**: Understanding how the pipeline handles custom tables prevents data loss
**Confidence**: Can run pipeline updates without worrying about losing analysis work
**Best Practices**: Proper backup and verification procedures
**Technical Understanding**: How schema validation works in practice

**Status**: ✅ **DATABASE SAFETY PROTOCOLS LEARNED AND DOCUMENTED**
