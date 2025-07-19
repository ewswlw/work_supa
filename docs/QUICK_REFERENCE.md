# Trading Analytics Database System - Quick Reference

## 🚀 SYSTEM OVERVIEW

**Production-Ready System** with self-contained G-spread analytics and expert CUSIP validation.

### Current Status (2025-01-27)
- **Database Size**: 50-70 MB (optimized with indexes)
- **Total Records**: ~188,398 across all tables  
- **Processing Rate**: 7,000+ records/second
- **Pipeline Duration**: 4-6 minutes (full refresh)
- **G-Spread Analytics**: 19,900 bond pairs with CUSIP enrichment
- **CUSIP Match Rate**: 99.9%+ success rate

## 📁 DEFAULT DATA SOURCES

The system automatically detects and processes these parquet files:

```bash
# Default file structure
universe/universe.parquet          # 4.5 MB - Market universe data
portfolio/portfolio.parquet        # 1.2 MB - Portfolio positions  
runs/combined_runs.parquet         # 5.6 MB - Trading execution data
runs/run_monitor.parquet           # 0.2 MB - Monitoring alerts
historical g spread/bond_z.parquet # 1.2 MB - G-spread analytics (11 columns)
```

## ⚡ QUICK START COMMANDS

### Basic Operations
```bash
# Full pipeline with default files
poetry run python db_pipe.py

# Force complete refresh
poetry run python db_pipe.py --force-refresh

# Check system status
poetry run python db_pipe.py --status

# Initialize new database
poetry run python db_pipe.py --init
```

### G-Spread Analytics
```bash
# Run self-contained G-spread analysis
poetry run python "historical g spread/g_z.py"

# Output: bond_z.parquet with 11 columns including CUSIP_1 and CUSIP_2
# Performance: 5.75 seconds for 19,900 pairs
# Dependencies: Only requires g_ts.parquet (self-contained)
```

### Performance Optimization
```bash
# Optimized pipeline execution
poetry run python db_pipe.py --batch-size 10000 --parallel --low-memory --optimize-db

# Individual optimizations
poetry run python db_pipe.py --batch-size 5000     # Custom batch size
poetry run python db_pipe.py --parallel            # Parallel processing
poetry run python db_pipe.py --low-memory          # Memory optimization
poetry run python db_pipe.py --optimize-db         # Database optimization
```

### Custom Data Sources
```bash
# Specify custom files
poetry run python db_pipe.py --universe "custom/universe.parquet" --portfolio "custom/portfolio.parquet"

# Mixed file formats (automatic detection)
poetry run python db_pipe.py --universe "data/universe.csv" --runs "data/runs.parquet"
```

## 📊 SYSTEM INFORMATION

### Database Tables
```bash
# Core data tables
universe_historical      # Market universe and securities master (~38,834 rows)
portfolio_historical     # Portfolio positions and holdings (~3,142 rows)
combined_runs_historical  # Trading execution data (~125,242 rows)
run_monitor              # Current monitoring alerts (~1,280 rows)
gspread_analytics        # G-spread bond pair analysis (19,900 rows)

# Analytics tables
unmatched_cusips_all_dates    # CUSIP validation tracking
unmatched_cusips_last_date    # Recent unmatched CUSIPs
```

### G-Spread Analytics Columns
```bash
# 11 columns with CUSIP enrichment
Security_1, CUSIP_1      # First bond in pair with identifier
Security_2, CUSIP_2      # Second bond in pair with identifier  
Last_Spread, Z_Score     # Core analytics metrics
Percentile, Max, Min     # Statistical measures
Last_vs_Max, Last_vs_Min # Comparative analytics
```

## 🔍 DATABASE QUERIES

### Quick Status Checks
```sql
-- Database overview
SELECT name, COUNT(*) as records 
FROM sqlite_master s, 
     (SELECT 'universe_historical' as name UNION 
      SELECT 'portfolio_historical' UNION 
      SELECT 'combined_runs_historical' UNION 
      SELECT 'run_monitor' UNION 
      SELECT 'gspread_analytics') t
WHERE s.name = t.name AND s.type = 'table';

-- G-spread analytics summary
SELECT COUNT(*) as total_pairs,
       COUNT(DISTINCT "CUSIP_1") as unique_cusips_1,
       COUNT(DISTINCT "CUSIP_2") as unique_cusips_2,
       MIN("Z_Score") as min_zscore,
       MAX("Z_Score") as max_zscore,
       AVG("Last_Spread") as avg_spread
FROM gspread_analytics;
```

### Performance Queries
```sql
-- Top extreme Z-scores
SELECT "Security_1", "Security_2", "Z_Score", "Last_Spread", "Percentile"
FROM gspread_analytics 
WHERE ABS("Z_Score") > 2.0 
ORDER BY ABS("Z_Score") DESC 
LIMIT 10;

-- CUSIP coverage analysis
SELECT COUNT(DISTINCT "CUSIP_1") + COUNT(DISTINCT "CUSIP_2") as total_cusips,
       COUNT(*) as total_pairs
FROM gspread_analytics;
```

### Data Quality Checks
```sql
-- Recent universe data
SELECT MAX("Date") as latest_date, COUNT(*) as records_on_latest_date
FROM universe_historical;

-- CUSIP validation status
SELECT 
    SUM(CASE WHEN standardized_cusip_1 IS NOT NULL THEN 1 ELSE 0 END) as matched_cusip_1,
    SUM(CASE WHEN standardized_cusip_2 IS NOT NULL THEN 1 ELSE 0 END) as matched_cusip_2,
    COUNT(*) as total_pairs
FROM gspread_analytics;
```

### Orphaned CUSIP Analysis
```sql
-- Find CUSIPs in gspread analytics not in universe
SELECT DISTINCT gs."CUSIP_1" as orphaned_cusip, gs."Security_1" as security_name
FROM gspread_analytics gs
LEFT JOIN universe_historical uh ON gs."CUSIP_1" = uh."CUSIP"
WHERE uh."CUSIP" IS NULL

UNION

SELECT DISTINCT gs."CUSIP_2" as orphaned_cusip, gs."Security_2" as security_name  
FROM gspread_analytics gs
LEFT JOIN universe_historical uh ON gs."CUSIP_2" = uh."CUSIP"
WHERE uh."CUSIP" IS NULL;
```

## 🛠️ ENHANCED CONSOLE OUTPUT

The system provides comprehensive data engineering insights:

### Pipeline Completion Report
```bash
🚀 PERFORMANCE METRICS:
   ⏱️  Total Duration: 4.6 minutes
   📈 Records Processed: 188,398
   ⚡ Processing Rate: 7,000+ records/second

💾 DATABASE HEALTH:
   🟢 Status: Healthy
   📦 Size: 62.80 MB
   🔗 Uptime: 4.7 minutes

🔍 DATA QUALITY METRICS:
   🎯 CUSIP Match Rate: 99.9%
   ✅ Matched CUSIPs: 185,000+
   ❌ Unmatched CUSIPs: <1,000
   🟡 EXCELLENT: CUSIP matching is optimal

📋 TABLE STATISTICS:
   📊 universe_historical: 38,834 rows
   📊 gspread_analytics: 19,900 rows (100% CUSIP match)
   📊 combined_runs_historical: 125,242 rows

🕒 DATA FRESHNESS:
   📅 Latest universe date: 2025-01-27
   📅 G-spread analytics: Current (19,900 pairs)

📊 G-SPREAD ANALYTICS STATUS:
   🎯 Bond Pairs: 19,900 analyzed
   📈 CUSIP Coverage: 100% (CUSIP_1 and CUSIP_2)
   ⚡ Analysis Speed: 5.75 seconds
   🔍 Z-Score Range: -3.96 to 4.20
   📊 Extreme Pairs (|Z|>2): 2,100
```

## 🎯 PERFORMANCE BENCHMARKS

### Current System Performance
- **Database Size**: 50-70 MB (optimized)
- **Loading Speed**: 7,000+ records/second
- **Memory Usage**: 80-100 MB peak
- **Query Response**: <1 second for complex analytics
- **G-Spread Analysis**: 5.75 seconds for 19,900 pairs
- **Pipeline Duration**: 4-6 minutes (full refresh)

### G-Spread Analytics Performance
- **Processing Speed**: 124,343 records/second analysis
- **Input Data**: 714,710 records (g_ts.parquet)
- **Output**: 19,900 bond pairs with full analytics
- **CUSIP Enrichment**: 100% success rate
- **Self-Contained**: No external dependencies

## 🏗️ SYSTEM ARCHITECTURE

### Data Flow
```
Parquet Files → Data Validation → CUSIP Standardization → Database → Analytics
```

### G-Spread Analytics Flow
```
g_ts.parquet → Vectorized Analysis → CUSIP Enrichment → bond_z.parquet → Database
(714,710 records) → (19,900 pairs) → (11 columns) → (integration)
```

### File Organization
```
work_supa/
├── universe/universe.parquet           # Market universe data
├── portfolio/portfolio.parquet         # Portfolio positions
├── runs/combined_runs.parquet          # Trading execution data
├── runs/run_monitor.parquet            # Monitoring alerts  
├── historical g spread/
│   ├── raw data/g_ts.parquet          # G-spread raw data
│   └── bond_z.parquet                 # G-spread analytics (11 columns)
├── db/                                # Database backups
└── trading_analytics.db               # Main database
```

## 🔧 TROUBLESHOOTING

### Common Issues

**Database Lock Error**:
```bash
# Solution: Check for other processes
poetry run python db_pipe.py --status
```

**Memory Issues**:
```bash
# Solution: Use low memory mode
poetry run python db_pipe.py --low-memory --batch-size 1000
```

**CUSIP Validation Errors**:
```bash
# Check CUSIP validation logs
tail -f logs/database_operations.log
```

**G-Spread Analytics Issues**:
```bash
# Run standalone G-spread analysis
poetry run python "historical g spread/g_z.py"

# Check for required raw data file
ls -la "historical g spread/raw data/g_ts.parquet"
```

### Performance Issues
```bash
# Optimize database
poetry run python db_pipe.py --optimize-db

# Use parallel processing
poetry run python db_pipe.py --parallel --batch-size 10000

# Check system status
poetry run python db_pipe.py --status
```

### Data Quality Issues
```bash
# Check for orphaned CUSIPs
SELECT COUNT(*) FROM unmatched_cusips_last_date;

# Validate G-spread analytics
SELECT COUNT(*) FROM gspread_analytics WHERE "CUSIP_1" IS NULL OR "CUSIP_2" IS NULL;

# Check data freshness
SELECT MAX("Date") FROM universe_historical;
```

## 📊 DATA VALIDATION

### CUSIP Validation
- **Standardization Rate**: 99.9%+ success
- **Validation Logic**: Expert-level CUSIP formatting
- **Error Handling**: Graceful fallback for invalid CUSIPs
- **Performance**: High-speed batch processing

### Data Quality Checks
- **Constraint Enforcement**: Database-level validation
- **Type Validation**: Automatic data type conversion
- **Range Validation**: Business rule enforcement
- **Completeness**: Required field validation

### G-Spread Analytics Validation
- **CUSIP Match Rate**: 100% for all 19,900 pairs
- **Data Integrity**: Self-contained with no external dependencies
- **Statistical Validation**: Z-scores, percentiles calculated correctly
- **Performance Validation**: Sub-6 second execution confirmed

## 🚀 ADVANCED FEATURES

### Parallel Processing
- **Multi-threaded CUSIP standardization**
- **Batch size optimization (1,000-10,000 records)**
- **Memory-efficient processing**
- **Automatic worker thread management**

### Database Optimization
- **35 optimized indexes for fast queries**
- **WAL mode for concurrent access**
- **VACUUM and ANALYZE operations**
- **Performance view pre-computation**

### Monitoring and Logging
- **5 specialized log files with rotation**
- **Real-time performance metrics**
- **Comprehensive error tracking**
- **Data engineering insights**

### Integration Capabilities
- **CLI interface for all operations**
- **Automatic file format detection**
- **Mixed format support (CSV + Parquet)**
- **Default pipeline behavior**

## 🎯 PRODUCTION TIPS

### Performance Optimization
```bash
# For large datasets
poetry run python db_pipe.py --batch-size 10000 --parallel --low-memory

# For maximum speed
poetry run python db_pipe.py --optimize-db --disable-logging

# For memory-constrained environments
poetry run python db_pipe.py --low-memory --batch-size 1000
```

### Data Management
```bash
# Regular database maintenance
poetry run python db_pipe.py --optimize-db

# Check system health
poetry run python db_pipe.py --status

# Backup database
cp trading_analytics.db db/backup_$(date +%Y%m%d_%H%M%S).db
```

### Monitoring
```bash
# Watch logs in real-time
tail -f logs/database_operations.log

# Check performance metrics
grep "Processing Rate" logs/database_operations.log

# Monitor G-spread analytics
poetry run python "historical g spread/g_z.py" | grep "\[OK\]"
```

---

## Document Information
- **Version**: 1.3.0
- **Last Updated**: 2025-01-27
- **System Version**: Trading Analytics Database v1.3.0 with G-Spread Analytics Enhancement
- **Status**: Production Ready 