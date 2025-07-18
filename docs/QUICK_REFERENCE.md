# Quick Reference Guide

## 🚀 Common Commands

### Database Operations
```bash
# Initialize database
poetry run python db_pipe.py --init --database trading_analytics.db

# Run full pipeline with default files (recommended)
poetry run python db_pipe.py

# Run with all optimizations for maximum performance
poetry run python db_pipe.py --force-refresh --batch-size 10000 --parallel --low-memory --optimize-db --disable-logging

# Load specific data sources
poetry run python db_pipe.py --universe universe/universe.parquet
poetry run python db_pipe.py --portfolio portfolio/portfolio.parquet
poetry run python db_pipe.py --runs runs/combined_runs.parquet
poetry run python db_pipe.py --run-monitor runs/run_monitor.parquet
poetry run python db_pipe.py --gspread-analytics "historical g spread/bond_z.parquet"

# Check system status
poetry run python db_pipe.py --status --database trading_analytics.db

# Force full refresh of all data
poetry run python db_pipe.py --force-refresh

# Performance optimization options
poetry run python db_pipe.py --batch-size 10000  # Increase batch size
poetry run python db_pipe.py --parallel          # Enable parallel processing
poetry run python db_pipe.py --low-memory        # Enable low memory mode
poetry run python db_pipe.py --optimize-db       # Optimize database after loading
poetry run python db_pipe.py --disable-logging   # Reduce logging for speed

# Enhanced console output with data engineering insights
poetry run python db_pipe.py --status --verbose
```

### Performance Analysis
```bash
# Run test suite
poetry run pytest tests/ -v

# Check database performance
poetry run python -c "
import sqlite3
import time
conn = sqlite3.connect('trading_analytics.db')
start = time.time()
result = conn.execute('SELECT COUNT(*) FROM combined_runs_historical').fetchone()
print(f'Query time: {time.time() - start:.4f}s, Records: {result[0]}')
conn.close()
"
```

### Database Queries
```sql
-- Check table sizes
SELECT 'universe_historical' as table_name, COUNT(*) as count FROM universe_historical
UNION ALL
SELECT 'portfolio_historical', COUNT(*) FROM portfolio_historical
UNION ALL
SELECT 'combined_runs_historical', COUNT(*) FROM combined_runs_historical
UNION ALL
SELECT 'gspread_analytics', COUNT(*) FROM gspread_analytics;

-- Daily summary (using performance view)
SELECT * FROM v_daily_summary ORDER BY date DESC LIMIT 10;

-- Top dealers
SELECT * FROM v_dealer_performance LIMIT 10;

-- Active CUSIPs
SELECT * FROM v_cusip_activity LIMIT 10;

-- Recent trading activity
SELECT date, COUNT(*) as runs, COUNT(DISTINCT cusip_standardized) as cusips
FROM combined_runs_historical
WHERE date >= '2025-07-01'
GROUP BY date
ORDER BY date DESC;
```

## 🔧 Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check database file exists
ls -la trading_analytics.db

# Check database integrity
poetry run python -c "
import sqlite3
conn = sqlite3.connect('trading_analytics.db')
result = conn.execute('PRAGMA integrity_check').fetchone()
print('Integrity:', result[0])
conn.close()
"

# Check database permissions
ls -la trading_analytics.db*
```

#### Performance Issues
```bash
# Analyze query performance
poetry run python analyze_performance.py

# Check database size
ls -lh trading_analytics.db

# Check memory usage during operations
poetry run python -c "
import psutil
print(f'Memory: {psutil.virtual_memory().percent}%')
print(f'Available: {psutil.virtual_memory().available / 1024 / 1024:.1f} MB')
"
```

#### Data Loading Errors
```bash
# Check log files
tail -f logs/db.log
tail -f logs/error.log

# Check parquet file integrity
poetry run python -c "
import pandas as pd
try:
    df = pd.read_parquet('universe/universe.parquet')
    print(f'File OK: {len(df)} records, {len(df.columns)} columns')
except Exception as e:
    print(f'Error: {e}')
"
```

### Error Messages and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `database is locked` | Concurrent access | Wait for other operations to complete |
| `no such table` | Schema not initialized | Run `--init` command |
| `CHECK constraint failed` | Data validation error | Check data quality and constraints |
| `type 'Timestamp' is not supported` | SQLite compatibility | Data conversion issue (fixed in code) |
| `memory allocation failed` | Insufficient memory | Reduce batch size or increase memory |

## 📊 Performance Tips

### ⚡ **Performance Optimization Commands**
```bash
# Full optimization suite (5.4x faster execution)
poetry run python db_pipe.py --force-refresh --batch-size 10000 --parallel --low-memory --optimize-db --disable-logging

# Individual optimizations
poetry run python db_pipe.py --batch-size 5000  # Batch size only
poetry run python db_pipe.py --parallel          # Parallel processing only
poetry run python db_pipe.py --low-memory        # Low memory mode only
poetry run python db_pipe.py --optimize-db       # Database optimization only
```

### Query Optimization
```sql
-- Use indexes effectively
EXPLAIN QUERY PLAN SELECT * FROM combined_runs_historical WHERE date = '2025-07-11';

-- Use performance views for common queries
SELECT * FROM v_daily_summary WHERE date >= '2025-07-01';

-- Limit result sets
SELECT * FROM combined_runs_historical LIMIT 1000;

-- Use specific columns instead of *
SELECT date, cusip_standardized, dealer FROM combined_runs_historical;
```

### Database Maintenance
```sql
-- Update statistics for better query planning
ANALYZE;

-- Optimize database space
VACUUM;

-- Check index usage
SELECT * FROM sqlite_stat1 WHERE tbl = 'combined_runs_historical';
```

### Memory Management
```bash
# Monitor memory usage
poetry run python -c "
import psutil
import os
process = psutil.Process(os.getpid())
print(f'Memory: {process.memory_info().rss / 1024 / 1024:.1f} MB')
"

# Check database cache settings
poetry run python -c "
import sqlite3
conn = sqlite3.connect('trading_analytics.db')
print('Cache size:', conn.execute('PRAGMA cache_size').fetchone()[0])
print('MMAP size:', conn.execute('PRAGMA mmap_size').fetchone()[0])
conn.close()
"
```

## 📊 Enhanced Console Output

### Data Engineering Insights
The system now provides comprehensive data engineering insights including:

**Pipeline Completion Report**:
- Data quality metrics and validation results
- Processing performance statistics
- CUSIP standardization success rates
- Orphaned CUSIP analysis (CUSIPs in other tables but not in universe)
- Last universe date coverage analysis
- Memory usage and database size tracking

**Status Command Features**:
- Real-time database health metrics
- Table record counts and data ranges
- Performance view summaries
- Orphaned CUSIP identification
- Data quality indicators

### Usage Examples
```bash
# Full pipeline with enhanced output
poetry run python db_pipe.py

# Status check with orphaned CUSIP analysis
poetry run python db_pipe.py --status

# Verbose status with detailed insights
poetry run python db_pipe.py --status --verbose
```

## 📈 Monitoring Commands

### System Health
```bash
# Check all log files
for log in logs/*.log; do
    echo "=== $log ==="
    tail -5 "$log"
    echo
done

# Check database status
poetry run python db_pipe.py --status

# Monitor real-time logs
tail -f logs/db.log | grep -E "(ERROR|WARNING|CRITICAL)"
```

### Performance Monitoring
```bash
# Quick performance check
poetry run python -c "
import sqlite3
import time
conn = sqlite3.connect('trading_analytics.db')
start = time.time()
conn.execute('SELECT COUNT(*) FROM combined_runs_historical').fetchone()
print(f'Query time: {(time.time() - start)*1000:.2f}ms')
conn.close()
"

# Check index effectiveness
poetry run python -c "
import sqlite3
conn = sqlite3.connect('trading_analytics.db')
cursor = conn.cursor()
cursor.execute('SELECT name, sql FROM sqlite_master WHERE type=\"index\"')
indexes = cursor.fetchall()
print(f'Total indexes: {len(indexes)}')
for name, sql in indexes[:5]:
    print(f'  {name}')
conn.close()
"
```

## 🔍 Debugging Commands

### Data Quality Checks
```sql
-- Check for unmatched CUSIPs
SELECT COUNT(*) FROM unmatched_cusips_last_date;

-- Check for orphaned CUSIPs (in other tables but not in universe)
SELECT 
    table_name,
    COUNT(*) as orphaned_count
FROM (
    SELECT 'portfolio' as table_name, "CUSIP" as cusip FROM portfolio_historical
    UNION ALL
    SELECT 'combined_runs' as table_name, "CUSIP" as cusip FROM combined_runs_historical
    UNION ALL
    SELECT 'run_monitor' as table_name, "CUSIP" as cusip FROM run_monitor
    UNION ALL
    SELECT 'gspread_analytics' as table_name, "CUSIP_1" as cusip FROM gspread_analytics
    UNION ALL
    SELECT 'gspread_analytics' as table_name, "CUSIP_2" as cusip FROM gspread_analytics
) all_cusips
WHERE cusip NOT IN (SELECT "CUSIP" FROM universe_historical)
GROUP BY table_name;

-- Check data ranges
SELECT 
    MIN("Date") as min_date,
    MAX("Date") as max_date,
    COUNT(DISTINCT "Date") as unique_dates
FROM combined_runs_historical;

-- Check for null values
SELECT 
    COUNT(*) as total_rows,
    COUNT("CUSIP") as non_null_cusips,
    COUNT("Dealer") as non_null_dealers
FROM combined_runs_historical;
```

### Log Analysis
```bash
# Find errors in logs
grep -i error logs/*.log | tail -10

# Find slow operations
grep -E "duration|time" logs/db.log | tail -10

# Find memory usage patterns
grep "memory_usage_mb" logs/db.log | tail -10
```

## 📋 Configuration Reference

### Database Settings
```sql
-- Current settings
PRAGMA journal_mode;      -- WAL
PRAGMA synchronous;       -- NORMAL
PRAGMA cache_size;        -- 64000 (64MB)
PRAGMA mmap_size;         -- 268435456 (256MB)
PRAGMA temp_store;        -- MEMORY
PRAGMA foreign_keys;      -- ON
```

### Logging Configuration
```python
# Log file locations
logs/
├── db.log              # Database operations
├── cusip.log           # CUSIP standardization
├── pipeline.log        # Pipeline execution
├── quality.log         # Data quality
└── error.log           # Error tracking
```

## 🚨 Emergency Procedures

### Database Recovery
```bash
# Create backup
cp trading_analytics.db trading_analytics_backup.db

# Check integrity
poetry run python -c "
import sqlite3
conn = sqlite3.connect('trading_analytics.db')
result = conn.execute('PRAGMA integrity_check').fetchone()
if result[0] == 'ok':
    print('Database integrity OK')
else:
    print('Database corruption detected:', result[0])
conn.close()
"

# Restore from backup if needed
# cp trading_analytics_backup.db trading_analytics.db
```

### Performance Emergency
```bash
# Stop all operations
pkill -f "python.*db_pipe"

# Check system resources
top -p $(pgrep -f "python.*db_pipe")

# Restart with reduced batch size
export BATCH_SIZE=100
poetry run python db_pipe.py --status
```

## 📞 Support Information

### System Information
- **Database Version**: SQLite 3.x
- **Python Version**: 3.11+
- **Total Records**: 2,108,635
- **Database Size**: 663 MB (optimized)
- **Indexes**: 35 optimized indexes
- **Performance**: 5.4x faster execution (4.6 minutes vs 25 minutes)

### Contact Information
- **Logs**: Check `logs/` directory
- **Documentation**: See `README.md` and `docs/`
- **Tests**: Run `poetry run pytest tests/ -v`

### Performance Benchmarks
- **Query Response**: < 1ms for simple queries
- **Data Loading**: 7,558 records/second (optimized)
- **Memory Usage**: 914MB peak (optimized with garbage collection)
- **Concurrent Access**: WAL mode enabled
- **Pipeline Duration**: 4.6 minutes (5.4x faster than before optimization)

---

**Last Updated**: 2025-07-18  
**Version**: 1.2.0 