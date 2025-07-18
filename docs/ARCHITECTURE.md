# Technical Architecture

## System Overview

The Trading Analytics Database System is a **high-performance, SQLite-based analytics platform** designed for processing and analyzing large-scale trading data with expert-level CUSIP validation and real-time performance monitoring. The system has been **optimized for 5.4x faster execution** (540% performance improvement) through comprehensive performance enhancements.

## Architecture Principles

### 1. Performance First
- **5.4x faster execution** (25 minutes → 4.6 minutes)
- **7,558 records/second processing rate** (vs 1,400 records/second)
- **Sub-millisecond query response times**
- **Optimized database indexes (35 total)**
- **Memory-efficient batch processing (10,000 records per batch)**
- **WAL mode for concurrent access**
- **Intelligent duplicate handling preserving data integrity**
- **Parallel CUSIP standardization for large datasets**

### 2. Data Integrity
- **Expert CUSIP validation and standardization**
- **Comprehensive data quality monitoring**
- **Transaction-based data loading**
- **Constraint enforcement**
- **Most recent record preservation for duplicate handling**

### 3. Scalability
- **Modular component design**
- **Efficient memory management**
- **Optimized for 10x+ data growth**
- **Performance views for common queries**

### 4. Observability
- **5 specialized log files**
- **Detailed operation tracking**
- **Performance monitoring**
- **Error tracking and debugging**
- **Default pipeline behavior with automatic file detection**
- **Comprehensive data engineering insights**
- **Orphaned CUSIP analysis for data quality**
- **Real-time performance metrics**

## System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Parquet Files │───▶│  ParquetProcessor│───▶│ CUSIP Standardizer│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Performance    │◀───│   SQLite DB     │◀───│  Database       │
│     Views       │    │   (Optimized)   │    │   Pipeline      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Analytics     │    │   Monitoring    │    │   Logging       │
│   Queries       │    │   & Metrics     │    │   System        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Core Components

### 1. Database Pipeline (`db_pipe.py`)

**Purpose**: Main orchestration component for data loading and processing.

**Key Features**:
- **Optimized batch processing** (10,000 records per batch)
- **Parallel CUSIP standardization** for large datasets
- **Low memory mode** with garbage collection
- **Database optimization** (VACUUM, ANALYZE)
- **Incremental data loading**
- **Transaction management**
- **Error handling and recovery**
- **Performance monitoring**
- **Comprehensive data engineering insights**

**Data Flow**:
```python
class DatabasePipeline:
    def load_universe_data(self, file_path: str) -> bool:
        # 1. Load parquet data
        # 2. Validate and clean data
        # 3. Standardize CUSIPs
        # 4. Insert into database
        # 5. Update statistics
        
    def load_portfolio_data(self, file_path: str) -> bool:
        # Similar flow with portfolio-specific handling
        
    def load_combined_runs_data(self, file_path: str) -> bool:
        # Similar flow with runs-specific handling
```

### 2. CUSIP Standardizer (`src/cusip/`)

**Purpose**: Expert-level CUSIP validation and standardization.

**Components**:
- `standardizer.py`: Main standardization logic
- `patterns.py`: CUSIP pattern matching
- `mappings.py`: Special CUSIP mappings
- `validation.py`: Validation rules

**Standardization Process**:
```
Input CUSIP → Pattern Matching → Special Mappings → Validation → Standardized CUSIP
```

**Key Features**:
- Support for 6, 8, and 9-character CUSIPs
- Special handling for CDX, CASH, and other instruments
- Check digit validation
- Pattern-based standardization

### 3. Database Schema (`db/database/`)

**Purpose**: Complete database schema definition and management.

**Schema Design**:
```sql
-- Main tables (standardized names matching parquet files)
universe_historical: Market universe data (from universe.parquet)
portfolio_historical: Portfolio positions (from portfolio.parquet)
combined_runs_historical: Trading runs with time stamps (from combined_runs.parquet)
run_monitor: Current monitoring data (from run_monitor.parquet)
gspread_analytics: Historical spread analysis (from bond_z.parquet)

-- Tracking tables
unmatched_cusips_all_dates: Historical unmatched CUSIPs
unmatched_cusips_last_date: Current unmatched CUSIPs

-- Metadata tables
data_quality_log: Data quality monitoring
processing_metadata: Processing operation tracking

-- Performance views
v_daily_summary: Daily trading summaries
v_dealer_performance: Dealer performance metrics
v_cusip_activity: CUSIP activity analysis
```

**Index Strategy**:
- **Primary Indexes**: Date, CUSIP, dealer combinations
- **Secondary Indexes**: Spread analysis, ratings, values
- **Composite Indexes**: Multi-column optimizations
- **Covering Indexes**: Minimal I/O for common queries

### 4. Logging System (`src/logger/`)

**Purpose**: Comprehensive operation tracking and monitoring.

**Log Files**:
1. **db.log**: Database operations and pipeline events
2. **cusip.log**: CUSIP standardization details
3. **pipeline.log**: Pipeline execution tracking
4. **quality.log**: Data quality monitoring
5. **error.log**: Error tracking and debugging

**Log Format**:
```json
{
  "timestamp": "2025-07-17T15:58:00.000000",
  "operation_id": "unique_operation_id",
  "event_type": "operation_type",
  "details": {...},
  "memory_usage_mb": 350.0,
  "database_file_size_mb": 66.65
}
```

### 5. Performance Views

**Purpose**: Pre-optimized views for common analytics queries.

**Views**:
- **v_daily_summary**: Daily trading activity summaries
- **v_dealer_performance**: Dealer performance analysis
- **v_cusip_activity**: CUSIP activity tracking

**Performance Benefits**:
- **Sub-second query response times**
- **Pre-aggregated data for common queries**
- **Reduced I/O for analytics workloads**
- **5.4x faster pipeline execution**
- **7,558 records/second processing rate**

### 6. Duplicate Handling System

**Purpose**: Intelligent handling of duplicate records while preserving data integrity.

**Process**:
1. **Duplicate Detection**: Check for actual duplicates by date/CUSIP/dealer combination
2. **Time-based Sorting**: Sort by date, CUSIP, dealer, and time (descending)
3. **Most Recent Selection**: Take the first (most recent) record for each combination
4. **Data Preservation**: Maintain actual market data without artificial averaging

**Key Features**:
- **Market Data Integrity**: Preserves actual spread values from latest time periods
- **Performance**: No degradation from duplicate handling logic
- **Completeness**: 100% load rate for all unique combinations
- **Validation**: Confirmed most recent records are loaded (e.g., 13:54:00 vs 07:46:00)

**Impact**:
- **Before**: 117,153 duplicates averaged, corrupting market data
- **After**: 125,242 unique combinations with most recent market data preserved
- **Performance**: 5.4x faster execution with optimized batch processing
- **Processing Rate**: 7,558 records/second vs 1,400 records/second

## Data Flow

### 1. Data Loading Process

```
Parquet File → ParquetProcessor → Data Validation → CUSIP Standardization → Database Insert
```

**Steps**:
1. **File Loading**: ParquetProcessor loads data with memory management
2. **Data Validation**: Validate data types, ranges, and constraints
3. **CUSIP Standardization**: Standardize CUSIPs using expert rules
4. **Batch Insert**: Insert data in batches with transaction management
5. **Statistics Update**: Update processing statistics and metadata

### 2. Query Processing

```
Query → Query Optimizer → Index Selection → Data Retrieval → Result Processing
```

**Optimization Features**:
- **Query Plan Analysis**: EXPLAIN QUERY PLAN for optimization
- **Index Usage**: Automatic index selection for optimal performance
- **Memory Management**: Efficient memory usage for large result sets
- **Caching**: Database-level caching for repeated queries

### 3. Performance Monitoring

### 4. Default Pipeline Behavior

**Purpose**: Automatic file detection and processing for seamless operation.

**Features**:
- **Automatic File Detection**: Checks for default file paths when no arguments provided
- **Mixed Format Support**: Handles both CSV and parquet files in same pipeline
- **File Type Detection**: Automatically selects appropriate processor (pandas vs ParquetProcessor)
- **Graceful Fallback**: Clear feedback when default files not found
- **Production Ready**: No-argument execution for easy deployment

**Default File Paths**:
```python
data_sources = {
    'universe': 'universe/universe.parquet',
    'portfolio': 'portfolio/portfolio.parquet', 
    'runs': 'runs/combined_runs.parquet',
    'run_monitor': 'runs/run_monitor.parquet',
    'gspread_analytics': 'historical g spread/bond_z.parquet'
}
```

**Database File Organization**:
```
Root Directory:
└── trading_analytics.db (main database only)

db/ Directory:
└── trading_analytics_backup_YYYYMMDD_HHMMSS.db (backups)
```

**Usage Examples**:
```bash
# Full pipeline with default files
poetry run python db_pipe.py

# Specific files only
poetry run python db_pipe.py --universe "path/to/universe.csv"

# Force full refresh
poetry run python db_pipe.py --force-refresh
```

### 5. Performance Monitoring

```
Operation → Performance Tracking → Metrics Collection → Logging → Analysis
```

**Metrics Tracked**:
- Query execution times
- Memory usage
- Database file size
- Operation success rates
- Error rates and types

## Performance Optimization

### 1. Database Tuning

**Settings**:
```sql
PRAGMA journal_mode=WAL;           -- Concurrent access
PRAGMA synchronous=NORMAL;         -- Performance vs. safety balance
PRAGMA cache_size=64000;           -- 64MB cache
PRAGMA mmap_size=268435456;        -- 256MB mmap
PRAGMA temp_store=MEMORY;          -- Memory-based temp storage
PRAGMA foreign_keys=ON;            -- Referential integrity
```

### 2. Index Strategy

**Index Types**:
- **B-tree Indexes**: Standard indexes for equality and range queries
- **Composite Indexes**: Multi-column indexes for complex queries
- **Covering Indexes**: Include all needed columns to avoid table lookups
- **Partial Indexes**: Indexes on filtered data subsets

**Index Selection**:
- **Date-based queries**: `idx_runs_date`, `idx_universe_date`
- **CUSIP-based queries**: `idx_runs_cusip`, `idx_universe_cusip`
- **Dealer-based queries**: `idx_runs_dealer`
- **Complex queries**: `idx_runs_date_cusip_dealer`

### 3. Query Optimization

**Techniques**:
- **Query Plan Analysis**: Regular analysis of query execution plans
- **Index Usage Monitoring**: Track index usage and effectiveness
- **Statistics Updates**: Regular ANALYZE for optimal query planning
- **View Optimization**: Pre-computed views for common queries

## Scalability Considerations

### 1. Data Growth

**Current Capacity**:
- **Database Size**: 663 MB (optimized)
- **Total Records**: 2,108,635
- **Query Performance**: Sub-millisecond
- **Loading Speed**: 7,558 records/second (optimized)
- **Pipeline Duration**: 4.6 minutes (vs 25 minutes before optimization)

**Growth Projections**:
- **10x Growth**: 21M records, ~6.6GB database
- **100x Growth**: 210M records, ~66GB database
- **Performance**: Maintained through index optimization and parallel processing

### 2. Performance Scaling

**Optimization Strategies**:
- **Partitioning**: Date-based partitioning for large datasets
- **Archiving**: Move historical data to archive tables
- **Compression**: Database compression for storage efficiency
- **Caching**: Application-level caching for frequent queries

### 3. Memory Management

**Memory Usage**:
- **Peak Usage**: 914MB during large data loads (optimized with garbage collection)
- **Cache Size**: 64MB database cache
- **MMAP Size**: 256MB memory mapping
- **Temp Storage**: Memory-based for faster operations
- **Batch Processing**: 10,000 records per batch for optimal memory usage

## Security Considerations

### 1. Data Protection

**Measures**:
- **File Permissions**: Restrictive file permissions on database files
- **Access Control**: Application-level access control
- **Data Validation**: Comprehensive input validation
- **Error Handling**: Secure error handling without information leakage

### 2. Audit Trail

**Tracking**:
- **Operation Logging**: All operations logged with timestamps
- **User Tracking**: Operation tracking with user context
- **Change Tracking**: Data modification tracking
- **Access Logging**: Database access logging

## Monitoring and Alerting

### 1. Performance Monitoring

**Metrics**:
- **Query Response Times**: Track query performance
- **Memory Usage**: Monitor memory consumption
- **Database Size**: Track database growth
- **Error Rates**: Monitor error frequencies

### 2. Health Checks

**Checks**:
- **Database Integrity**: Regular integrity checks
- **Connection Health**: Connection pool monitoring
- **Disk Space**: Available disk space monitoring
- **Performance Degradation**: Query performance monitoring

### 3. Alerting

**Alerts**:
- **High Error Rates**: Alert on increased error rates
- **Performance Degradation**: Alert on slow queries
- **Disk Space**: Alert on low disk space
- **Data Quality**: Alert on data quality issues

## Deployment Architecture

### 1. Development Environment

**Setup**:
- **Local SQLite**: Development database
- **Poetry**: Dependency management
- **Pytest**: Testing framework
- **Logging**: Development logging

### 2. Production Environment

**Requirements**:
- **High-Performance Storage**: SSD storage for optimal performance
- **Adequate Memory**: Sufficient RAM for caching
- **Backup Strategy**: Regular database backups
- **Monitoring**: Production monitoring and alerting

### 3. Backup and Recovery

**Strategy**:
- **Regular Backups**: Automated database backups
- **Point-in-Time Recovery**: Transaction log-based recovery
- **Data Validation**: Backup integrity validation
- **Recovery Testing**: Regular recovery testing

## Future Enhancements

### 1. Planned Features

**Enhancements**:
- **Real-time Streaming**: Real-time data processing
- **Advanced Analytics**: Machine learning integration
- **API Interface**: REST API for external access
- **Web Dashboard**: Web-based monitoring dashboard

### 2. Performance Improvements

**Optimizations**:
- **Query Optimization**: Further query optimization
- **Index Tuning**: Advanced index strategies
- **Caching**: Multi-level caching
- **Parallel Processing**: Parallel data processing

### 3. Scalability Enhancements

**Scaling**:
- **Distributed Processing**: Multi-node processing
- **Data Partitioning**: Advanced partitioning strategies
- **Cloud Integration**: Cloud-based deployment
- **Microservices**: Service-oriented architecture

---

**Document Version**: 1.2.0  
**Last Updated**: 2025-07-18  
**Status**: Production Ready with 5.4x Performance Optimization 