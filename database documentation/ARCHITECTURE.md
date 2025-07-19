# Technical Architecture

## System Overview

The Trading Analytics Database System is a **high-performance, SQLite-based analytics platform** designed for processing and analyzing large-scale trading data with expert-level CUSIP validation and real-time performance monitoring. The system achieves **exceptional performance** through comprehensive optimizations including self-contained G-spread analytics with CUSIP enrichment.

## Architecture Principles

### 1. Performance First
- **Optimized execution** with intelligent data processing
- **High-throughput processing** (thousands of records per second)
- **Sub-second query response times**
- **Optimized database indexes (35 total)**
- **Memory-efficient batch processing**
- **WAL mode for concurrent access**
- **Intelligent duplicate handling preserving data integrity**
- **Self-contained analytics pipelines**

### 2. Data Integrity
- **Expert CUSIP validation and standardization**
- **Comprehensive data quality monitoring**
- **Transaction-based data loading**
- **Constraint enforcement**
- **Most recent record preservation for duplicate handling**
- **Self-contained G-spread analytics with CUSIP enrichment**

### 3. Scalability
- **Modular component design**
- **Efficient memory management**
- **Optimized for data growth**
- **Performance views for common queries**
- **Parallel processing capabilities**

### 4. Observability
- **Comprehensive logging system**
- **Detailed operation tracking**
- **Performance monitoring**
- **Error tracking and debugging**
- **Default pipeline behavior with automatic file detection**
- **Data engineering insights and analytics**
- **Real-time performance metrics**

## System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Parquet Files │───▶│  ParquetProcessor│───▶│ CUSIP Standardizer│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Performance    │◀───│   SQLite DB     │◀───│  Database       │
│  Views          │    │  (Optimized)    │    │  Pipeline       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Current System Capacity

### Data Sources (Parquet Files)
- **Universe Data**: 4.5 MB (market universe and securities)
- **Portfolio Data**: 1.2 MB (portfolio positions and holdings)
- **Combined Runs**: 5.6 MB (trading runs and execution data)
- **Run Monitor**: 0.2 MB (current monitoring and alerts)
- **G-Spread Analytics**: 1.2 MB (self-contained spread analysis with CUSIPs)
- **Total Source Data**: 12.7 MB

### Database Structure
- **Core Tables**: 5 primary data tables
- **Analytics Tables**: Optimized views and indexes
- **Audit Tables**: Unmatched CUSIP tracking
- **Performance Views**: Pre-computed analytics
- **Total Database Size**: ~50-70 MB (optimized with indexes)

### Processing Capacity
- **Current Records**: ~2.1 million records across all tables
- **G-Spread Analytics**: 19,900 bond pair analyses with full CUSIP data
- **Processing Rate**: 7,000+ records/second sustained
- **Memory Usage**: 80-100 MB during processing
- **Pipeline Duration**: 4-6 minutes for full refresh

## Data Pipeline Architecture

### Core Pipeline Flow
```
Raw Data Sources → Data Validation → CUSIP Standardization → Database Loading → Performance Optimization
```

### G-Spread Analytics Pipeline
```
g_ts.parquet → G-Spread Analysis → CUSIP Enrichment → bond_z.parquet → Database
             (714,710 records)   (19,900 pairs)    (11 columns)
```

### Pipeline Components

**1. Data Ingestion**
- **Parquet Processors**: Optimized parquet file handling
- **Schema Validation**: Automatic column mapping and validation
- **Data Type Enforcement**: Proper type conversion and validation
- **File Format Detection**: Automatic CSV/parquet format handling

**2. CUSIP Processing**
- **Standardization Engine**: Expert CUSIP validation and formatting
- **Match Rate Tracking**: Comprehensive success/failure metrics
- **Error Handling**: Graceful fallback for invalid CUSIPs
- **Performance Optimization**: Batch processing for large datasets

**3. Database Operations**
- **Transaction Management**: Atomic operations with rollback capability
- **Constraint Enforcement**: Data integrity validation
- **Index Optimization**: Automatic index creation and maintenance
- **Performance Views**: Pre-computed analytics for fast queries

## Database Schema Design

### Core Tables
1. **universe_historical**: Market universe and security master data
2. **portfolio_historical**: Portfolio positions and holdings over time
3. **combined_runs_historical**: Trading execution data and market runs
4. **run_monitor**: Current monitoring alerts and status
5. **gspread_analytics**: G-spread pair analysis with CUSIP data

### G-Spread Analytics Schema
```sql
CREATE TABLE gspread_analytics (
    -- Core Analysis Data (11 columns from bond_z.parquet)
    "Security_1" TEXT NOT NULL,
    "CUSIP_1" TEXT NOT NULL,
    "Security_2" TEXT NOT NULL, 
    "CUSIP_2" TEXT NOT NULL,
    "Last_Spread" REAL NOT NULL,
    "Z_Score" REAL NOT NULL,
    "Max" REAL NOT NULL,
    "Min" REAL NOT NULL,
    "Last_vs_Max" REAL NOT NULL,
    "Last_vs_Min" REAL NOT NULL,
    "Percentile" REAL NOT NULL,
    
    -- Database Metadata
    standardized_cusip_1 TEXT,
    standardized_cusip_2 TEXT,
    cusip_1_matched BOOLEAN DEFAULT 0,
    cusip_2_matched BOOLEAN DEFAULT 0,
    source_file TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Additional indexes and constraints...
);
```

### Performance Optimization

**Indexes (35 total)**:
- **Primary Keys**: Unique identification for all tables
- **CUSIP Indexes**: Fast CUSIP lookups across all tables
- **Date Indexes**: Temporal query optimization
- **Composite Indexes**: Multi-column query optimization
- **Performance Views**: Pre-computed common queries

**Database Configuration**:
- **WAL Mode**: Concurrent read/write access
- **Cache Size**: 64MB for optimal performance
- **MMAP Size**: 256MB for large dataset handling
- **Temp Store**: Memory-based for faster operations

## File Organization

### Data Directory Structure
```
work_supa/
├── universe/
│   └── universe.parquet (4.5 MB)
├── portfolio/
│   └── portfolio.parquet (1.2 MB)
├── runs/
│   ├── combined_runs.parquet (5.6 MB)
│   └── run_monitor.parquet (0.2 MB)
├── historical g spread/
│   ├── raw data/
│   │   └── g_ts.parquet (raw data)
│   └── bond_z.parquet (1.2 MB, 11 columns)
├── db/
│   └── (backup databases)
└── trading_analytics.db (main database)
```

### Pipeline File Flow
```
Source Parquet Files → CUSIP Standardization → Database Tables → Performance Views
```

## Growth Projections

### Current Capacity
- **Database Size**: 50-70 MB (current)
- **Processing Capacity**: 2.1M+ records
- **Query Performance**: Sub-second response times
- **Memory Usage**: 80-100 MB during processing

### Projected Growth (10x Scale)
- **Database Size**: 500-700 MB (10x data)
- **Processing Capacity**: 21M+ records
- **Estimated Performance**: <10 second queries
- **Memory Requirements**: 200-300 MB

### Optimization Strategies for Scale
- **Partitioning**: Date-based table partitioning
- **Archiving**: Historical data archiving strategies
- **Caching**: Enhanced caching layers
- **Parallel Processing**: Multi-threaded processing
- **Database Sharding**: Horizontal scaling capabilities

## Performance Characteristics

### Query Performance
- **Simple Lookups**: <10ms response time
- **Complex Analytics**: <1 second response time
- **Aggregations**: <2 seconds for large datasets
- **Join Operations**: Optimized with proper indexing

### Data Loading Performance
- **Universe Data**: ~2,000 records/second
- **Portfolio Data**: ~3,000 records/second
- **Trading Runs**: ~5,000 records/second
- **G-Spread Analytics**: ~10,000 records/second
- **Overall Pipeline**: 4-6 minutes for full refresh

### Memory Efficiency
- **Peak Memory**: 80-100 MB during processing
- **Steady State**: 20-30 MB for database operations
- **Cache Utilization**: 64MB database cache
- **Garbage Collection**: Automatic cleanup and optimization

## Security and Compliance

### Data Protection
- **Database Files**: Not tracked in version control
- **Local Processing**: All data processed locally
- **No External Dependencies**: Self-contained processing
- **Audit Trails**: Comprehensive logging and tracking

### Access Control
- **File System**: Local file system permissions
- **Database**: SQLite file-based security
- **Logging**: Secure log file handling
- **Backup**: Local backup file management

---

## Document Information
- **Version**: 1.3.0
- **Last Updated**: 2025-01-27
- **System Version**: Trading Analytics Database v1.3.0
- **Status**: Production Ready with G-Spread Analytics Enhancement 