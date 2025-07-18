# Trading Analytics Pipeline - Complete Operations Guide

## 📋 Table of Contents
1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Installation & Setup](#installation--setup)
4. [Basic Commands](#basic-commands)
5. [Pipeline Stages](#pipeline-stages)
6. [Data Management](#data-management)
7. [Advanced Features](#advanced-features)
8. [Monitoring & Logging](#monitoring--logging)
9. [Troubleshooting](#troubleshooting)
10. [Best Practices](#best-practices)
11. [Examples & Workflows](#examples--workflows)

---

## Overview

The Trading Analytics Pipeline is a comprehensive data processing system that transforms raw financial data into structured datasets for analysis, risk management, and reporting. The system processes multiple data sources including bond universe data, portfolio holdings, trading runs, and historical G-spread analytics.

### 🎯 **Key Features**
- **Unified Orchestration**: Single entry point for all data processing
- **Dependency Management**: Automatic validation of data requirements
- **Parallel Processing**: Simultaneous execution where possible
- **Incremental Updates**: Only processes new/modified data
- **Comprehensive Logging**: Detailed execution tracking and monitoring
- **Interactive Mode**: User-friendly menu system
- **Flexible Execution**: Dry-run, validation-only, and force modes
- **Enhanced Console Output**: Comprehensive data engineering insights
- **Orphaned CUSIP Analysis**: Identify CUSIPs in other tables but not in universe
- **Standardized Naming**: Database columns and tables match parquet file names exactly
- **5.4x Performance Optimization**: Pipeline execution 540% faster with optimized batch processing
- **Parallel CUSIP Standardization**: Multi-threaded processing for large datasets

### 🏗️ **Architecture**
```
run_pipe.py (Master Orchestrator)
├── Universe Processing    # Bond universe master dataset
├── Portfolio Processing   # Portfolio holdings and performance
├── Trading Runs          # Trade execution data
├── Historical G-Spread   # Government spread analytics
└── Run Monitoring        # Trading performance analytics
```

---

## Quick Start

### ⚡ **30-Second Start**
```bash
# Install dependencies
poetry install

# Run complete pipeline
poetry run python run_pipe.py --full

# Interactive menu (recommended for beginners)
poetry run python run_pipe.py --menu
```

### 🎯 **Common Commands**
```bash
# Individual pipeline stages
poetry run python run_pipe.py --universe
poetry run python run_pipe.py --portfolio
poetry run python run_pipe.py --runs

# Planning and validation
poetry run python run_pipe.py --dry-run
poetry run python run_pipe.py --validate-only

# Force execution despite missing data
poetry run python run_pipe.py --full --force
```

---

## Installation & Setup

### 📋 **Prerequisites**
- Python 3.11+
- Poetry (package manager)
- Git
- 8GB+ RAM (for large dataset processing)
- 10GB+ available disk space

### 🔧 **Installation Steps**

1. **Clone Repository**
   ```bash
   git clone <repository-url>
   cd work_supa
   ```

2. **Install Dependencies**
   ```bash
   poetry install
   ```

3. **Verify Installation**
   ```bash
   poetry run python run_pipe.py --validate-only
   ```

4. **Setup Data Sources** (see [Data Management](#data-management))

### 🌍 **Environment Setup**
```bash
# Create .env file for sensitive configuration (optional)
PIPELINE_LOG_LEVEL=INFO
PIPELINE_PARALLEL=true
```

---

## Basic Commands

### 🎮 **Master Command Structure**
```bash
poetry run python run_pipe.py [PIPELINE_FLAGS] [EXECUTION_FLAGS] [CONFIG_FLAGS]
```

### 📊 **Pipeline Selection Flags**

| Flag | Description | Estimated Time |
|------|-------------|----------------|
| `--full` | Run complete pipeline (all stages) | ~4.6 minutes (optimized) |
| `--universe` | Process bond universe data | ~2 minutes |
| `--portfolio` | Process portfolio holdings | ~3 minutes |
| `--historical-gspread` | G-spread analytics | ~5 minutes |
| `--runs` | Process trading runs Excel files | ~4 minutes |
| `--runs-monitor` | Run trading performance analysis | ~2 minutes |

### ⚙️ **Execution Control Flags**

| Flag | Description | Use Case |
|------|-------------|----------|
| `--dry-run` | Show execution plan without running | Planning and verification |
| `--validate-only` | Check dependencies and config only | Quick health check |
| `--force` | Proceed despite missing dependencies | Emergency processing |
| `--parallel` | Enable parallel execution | Faster processing |
| `--skip-validation` | Skip data dependency checks | Advanced users only |

### 📁 **Configuration Flags**

| Flag | Description | Default |
|------|-------------|---------|
| `--config FILE` | Custom configuration file | `config/config.yaml` |
| `--log-level LEVEL` | Set logging verbosity | `INFO` |
| `--log-file FILE` | Override log file location | `logs/pipeline_master.log` |

### 🎛️ **Interactive & Monitoring**

| Flag | Description |
|------|-------------|
| `--menu` | Launch interactive menu |
| `--monitor` | Enable real-time monitoring |
| `--report` | Generate execution report |

---

## Pipeline Stages

### 🌍 **Universe Processing**
**Purpose**: Create master bond universe dataset from API extracts

**Input**: `universe/raw data/API MM.DD.YY.xlsx`  
**Output**: `universe/universe.parquet`, `universe/processed data/universe_processed.csv`

**Command**: `poetry run python run_pipe.py --universe`

**What it does**:
- Processes bond universe Excel files from API extracts
- Creates comprehensive bond characteristics database
- Validates CUSIP formats and data quality
- Generates both Parquet (fast) and CSV (readable) outputs
- Serves as master reference for all other pipelines

### 💼 **Portfolio Processing**
**Purpose**: Process portfolio holdings and performance data

**Input**: `portfolio/raw data/Aggies MM.DD.YY.xlsx`  
**Output**: `portfolio/portfolio.parquet`, `portfolio/processed data/portfolio.csv`

**Command**: `poetry run python run_pipe.py --portfolio`

**What it does**:
- Converts portfolio Excel files to structured datasets
- Extracts date information from filenames
- Combines multiple portfolio snapshots with proper dating
- Validates holdings against universe master data
- Supports incremental processing (only new files)

### 📈 **Trading Runs Processing**
**Purpose**: Process trade execution data for analysis

**Input**: `runs/raw/RUNS MM.DD.YY.xlsx`  
**Output**: `runs/combined_runs.parquet`, `runs/processed runs data/`

**Command**: `poetry run python run_pipe.py --runs`

**What it does**:
- Processes multiple trading run Excel files
- Combines trade data with proper date indexing
- Validates trade data quality and consistency
- Creates master trading dataset for analysis
- Tracks processing state to avoid duplicates

### 📊 **Historical G-Spread Analytics**
**Purpose**: Analyze government spread relationships and trends

**Input**: `historical g spread/raw data/g_ts.parquet`  
**Output**: `historical g spread/bond_z.parquet`, `historical g spread/processed data/bond_z.csv`

**Command**: `poetry run python run_pipe.py --historical-gspread`

**What it does**:
- Performs pairwise Z-score analysis of bond spreads
- Calculates statistical measures and percentiles
- Identifies extreme spread relationships
- Generates analytics for government curve analysis
- Self-contained (no external dependencies)

### 🔍 **Run Monitoring**
**Purpose**: Generate trading performance analytics and monitoring

**Input**: `runs/combined_runs.parquet` (from runs processing)  
**Output**: `runs/run_monitor.parquet`, `runs/processed runs data/run_monitor.csv`

**Command**: `poetry run python run_pipe.py --runs-monitor`

**Dependencies**: Requires runs processing to be completed first

**What it does**:
- Analyzes trading performance and attribution
- Generates risk metrics and exposures
- Creates monitoring dashboards and reports
- Validates trades against universe data

---

## Data Management

### 📁 **Data Directory Structure**
```
work_supa/
├── universe/
│   ├── raw data/                    # API Excel extracts
│   ├── processed data/              # Generated CSV outputs
│   ├── universe.parquet            # Master dataset
│   └── data_instructions_universe.md
├── portfolio/
│   ├── raw data/                    # Aggies Excel files
│   ├── processed data/              # Generated outputs
│   ├── portfolio.parquet           # Master dataset
│   └── data_instructions_portfolio.md
├── runs/
│   ├── raw/                         # Trading run Excel files
│   ├── processed runs data/         # Generated outputs
│   ├── combined_runs.parquet       # Master dataset
│   └── data_instructions_runs.md
├── historical g spread/
│   ├── raw data/                    # Time series data
│   ├── processed data/              # Analytics outputs
│   └── data_instructions_historical_g_spread.md
└── logs/                           # Execution logs
```

### 🔄 **Data Sync Workflow**

1. **Source Data** (Stored in Dropbox)
2. **Local Transfer** (Copy to appropriate raw data folders)
3. **Pipeline Processing** (Run specific stages)
4. **Validation** (Check outputs for quality)
5. **Cleanup** (Remove large raw files if needed)

### 📋 **Data Requirements by Stage**

| Stage | Required Files | Location | Size |
|-------|---------------|----------|------|
| Universe | `API MM.DD.YY.xlsx` | `universe/raw data/` | ~1-2MB each |
| Portfolio | `Aggies MM.DD.YY.xlsx` | `portfolio/raw data/` | ~400KB each |
| Trading Runs | `RUNS MM.DD.YY.xlsx` | `runs/raw/` | ~1-3MB each |
| G-Spread | `g_ts.parquet` | `historical g spread/raw data/` | ~7.5MB |

### 🚨 **Missing Data Handling**

The pipeline includes intelligent dependency validation:

```bash
# Example validation output
[VALIDATION] Missing data dependencies detected:

UNIVERSE:
  Description: Universe API Excel files
  ❌ Directory missing: universe/raw data
  📖 See: universe/data_instructions_universe.md for setup instructions

💡 Use --force flag to proceed anyway, or add missing data files
```

---

## Advanced Features

### 🚀 **Parallel Processing**
```bash
# Enable parallel execution where possible
poetry run python run_pipe.py --full --parallel

# Parallel processing automatically used for:
# - Multiple file processing within stages
# - Independent pipeline stages (universe, portfolio, etc.)
```

### ⏰ **Resume from Specific Stage**
```bash
# Resume pipeline from G-spread analytics
poetry run python run_pipe.py --resume-from=gspread-analytics

# Available resume points:
# - universe
# - portfolio  
# - historical-gspread
# - runs-excel
# - runs-monitor
```

### 🎯 **Custom Configuration**
```bash
# Use custom config file
poetry run python run_pipe.py --config my_config.yaml --full

# Override log level
poetry run python run_pipe.py --log-level DEBUG --universe
```

### 🔍 **Validation and Planning**
```bash
# Check all dependencies without execution
poetry run python run_pipe.py --validate-only

# See detailed execution plan
poetry run python run_pipe.py --full --dry-run

# Skip validation for advanced users
poetry run python run_pipe.py --skip-validation --portfolio
```

### 📊 **Monitoring and Reporting**
```bash
# Enable real-time monitoring
poetry run python run_pipe.py --full --monitor

# Generate comprehensive execution report
poetry run python run_pipe.py --full --report

# Send notifications (future feature)
poetry run python run_pipe.py --full --notify=email
```

---

## Monitoring & Logging

### 📝 **Log Files**
- **Master Log**: `logs/pipeline_master.log` (comprehensive execution log)
- **Stage Logs**: Individual processors create detailed logs
- **Debug Logs**: Available with `--log-level DEBUG`

### 📊 **Execution Metrics**
The pipeline tracks and reports:
- Processing duration per stage
- Records processed per stage
- Output files generated
- Memory usage
- Error counts and types

### 🔍 **Real-time Monitoring**
```bash
# Monitor execution in real-time
poetry run python run_pipe.py --full --monitor

# Example monitoring output:
[STAGE START] universe
  Stage Info: {"script": "universe/universe_raw_to_parquet.py"}
[MEMORY] Stage Start: universe
  RSS: 78.1 MB, VMS: 669.0 MB, Percent: 0.2%
[STAGE END] universe
  Success: True, Duration: 3.60 seconds
  Records Processed: 2,025, Output Files: 1
```

### 📈 **Performance Metrics**
```bash
# Example performance summary:
PERFORMANCE METRICS:
  universe: {'duration_seconds': 3.59, 'records_per_second': 563.27, 'success': True}

DATA METRICS:
  universe: {'records_processed': 2025, 'output_files': 1}
```

---

## Troubleshooting

### 🚨 **Common Issues and Solutions**

#### **Missing Data Files**
**Problem**: Pipeline stops with dependency validation errors
```
❌ No files found matching: universe/raw data/API *.xlsx
```
**Solution**: 
1. Check data instruction files for setup guidance
2. Copy required files from Dropbox to appropriate directories
3. Use `--force` flag to proceed with existing data

#### **Memory Issues**
**Problem**: Pipeline fails with out-of-memory errors
**Solution**:
```bash
# Process stages individually
poetry run python run_pipe.py --universe
poetry run python run_pipe.py --portfolio

# Disable parallel processing
poetry run python run_pipe.py --full  # (parallel disabled by default)
```

#### **Poetry/Environment Issues**
**Problem**: `poetry: command not found` or package conflicts
**Solution**:
```bash
# Reinstall Poetry environment
poetry env remove python
poetry install

# Check environment
poetry env info
poetry run python --version
```

#### **File Processing Errors**
**Problem**: Excel files fail to process
**Solution**:
1. Verify file naming conventions (see data instruction files)
2. Check file corruption
3. Ensure files are not open in Excel
4. Review logs for specific error details

#### **Configuration Issues**
**Problem**: Pipeline can't find configuration files
**Solution**:
```bash
# Verify config file exists
ls config/config.yaml

# Use absolute path if needed
poetry run python run_pipe.py --config /full/path/to/config.yaml
```

### 🔧 **Debug Mode**
```bash
# Enable detailed debugging
poetry run python run_pipe.py --log-level DEBUG --universe

# This provides:
# - Detailed file processing logs
# - Memory usage tracking
# - Step-by-step execution details
# - SQL queries (if applicable)
# - Network requests (if applicable)
```

### 📋 **Health Check Commands**
```bash
# Quick health check
poetry run python run_pipe.py --validate-only

# Environment verification
poetry env info
poetry run python -c "import pandas; print(pandas.__version__)"

# Configuration validation
poetry run python run_pipe.py --config config/config.yaml --validate-only
```

---

## Best Practices

### 🎯 **Development Workflow**

1. **Before Making Changes**
   ```bash
   # Always validate first
   poetry run python run_pipe.py --validate-only
   
   # Test with dry run
   poetry run python run_pipe.py --dry-run --universe
   ```

2. **Testing New Data**
   ```bash
   # Process individual stages first
   poetry run python run_pipe.py --universe
   
   # Then run dependent stages
   poetry run python run_pipe.py --runs-monitor
   ```

3. **Regular Maintenance**
   ```bash
   # Weekly: Update universe data
   poetry run python run_pipe.py --universe
   
   # As needed: Process new portfolio/trading data
   poetry run python run_pipe.py --portfolio --runs
   ```

### 📊 **Data Management Best Practices**

1. **Organized File Naming**
   - Universe: `API MM.DD.YY.xlsx`
   - Portfolio: `Aggies MM.DD.YY.xlsx`
   - Trading Runs: `RUNS MM.DD.YY.xlsx`

2. **Regular Cleanup**
   ```bash
   # Remove old raw files after processing
   # Keep only recent data for active analysis
   ```

3. **Backup Strategy**
   - Raw data: Stored in Dropbox (shared access)
   - Processed data: Generated automatically (can be regenerated)
   - Code: Version controlled in Git

### ⚡ **Performance Optimization**

1. **Use Parallel Processing**
   ```bash
   poetry run python run_pipe.py --full --parallel
   ```

2. **Process Only What's Needed**
   ```bash
   # Instead of always running --full
   poetry run python run_pipe.py --universe --portfolio
   ```

3. **Monitor Resource Usage**
   ```bash
   # Use monitoring to track performance
   poetry run python run_pipe.py --full --monitor
   ```

### 🔒 **Security Best Practices**

1. **Environment Variables**: Store sensitive config in `.env` files
2. **Access Control**: Limit access to Dropbox data sources  
3. **Log Management**: Regularly rotate and archive log files
4. **Git Hygiene**: Never commit large data files or secrets

---

## Examples & Workflows

### 🚀 **Daily Operations Workflow**
```bash
# 1. Morning: Update universe data
poetry run python run_pipe.py --universe

# 2. Process yesterday's portfolio
poetry run python run_pipe.py --portfolio

# 3. Process any new trading runs
poetry run python run_pipe.py --runs

# 4. Generate monitoring reports
poetry run python run_pipe.py --runs-monitor
```

### 📊 **Weekly Analysis Workflow**
```bash
# 1. Complete data refresh
poetry run python run_pipe.py --full

# 2. Generate comprehensive reports
poetry run python run_pipe.py --full --report

# 3. Historical analysis (if needed)
poetry run python run_pipe.py --historical-gspread
```

### 🔧 **New Machine Setup Workflow**
```bash
# 1. Clone and install
git clone <repository-url>
cd work_supa
poetry install

# 2. Validate setup
poetry run python run_pipe.py --validate-only

# 3. Test with dry run
poetry run python run_pipe.py --dry-run

# 4. Process with existing data
poetry run python run_pipe.py --universe --force
```

### 🚨 **Emergency Data Processing**
```bash
# 1. Skip validation if data sources unavailable
poetry run python run_pipe.py --portfolio --skip-validation

# 2. Force execution despite missing dependencies
poetry run python run_pipe.py --full --force

# 3. Process specific urgent data only
poetry run python run_pipe.py --runs --parallel
```

### 🔍 **Data Investigation Workflow**
```bash
# 1. Check what data we have
poetry run python run_pipe.py --validate-only

# 2. See execution plan
poetry run python run_pipe.py --full --dry-run

# 3. Process with detailed logging
poetry run python run_pipe.py --universe --log-level DEBUG

# 4. Validate outputs
ls -la universe/processed\ data/
head universe/processed\ data/universe_processed.csv
```

### 📈 **Performance Testing Workflow**
```bash
# 1. Baseline measurement
time poetry run python run_pipe.py --universe

# 2. Parallel processing test
time poetry run python run_pipe.py --universe --parallel

# 3. Memory monitoring
poetry run python run_pipe.py --universe --monitor

# 4. Full pipeline benchmark
time poetry run python run_pipe.py --full --parallel --monitor
```

---

## 🆕 Recent Enhancements (v2.1)

### 📊 **Enhanced Console Output**
The pipeline now provides comprehensive data engineering insights:

**Database Pipeline Features**:
- **Data Quality Metrics**: Real-time validation and quality indicators
- **Processing Performance**: Detailed statistics and timing information
- **CUSIP Standardization**: Success rates and validation results
- **Orphaned CUSIP Analysis**: Identify CUSIPs in other tables but not in universe
- **Last Universe Date Coverage**: Analysis of data coverage on the most recent universe date
- **Memory Usage Tracking**: Real-time memory consumption monitoring

**Status Command Enhancements**:
- **System Health Metrics**: Database size, record counts, and performance indicators
- **Table Coverage Analysis**: Detailed breakdown of data in each table
- **Performance View Summaries**: Quick access to key analytics
- **Data Quality Indicators**: Validation results and potential issues

### 🔄 **Standardized Naming**
- **Column Name Alignment**: All database columns now exactly match parquet file column names
- **Table Name Consistency**: Database table names match their source parquet file names
- **Perfect Data Flow**: Eliminates column mapping issues and data loss
- **Maintainability**: Easier to understand and debug data flow

### 🗂️ **Database File Organization**
- **Clean Root Directory**: Only main database file in project root
- **Organized Backups**: All backup files stored in dedicated `db/` directory
- **Professional Structure**: Follows data engineering best practices
- **Easy Maintenance**: Clear separation between active and backup files

### 📈 **Performance Improvements**
- **Loading Speed**: Increased from 1,400 to 7,558 records/second (5.4x improvement)
- **Database Size**: Optimized to 663 MB with 2.1M+ records
- **Total Records**: Successfully processing 2,108,635 records
- **Query Performance**: Maintained sub-millisecond response times
- **Pipeline Duration**: Reduced from 25 minutes to 4.6 minutes (5.4x faster)
- **Batch Processing**: Optimized from 1,000 to 10,000 records per batch
- **Parallel Processing**: Multi-threaded CUSIP standardization for large datasets

## Support & Resources

### 📚 **Documentation Files**
- `universe/data_instructions_universe.md` - Universe data setup
- `portfolio/data_instructions_portfolio.md` - Portfolio data setup  
- `runs/data_instructions_runs.md` - Trading runs data setup
- `historical g spread/data_instructions_historical_g_spread.md` - G-spread setup

### 🔗 **Quick Reference**
```bash
# Most common commands
poetry run python run_pipe.py --menu          # Interactive mode
poetry run python run_pipe.py --full          # Complete pipeline
poetry run python run_pipe.py --dry-run       # Planning mode
poetry run python run_pipe.py --validate-only # Health check
poetry run python run_pipe.py --universe      # Universe only
poetry run python run_pipe.py --force         # Force execution

# Database operations (enhanced console output)
poetry run python db_pipe.py                  # Full database pipeline
poetry run python db_pipe.py --status         # System status with orphaned CUSIP analysis
poetry run python db_pipe.py --status --verbose # Detailed data engineering insights
```

### ⚡ **Emergency Commands**
```bash
# Pipeline stuck? Check these:
ps aux | grep python              # Check running processes
tail -f logs/pipeline_master.log  # Monitor real-time logs
du -sh universe/ portfolio/ runs/ # Check disk usage
poetry env info                   # Verify environment
```

---

*Last Updated: 2025-07-18*  
*Pipeline Version: v2.2 - 5.4x Performance Optimization & Parallel Processing*

**For additional support, see the individual data instruction files or contact the development team.** 