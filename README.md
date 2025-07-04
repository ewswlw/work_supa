# Trading Data Pipeline (work_supa)

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Poetry](https://img.shields.io/badge/dependency%20manager-poetry-blue.svg)](https://python-poetry.org/)
[![Data Format](https://img.shields.io/badge/data%20format-parquet-green.svg)](https://parquet.apache.org/)
[![Platform](https://img.shields.io/badge/platform-windows-lightgrey.svg)](https://www.microsoft.com/en-us/windows)

A comprehensive, enterprise-grade data processing pipeline for trading operations, portfolio management, and market analytics. This system processes multiple data sources including universe data, portfolio holdings, historical G-spread analytics, and trading execution monitoring.

## 🎯 Project Overview

The **work_supa** pipeline is designed to handle the complete lifecycle of trading data processing, from raw Excel/CSV inputs to enriched analytical outputs. It provides a unified orchestration system that processes:

- **Universe Data**: Market instrument definitions and metadata
- **Portfolio Data**: Holdings, positions, and portfolio analytics  
- **Historical G-Spread Data**: Bond spread analytics and time series
- **Trading Runs**: Execution monitoring and performance analytics

### Key Features

- ✅ **Unified Orchestration**: Single command execution of entire pipeline
- ✅ **Incremental Processing**: Only processes new/changed data
- ✅ **Robust Error Handling**: Graceful failure recovery and detailed logging
- ✅ **Performance Optimized**: Vectorized operations with 1000x speedup
- ✅ **Enterprise Logging**: Comprehensive audit trails and monitoring
- ✅ **Windows Compatible**: Full Unicode/encoding support for Windows environments
- ✅ **Dependency Management**: Automatic stage ordering and execution
- ✅ **Data Validation**: Schema validation and integrity checks

## 📊 Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw Data      │    │   Processing     │    │   Outputs       │
│   Sources       │───▶│   Pipeline       │───▶│   & Analytics   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
│                      │                      │
├─ Excel Files         ├─ Universe Processor  ├─ Parquet Files
├─ CSV Time Series     ├─ Portfolio Processor ├─ Enhanced Analytics
├─ API Data           ├─ G-Spread Processor  ├─ Performance Metrics
└─ Trading Logs       ├─ Analytics Engine    └─ Monitoring Reports
                      └─ Runs Processor
```

### Data Flow

1. **Ingestion**: Raw Excel/CSV files from multiple sources
2. **Processing**: Data cleaning, validation, and transformation
3. **Analytics**: Advanced statistical analysis and enrichment
4. **Storage**: Efficient Parquet format for fast querying
5. **Monitoring**: Execution tracking and performance metrics

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- **Poetry** (dependency management)
- **Windows 10/11** (optimized for Windows environments)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd work_supa

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Basic Usage

```bash
# Run the complete pipeline
python run_pipe.py

# Run specific stages
python run_pipe.py --stage universe
python run_pipe.py --stage portfolio,historical-gspread

# Force reprocessing (ignore incremental logic)
python run_pipe.py --force

# Interactive mode
python run_pipe.py --interactive
```

## 📁 Project Structure

```
work_supa/
├── 📁 src/                          # Core source code
│   ├── 📁 models/                   # Data models and schemas
│   ├── 📁 orchestrator/             # Pipeline orchestration
│   ├── 📁 pipeline/                 # Processing modules
│   └── 📁 utils/                    # Utilities and helpers
├── 📁 universe/                     # Universe data pipeline
│   ├── 📁 raw data/                 # Input Excel files
│   ├── 📁 processed data/           # CSV outputs
│   └── universe.parquet             # Final parquet output
├── 📁 portfolio/                    # Portfolio data pipeline
│   ├── 📁 raw data/                 # Aggies Excel files
│   ├── 📁 processed data/           # CSV outputs
│   └── portfolio.parquet            # Final parquet output
├── 📁 historical g spread/          # G-spread analytics
│   ├── 📁 raw data/                 # Time series CSV
│   ├── 📁 processed data/           # Enhanced analytics
│   └── *.parquet                    # Multiple parquet outputs
├── 📁 runs/                         # Trading execution data
│   ├── 📁 raw/                      # Excel execution logs
│   ├── 📁 processed runs data/      # CSV outputs
│   └── *.parquet                    # Execution analytics
├── 📁 logs/                         # System and pipeline logs
├── 📁 config/                       # Configuration files
├── 📁 test/                         # Test suite
├── 📁 change logs/                  # Documentation
├── run_pipe.py                      # Main pipeline entry point
└── pyproject.toml                   # Poetry configuration
```

## 🔧 Pipeline Stages

### 1. Universe Processing (`universe`)
**Purpose**: Process market instrument definitions and metadata  
**Input**: Excel files in `universe/raw data/`  
**Output**: `universe.parquet` with standardized instrument data  
**Duration**: ~4 seconds, ~6,000 records  

### 2. Portfolio Processing (`portfolio`)
**Purpose**: Process portfolio holdings and position data  
**Input**: Aggies Excel files in `portfolio/raw data/`  
**Output**: `portfolio.parquet` with consolidated holdings  
**Duration**: ~3.5 seconds, ~10,000 records  

### 3. Historical G-Spread (`historical-gspread`)
**Purpose**: Process bond spread time series data  
**Input**: `bond_g_sprd_time_series.csv` in `historical g spread/raw data/`  
**Output**: `bond_g_sprd_time_series.parquet` with cleaned time series  
**Duration**: ~15 seconds, ~3M records  

### 4. G-Spread Analytics (`gspread-analytics`)
**Purpose**: Advanced statistical analysis and pairwise correlations  
**Dependencies**: universe, portfolio, historical-gspread  
**Output**: Enhanced analytics with Z-scores and correlations  
**Duration**: ~14 seconds, advanced statistical processing  

### 5. Runs Excel Processing (`runs-excel`)
**Purpose**: Process trading execution logs  
**Input**: Excel files in `runs/raw/`  
**Output**: `combined_runs.parquet` with execution data  
**Duration**: ~2.5 seconds, ~230,000 records  

### 6. Runs Monitoring (`runs-monitor`)
**Purpose**: Execution analytics and performance monitoring  
**Dependencies**: universe, portfolio, runs-excel  
**Output**: `run_monitor.parquet` with performance metrics  
**Duration**: ~2.4 seconds, execution analysis  

## ⚙️ Configuration

### Main Configuration (`config/config.yaml`)

```yaml
universe:
  raw_folder: "universe/raw data"
  output_folder: "universe/processed data"
  parquet_file: "universe.parquet"
  
portfolio:
  raw_folder: "portfolio/raw data"
  output_folder: "portfolio/processed data"
  parquet_file: "portfolio.parquet"
  columns_to_drop:
    - "Unnamed: 0"
    - "Unnamed: 1"
    
# ... additional configuration
```

### Pipeline Dependencies

The system automatically manages dependencies:
```
universe ──┐
           ├──▶ gspread-analytics
portfolio ─┘

historical-gspread ──▶ gspread-analytics

runs-excel ──┐
             ├──▶ runs-monitor  
universe ────┤
portfolio ───┘
```

## 📊 Performance Metrics

| Stage | Duration | Records | Performance Notes |
|-------|----------|---------|-------------------|
| Universe | 4.1s | 6,088 | Fast Excel processing |
| Portfolio | 3.5s | 10,085 | Efficient data cleaning |
| Historical G-Spread | 14.9s | 2,929,848 | Large time series |
| G-Spread Analytics | 13.9s | N/A | Vectorized operations |
| Runs Excel | 2.5s | 232,284 | Incremental processing |
| Runs Monitor | 2.4s | N/A | Analytics generation |
| **Total Pipeline** | **~41s** | **3.2M+** | **End-to-end execution** |

## 🔍 Monitoring & Logging

### Log Locations
- **Pipeline Logs**: `logs/pipeline_orchestrator_*.log`
- **Stage-Specific**: `logs/{stage}_processor.log`
- **Error Logs**: Detailed stack traces and error context

### Log Levels
- **INFO**: Normal operation status
- **WARN**: Non-critical issues (e.g., missing optional data)
- **ERROR**: Processing failures with recovery attempts
- **DEBUG**: Detailed execution information

### Monitoring Features
- Real-time progress tracking
- Performance benchmarking
- Data quality validation
- Error recovery and retry logic

## 🛠️ Development

### Running Tests

```bash
# Run all tests
poetry run pytest test/

# Run specific test modules
poetry run pytest test/test_pipeline_manager.py
poetry run pytest test/test_pipeline_config.py

# Run with coverage
poetry run pytest --cov=src test/
```

### Adding New Pipeline Stages

1. **Create Processor**: Add new processor in `src/pipeline/`
2. **Update Configuration**: Add stage config in `config/config.yaml`
3. **Register Stage**: Update `src/orchestrator/pipeline_config.py`
4. **Add Dependencies**: Define stage dependencies
5. **Write Tests**: Add comprehensive test coverage

### Code Standards

- **Functional Programming**: Prefer pure functions and immutability
- **Type Hints**: Full type annotation for all functions
- **Error Handling**: Comprehensive exception handling with user-friendly messages
- **Documentation**: Docstrings for all public functions and classes
- **Testing**: Unit tests for all critical functionality

## 🔧 Troubleshooting

### Common Issues

#### Unicode/Encoding Errors
**Problem**: `UnicodeEncodeError` on Windows console  
**Solution**: All Unicode characters have been replaced with ASCII equivalents

#### Large File Git Issues
**Problem**: Git rejects files >100MB  
**Solution**: Large parquet files are in `.gitignore`

#### Missing Raw Data
**Problem**: Pipeline fails with missing input files  
**Solution**: Ensure raw data files are in correct folders:
- Universe: `universe/raw data/*.xlsx`
- Portfolio: `portfolio/raw data/*.xlsx`
- G-Spread: `historical g spread/raw data/bond_g_sprd_time_series.csv`
- Runs: `runs/raw/*.xlsx`

#### Memory Issues
**Problem**: Out of memory on large datasets  
**Solution**: Pipeline uses chunked processing and efficient data types

### Debug Mode

```bash
# Enable debug logging
python run_pipe.py --debug

# Run single stage with verbose output
python run_pipe.py --stage universe --debug
```

## 📈 Data Quality & Validation

### Input Validation
- Schema validation for all input files
- Data type checking and conversion
- Range validation for numerical fields
- Business rule validation (e.g., positive prices)

### Output Validation
- Record count verification
- Data completeness checks
- Statistical validation (outlier detection)
- Cross-stage consistency validation

### Data Integrity
- Audit trails for all transformations
- Checksum validation for critical data
- Version control for processed datasets
- Backup and recovery procedures

## 🚀 Performance Optimization

### Key Optimizations
- **Vectorized Operations**: NumPy/pandas optimizations (1000x speedup)
- **Efficient Data Types**: Optimized dtypes for memory usage
- **Chunked Processing**: Handle large datasets without memory issues
- **Parallel Processing**: Multi-stage execution where possible
- **Incremental Updates**: Only process new/changed data

### Memory Management
- Automatic garbage collection
- Efficient data structures
- Memory usage monitoring
- Large file streaming

## 📋 Maintenance

### Regular Tasks
- **Daily**: Monitor pipeline execution logs
- **Weekly**: Validate data quality metrics
- **Monthly**: Review performance benchmarks
- **Quarterly**: Update dependencies and security patches

### Backup Strategy
- **Raw Data**: Version controlled input files
- **Processed Data**: Automated parquet backups
- **Configuration**: Git-tracked configuration files
- **Logs**: Archived execution logs

## 🤝 Contributing

1. **Fork** the repository
2. **Create** feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** changes (`git commit -m 'Add amazing feature'`)
4. **Push** to branch (`git push origin feature/amazing-feature`)
5. **Open** Pull Request

### Development Workflow
1. Update dependencies: `poetry update`
2. Run tests: `poetry run pytest`
3. Check code quality: `poetry run flake8`
4. Update documentation as needed

## 📞 Support

### Documentation
- **Detailed Changelog**: `change logs/project_changelog.md`
- **Quick Reference**: `change logs/quick_reference.md`
- **API Documentation**: Generated from docstrings

### Getting Help
- Check the troubleshooting section above
- Review log files in `logs/` directory
- Consult the detailed changelog for historical context
- Run with `--debug` flag for verbose output

## 📜 License

This project is proprietary software for trading operations. All rights reserved.

---

**Last Updated**: January 2025  
**Version**: 2.0.0  
**Status**: Production Ready ✅

> **Note**: This pipeline processes financial data and maintains strict data integrity and audit requirements. All modifications should be thoroughly tested before production deployment. 