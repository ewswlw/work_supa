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

# Bond Z-Spread Analysis - Excel-like Interface

A comprehensive React-based web application that provides an Excel-like interface for analyzing bond Z-spread data. This application offers advanced filtering, sorting, formatting, and editing capabilities designed for financial professionals working with bond trading data.

## 🚀 Features

### Core Excel-like Functionality
- **Advanced Table Interface**: Mimics Excel's look and feel with bordered cells, header styling, and row highlighting
- **Multi-column Sorting**: Click headers to sort by any column (ascending/descending)
- **Real-time Filtering**: Individual column filters with various operators (contains, equals, greater than, less than)
- **Global Search**: Search across all data with highlighted results
- **Cell Selection**: Click to select individual cells, visual selection feedback
- **Inline Editing**: Double-click any cell to edit values directly
- **Column Management**: Show/hide columns with visual toggle controls

### Advanced Features
- **Smart Number Formatting**: Automatic formatting for spreads, scores, bids, and offers
- **Color Coding**: Positive numbers in green, negative in red
- **Pagination**: Configurable page sizes (25, 50, 100, 500 rows)
- **Data Export**: Export filtered/sorted data to CSV format
- **Responsive Design**: Works on desktop and tablet devices
- **Performance Optimized**: Handles large datasets (13,500+ rows) efficiently

### Data Analysis Capabilities
- **Bond Pair Analysis**: Compare Security_1 vs Security_2 spreads
- **Z-Score Analysis**: Statistical spread analysis with percentile rankings
- **Trading Data**: Real-time bid/offer spreads, dealer information, and size data
- **Historical Comparison**: Multiple time period analysis with runs data

## 📊 Data Structure

The application works with bond Z-spread data containing 59 columns including:

- **Securities**: Security_1, Security_2, Ticker_1, Ticker_2
- **Spread Analysis**: Last_Spread, Z_Score, Max, Min, Percentile
- **Bond Characteristics**: Rating_1, Rating_2, Currency_1, Currency_2, Custom_Sector_1, Custom_Sector_2
- **Trading Data**: Best Bid/Offer, Dealer information, Size data
- **Historical Data**: Multiple time period runs for comparison

## 🛠️ Technology Stack

- **Frontend**: React 18 with TypeScript
- **Styling**: Tailwind CSS with custom Excel-like styling
- **Icons**: Lucide React for modern iconography
- **Data Processing**: Pandas (Python) for parquet to JSON conversion
- **Build Tools**: Create React App with TypeScript template

## 🚀 Getting Started

### Prerequisites
- Node.js 18+ 
- Python 3.8+ with pandas and pyarrow
- Bond data in parquet format

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd bond-excel-app
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Convert bond data to JSON** (if you have parquet data)
   ```bash
   python3 -c "
   import pandas as pd
   df = pd.read_parquet('path/to/bond_data.parquet')
   df.to_json('public/bond_data.json', orient='records', indent=2)
   "
   ```

4. **Start the development server**
   ```bash
   npm start
   ```

5. **Open your browser**
   Navigate to `http://localhost:3000`

## 💡 Usage Guide

### Basic Operations

1. **Sorting**: Click any column header to sort data
2. **Filtering**: Type in the filter boxes below column headers
3. **Searching**: Use the global search box in the toolbar
4. **Editing**: Double-click any cell to edit its value
5. **Selection**: Click cells to select them (visual feedback provided)

### Advanced Operations

1. **Column Management**: Use the column visibility toggles to show/hide columns
2. **Export Data**: Click "Export CSV" to download filtered data
3. **Pagination**: Use the bottom controls to navigate large datasets
4. **Page Size**: Change how many rows to display per page

### Filtering Options

- **Text Columns**: Use "contains" filtering for partial matches
- **Number Columns**: Use comparison operators (>, <, =)
- **Multiple Filters**: Apply filters to multiple columns simultaneously
- **Clear Filters**: Delete filter text to remove column filters

## 🎨 Customization

### Styling
The application uses custom CSS classes for Excel-like styling:
- `.excel-table`: Main table styling
- `.excel-header`: Column header styling
- `.excel-cell`: Individual cell styling
- `.excel-row`: Row styling with alternating colors

### Color Coding
- **Positive Numbers**: Green text (#008000)
- **Negative Numbers**: Red text (#cc0000)
- **Selected Cells**: Blue background (#0066cc)
- **Header Sorting**: Blue indicators for sort direction

## 📈 Performance

- **Optimized Rendering**: Uses React optimization techniques for large datasets
- **Pagination**: Reduces DOM load by showing only visible rows
- **Memoization**: Expensive calculations are memoized for performance
- **Virtual Scrolling**: Handles thousands of rows efficiently

## 🔧 Configuration

### Environment Variables
Create a `.env` file for custom configuration:
```env
REACT_APP_DEFAULT_PAGE_SIZE=50
REACT_APP_MAX_PAGE_SIZE=1000
REACT_APP_ENABLE_EDITING=true
```

### Data Source
Update `src/App.tsx` to change the data source:
```typescript
const response = await fetch('/your-data-endpoint.json');
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Commit changes: `git commit -am 'Add new feature'`
4. Push to branch: `git push origin feature/new-feature`
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🐛 Known Issues

- Large datasets (>10k rows) may experience slower filtering performance
- Mobile support is limited due to table width requirements
- Some advanced Excel features (formulas, charts) are not yet implemented

## 🔮 Future Enhancements

- [ ] Formula support for calculated columns
- [ ] Chart integration for data visualization
- [ ] Advanced filter operators (between, in list)
- [ ] Multi-column sorting
- [ ] Data validation rules
- [ ] Import from Excel/CSV files
- [ ] Real-time data updates
- [ ] User preferences saving
- [ ] Print formatting options
- [ ] Advanced conditional formatting

## 📞 Support

For questions or support, please:
1. Check the [documentation](docs/)
2. Search existing [issues](issues/)
3. Create a new issue with detailed information

---

Built with ❤️ for bond trading professionals who need Excel-like functionality in a modern web interface. 