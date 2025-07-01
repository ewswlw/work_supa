# Work Supa - Financial Data Processing Pipeline

A comprehensive, modular data pipeline for processing financial trading data from Excel files into structured formats with extensive validation and observability.

## Project Overview

This project handles three main financial data streams:
- **Universe Data**: Bond/security master data from Bloomberg API exports
- **Portfolio Data**: Position and holdings data from trading systems  
- **Runs Data**: Trading execution data (legacy pipeline)

## Key Features

- **✅ Modular Architecture**: Separate processors for each data type with shared utilities
- **✅ Incremental Processing**: Only processes new or modified files using state management
- **✅ Comprehensive Logging**: Detailed observability with DataFrame info logging
- **✅ Data Validation**: Extensive quality checks with automated error reporting
- **✅ Configuration-Driven**: YAML-based configuration for all processing rules
- **✅ Multiple Output Formats**: Both Parquet (structured) and CSV (analysis) outputs
- **✅ Performance Optimized**: 55:1 compression ratio, 23x faster reads with Parquet
- **✅ Production Ready**: Fail-fast error handling with detailed diagnostics

## Project Structure

```
work_supa/
├── src/
│   ├── pipeline/
│   │   ├── universe_processor.py      # Core universe data logic
│   │   ├── portfolio_processor.py     # Core portfolio data logic
│   │   ├── excel_processor.py         # Excel file processing
│   │   ├── parquet_processor.py       # Parquet operations
│   │   └── supabase_processor.py      # Supabase integration
│   ├── utils/
│   │   ├── config.py                  # Configuration management
│   │   ├── logging.py                 # LogManager for comprehensive logging
│   │   ├── validators.py              # Data validation framework
│   │   └── reporting.py               # Data quality reporting
│   └── models/
│       └── data_models.py             # Data structures and schemas
├── config/
│   └── config.yaml                    # Centralized configuration
├── logs/
│   ├── universe_processor.log         # Universe pipeline logs
│   ├── portfolio_processor.log        # Portfolio pipeline logs
│   └── pipeline.log                   # General pipeline logs
├── universe/
│   ├── raw data/                      # Bloomberg API exports (API MM.DD.YY.xlsx)
│   ├── processed data/                # CSV outputs
│   ├── universe.parquet               # Structured output
│   ├── processing_state.json          # Incremental processing state
│   └── universe_raw_to_parquet.py     # Universe pipeline runner
├── portfolio/
│   ├── raw data/                      # Trading system exports (Aggies MM.DD.YY.xlsx)
│   ├── processed data/                # CSV outputs
│   ├── portfolio.parquet              # Structured output
│   ├── processing_state.json          # Incremental processing state
│   └── portfolio_excel_to_parquet.py  # Portfolio pipeline runner
├── runs/
│   ├── older files/                   # Historical run data (RUNS MM.DD.YY.xlsx)
│   └── combined_runs.parquet          # Legacy runs output
├── scripts/                           # Utility and debug scripts
├── change_logs/                       # Comprehensive project documentation
└── ai_instructions/                   # AI context and instructions
```

## Setup

### Prerequisites
```bash
# Install Poetry (dependency manager)
curl -sSL https://install.python-poetry.org | python3 -

# Clone repository
git clone <repository-url>
cd work_supa
```

### Installation
```bash
# Install dependencies
poetry install

# Verify installation
poetry run python --version
```

### Environment Configuration
Create a `.env` file for Supabase integration (optional):
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
SUPABASE_TABLE=your_table_name
```

## Usage

### Universe Data Processing
Process Bloomberg API exports with comprehensive validation:
```bash
poetry run python universe/universe_raw_to_parquet.py
```

**Input**: `universe/raw data/API MM.DD.YY.xlsx` files  
**Output**: 
- `universe/universe.parquet` (structured data)
- `universe/processed data/universe_processed.csv` (analysis-ready)
- `logs/universe_processor.log` (detailed processing logs)

### Portfolio Data Processing  
Process trading system position data:
```bash
poetry run python portfolio/portfolio_excel_to_parquet.py
```

**Input**: `portfolio/raw data/Aggies MM.DD.YY.xlsx` files  
**Output**:
- `portfolio/portfolio.parquet` (structured data)  
- `portfolio/processed data/portfolio.csv` (analysis-ready)
- `logs/portfolio_processor.log` (detailed processing logs)

### Legacy Runs Processing
Process historical trading execution data:
```bash
poetry run python scripts/run_pipeline.py
```

## Configuration

All processing rules are defined in `config/config.yaml`:

### Universe Processor Configuration
```yaml
universe_processor:
  # Column selection and bucketing rules
  columns_to_keep: [Date, CUSIP, Security, ...]
  
  # Risk-based bucketing for analysis
  bucketing:
    yrs_to_maturity:
      column_name: "Yrs (Mat)"
      new_column_name: "Yrs (Mat) Bucket"
      bins: [0, 0.25, 0.50, 1, 2.1, 3.1, 4.1, 5.1, 7.1, 10.1, 25.1, .inf]
      labels: ['0-0.25', '0.25-0.50', '0.50-1', ...]
  
  # Data validation rules
  validation:
    numeric_columns: [Stochastic Duration, MTD Return, ...]
```

### Portfolio Processor Configuration  
```yaml
portfolio_processor:
  # Columns to remove from analysis
  columns_to_drop: [BBG YIELD SPREAD, PROFIT, REALIZED, ...]
  
  # Special security CUSIP mappings
  cusip_mappings:
    CDX:
      security_type: "CDX"
      security_name: "CDX"
      cusip: "460"
    CASH_CAD:
      security_name: "CASH CAD"
      cusip: "123"
  
  # Validation rules
  validation:
    required_columns: [Date, SECURITY, CUSIP]
    numeric_columns: [PRICE, QUANTITY, VALUE, ...]
```

## Data Processing Features

### Incremental Processing
- **File Change Detection**: Tracks modification time and file size
- **State Persistence**: Maintains `processing_state.json` for each pipeline
- **Smart Reprocessing**: Only processes new/modified files
- **Data Deduplication**: Handles overlapping data intelligently

### Data Validation & Quality
```python
# Example validation output:
VALIDATION_ISSUES = {
    'Stochastic Duration': '25,006 non-numeric entries',
    'MTD Return': '2,505 non-numeric entries', 
    'G Sprd': '3,116 non-numeric entries'
}

# Quality metrics automatically generated:
# - Null value analysis
# - Data type validation  
# - Statistical summaries
# - Categorical distributions
```

### Comprehensive Logging
```python
# DataFrame structure logging:
[2025-06-27 12:50:48,117] INFO: DataFrame info after loading from Parquet:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 29967 entries, 0 to 29966
Data columns (total 47 columns):
 #   Column                  Non-Null Count  Dtype         
---  ------                  --------------  -----         
 0   Date                    29967 non-null  datetime64[ns]
 1   CUSIP                   29967 non-null  object        
...

# Processing statistics:
[2025-06-27 12:50:48,121] INFO: No new or modified files to process
[2025-06-27 12:50:48,171] INFO: Analysis: Found 0 rows with blank CUSIP out of 1937 total rows
```

## Performance Metrics

### Current Processing Statistics
```
Universe Pipeline:
- Records: 29,967 rows × 47 columns
- Processing Time: ~2 seconds
- File Size: 5.8MB (Parquet) vs ~100MB (CSV equivalent)
- Compression: 55:1 ratio

Portfolio Pipeline:  
- Records: 1,937 rows × 51 columns
- Processing Time: ~1 second
- File Size: 274KB (Parquet) vs ~15MB (CSV)
- Read Performance: 23x faster than CSV
```

### Data Quality Metrics
- **✅ Universe**: Comprehensive validation across 47 financial metrics
- **✅ Portfolio**: CUSIP mapping and data consistency checks  
- **✅ Automated**: Quality reports generated with every run
- **✅ Monitoring**: Detailed error logging with impact analysis

## Monitoring & Observability

### Log Files
Each pipeline maintains detailed logs:
```bash
# Universe processing logs
tail -f logs/universe_processor.log

# Portfolio processing logs  
tail -f logs/portfolio_processor.log

# View last 50 lines (Windows PowerShell)
Get-Content "logs/universe_processor.log" -Tail 50
```

### Processing State
Check incremental processing status:
```bash
# View universe processing state
cat universe/processing_state.json

# View portfolio processing state
cat portfolio/processing_state.json
```

## Architecture Patterns

### Runner Pattern
Simple, focused runner scripts that delegate to core processors:
```python
# universe/universe_raw_to_parquet.py
if __name__ == "__main__":
    logger = LogManager(log_file='logs/universe_processor.log')
    
    try:
        final_df = process_universe_files(logger)
        logger.info("Pipeline finished successfully")
        # Export to CSV automatically
        df.to_csv('universe/processed data/universe_processed.csv')
    except Exception as e:
        logger.error("Pipeline failed", exc=e)
```

### Configuration-Driven Processing
Business rules externalized to YAML configuration:
```python
# Load configuration
config = load_config()

# Apply business rules
drop_cols = [col for col in config['columns_to_drop'] if col in df.columns]
df.drop(columns=drop_cols, inplace=True)

# Apply CUSIP mappings
for mapping_name, mapping_config in config['cusip_mappings'].items():
    # Apply mapping logic based on configuration
```

## Troubleshooting

### Common Issues

**Files Not Processing**:
```bash
# Check processing state
cat universe/processing_state.json
# Delete state file to force reprocessing
rm universe/processing_state.json
```

**Data Quality Issues**:
```bash
# Review validation reports in logs
grep "VALIDATION ERRORS" logs/universe_processor.log
# Check source Excel files for data inconsistencies
```

**Performance Issues**:
```bash
# Monitor processing times in logs
grep "Processing Complete" logs/universe_processor.log
# Check DataFrame sizes at each step
grep "DataFrame Shape" logs/universe_processor.log
```

### Debug Mode
Enable detailed debugging:
```yaml
# config/config.yaml
logging:
  level: "DEBUG"  # Show all processing details
```

## Development

### Project Documentation
- **📖 Comprehensive Changelog**: `change_logs/project_changelog.md`
- **🤖 AI Instructions**: `ai_instructions/` directory
- **🔧 Technical Details**: Extensive inline documentation

### Adding New Features
1. Update configuration in `config/config.yaml`
2. Implement core logic in appropriate processor
3. Add validation rules if needed
4. Update tests and documentation
5. Add changelog entry with timestamp

### Code Quality
- **Modular Design**: Separation of concerns across processors
- **Error Handling**: Fail-fast with detailed error reporting
- **Logging**: Comprehensive observability at all levels
- **Configuration**: Externalized business rules
- **Type Safety**: Data models and validation frameworks

## Future Roadmap

### Immediate Priorities
- Unit testing framework with pytest
- Performance monitoring and alerting
- Data quality dashboard

### Medium-term Goals  
- Supabase cloud integration
- Automated scheduling and notifications
- Data lineage tracking

### Long-term Vision
- Real-time streaming capabilities
- Machine learning integration
- Multi-environment deployment

---

## Support

For questions or issues:
1. Check the comprehensive changelog: `change_logs/project_changelog.md`
2. Review relevant log files in `logs/`
3. Examine configuration settings in `config/config.yaml`
4. Consult AI instructions in `ai_instructions/`

---

**Last Updated**: 2025-06-27  
**Version**: Production Ready  
**Maintainer**: Data Engineering Team 