# Data Pipeline - Excel to Supabase

A modular, robust data pipeline for processing Excel files and uploading them to Supabase.

## Features

- **Modular Design**: Separate processors for Excel, Parquet, and Supabase operations
- **Incremental Processing**: Only processes new or modified files
- **Parallel Processing**: Multi-threaded Excel file processing
- **Data Validation**: Comprehensive data quality checks
- **Error Handling**: Robust error handling with detailed logging
- **Flexible Configuration**: YAML-based configuration management
- **Multiple Run Modes**: Full pipeline, individual steps, or connection testing

## Project Structure

```
work_supa/
├── src/
│   ├── pipeline/
│   │   ├── excel_processor.py      # Excel file processing
│   │   ├── parquet_processor.py    # Parquet file operations
│   │   └── supabase_processor.py   # Supabase upload operations
│   ├── utils/
│   │   ├── config.py              # Configuration management
│   │   ├── logging.py             # Logging utilities
│   │   └── validators.py          # Data validation
│   └── models/
│       └── data_models.py         # Data models and schemas
├── config/
│   └── config.yaml               # Pipeline configuration
├── scripts/
│   └── run_pipeline.py           # Main pipeline script
└── runs/
    ├── older files/              # Excel files to process
    ├── logs/                     # Log files
    └── combined_runs.parquet     # Output Parquet file
```

## Setup

1. **Environment Variables**: Create a `.env` file in the project root:
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
SUPABASE_TABLE=runs
```

2. **Install Dependencies**:
```bash
poetry install
```

3. **Configuration**: Edit `config/config.yaml` if needed to adjust settings.

## Usage

### Run Complete Pipeline
Process Excel files → Save to Parquet → Upload to Supabase:
```bash
poetry run python scripts/run_pipeline.py
```

### Run Individual Steps

**Excel Processing Only** (Excel → Parquet):
```bash
poetry run python scripts/run_pipeline.py --excel-only
```

**Supabase Upload Only** (Parquet → Supabase):
```bash
poetry run python scripts/run_pipeline.py --supabase-only
```

**Test Connections**:
```bash
poetry run python scripts/run_pipeline.py --test
```

**Custom Config File**:
```bash
poetry run python scripts/run_pipeline.py --config path/to/config.yaml
```

## Configuration

Edit `config/config.yaml` to customize the pipeline:

```yaml
pipeline:
  input_dir: "runs/older files"        # Directory with Excel files
  file_pattern: "*.xls*"               # File pattern to match
  output_parquet: "runs/combined_runs.parquet"
  parallel_load: true                  # Enable parallel processing
  n_workers: 20                        # Number of parallel workers
  chunk_size: 10000                    # Rows per chunk

supabase:
  batch_size: 1000                     # Upload batch size

logging:
  level: "INFO"                        # Log level (DEBUG, INFO, WARNING, ERROR)
```

## Data Processing Features

### Incremental Processing
- Tracks last processed date
- Only processes new or modified files
- Automatic deduplication of records

### Data Validation
- Validates numeric ranges (no negative prices/sizes)
- Checks date/time formats
- Comprehensive data quality reporting
- Schema validation against Supabase table

### Data Cleaning
- Removes duplicate records
- Handles missing values
- Converts data types for database compatibility
- Sorts data by date and time

## Logging

All operations are logged to:
- Console output
- Log file: `runs/logs/pipeline.log`

Log levels available: DEBUG, INFO, WARNING, ERROR

## Error Handling

The pipeline includes comprehensive error handling:
- Individual file processing errors don't stop the entire pipeline
- Detailed error logging with stack traces
- Graceful handling of network/database issues
- Validation errors are clearly reported

## Monitoring

Each pipeline run provides:
- Processing statistics (files processed, rows handled, duration)
- Data quality reports
- Memory and performance metrics
- Success/failure status for each step

## Troubleshooting

**Connection Issues**:
```bash
# Test all connections
poetry run python scripts/run_pipeline.py --test
```

**View Detailed Logs**:
```bash
# Check the log file
cat "runs/logs/pipeline.log"
```

**Configuration Issues**:
- Verify `.env` file exists with correct credentials
- Check `config/config.yaml` paths and settings
- Ensure input directory exists and contains Excel files

**Data Issues**:
- Check data quality reports in logs
- Verify Excel file formats and column names
- Review Supabase table schema compatibility

## Performance Tips

1. **Parallel Processing**: Adjust `n_workers` based on your system
2. **Batch Size**: Tune `batch_size` for optimal upload performance
3. **Chunk Size**: Adjust `chunk_size` for memory management
4. **Log Level**: Use `WARNING` or `ERROR` for production runs to reduce log volume 