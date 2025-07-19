# QUICK REFERENCE GUIDE
## Trading Data Pipeline - Developer Quick Start

**Last Updated**: July 4, 2025 - 4:56 PM EST  
**Pipeline Status**: ✅ FULLY OPERATIONAL  

---

## ESSENTIAL COMMANDS

### **Run Full Pipeline**
```bash
poetry run python run_pipe.py
```

### **Interactive Menu**
```bash
poetry run python run_pipe.py --menu
```

### **Individual Stages**
```bash
# Universe processing
poetry run python run_pipe.py --universe

# Portfolio processing  
poetry run python run_pipe.py --portfolio

# Historical G-spread processing
poetry run python run_pipe.py --historical-gspread

# G-spread analytics
poetry run python run_pipe.py --gspread-analytics

# Trading runs processing
poetry run python run_pipe.py --runs-excel

# Runs monitoring
poetry run python run_pipe.py --runs-monitor
```

### **Environment Setup**
```bash
# Install dependencies
poetry install

# Activate environment
poetry shell

# Check environment
poetry env info
```

---

## KEY FILE LOCATIONS

### **Executables**
- **Main Pipeline**: `run_pipe.py`
- **G-Spread Analytics**: `historical g spread/g_z.py`
- **Universe Processor**: `universe/universe_raw_to_parquet.py`
- **Portfolio Processor**: `portfolio/portfolio_excel_to_parquet.py`
- **Runs Processor**: `runs/excel_to_df_debug.py`

### **Configuration**
- **Main Config**: `config/config.yaml`
- **Dependencies**: `pyproject.toml`
- **Git Ignore**: `.gitignore`

### **Data Locations**
- **Raw Data**: `*/raw data/` directories
- **Processed Data**: `*/parquet/` directories
- **Analytics Output**: `*/processed data/` directories
- **Logs**: `logs/` directory

### **Core Infrastructure**
- **Pipeline Manager**: `src/orchestrator/pipeline_manager.py`
- **Configuration**: `src/orchestrator/pipeline_config.py`
- **Logging System**: `src/utils/logging.py`
- **Data Processors**: `src/pipeline/` directory

---

## CURRENT DATA STATUS

| Data Source | Records | Last Updated | Status |
|-------------|---------|--------------|---------|
| Universe | 6,088 | 2025-07-04 | ✅ Current |
| Portfolio | 10,085 | 2025-07-04 | ✅ Current |
| Historical G-Spread | 2,929,848 | 2025-07-04 | ✅ Current |
| Trading Runs | 232,284 | 2025-07-02 | ✅ Current |

---

## TROUBLESHOOTING

### **Common Issues & Solutions**

#### **Unicode/Encoding Errors**
**Fixed**: All Unicode characters replaced with ASCII equivalents  
**If you see**: `UnicodeEncodeError` or `UnicodeDecodeError`  
**Solution**: Use `safe_read_csv()` function for file reading  

#### **Pipeline Stage Failures**
**Check**: 
1. Log files in `logs/` directory
2. Input data in `*/raw data/` directories
3. Configuration in `config/config.yaml`

#### **"No New Files" Message**
**Normal**: Incremental processing working correctly  
**Means**: Using existing processed data (no reprocessing needed)  

#### **Path Not Found Errors**
**Check**:
1. Folder structure matches expected layout
2. Raw data files in correct locations
3. File permissions

### **Debugging Commands**
```bash
# Check logs
ls -la logs/

# Validate parquet files
poetry run python -c "import pandas as pd; print(pd.read_parquet('universe/universe.parquet').shape)"

# Test individual processor
poetry run python universe/universe_raw_to_parquet.py

# Reset runs processing (if needed)
poetry run python runs/excel_to_df_debug.py --reset-date
```

### **Performance Monitoring**
- **Expected Runtime**: ~41 seconds for full pipeline
- **Memory Usage**: Peak ~200MB
- **Parallel Stages**: 2 groups, up to 4 stages in parallel

---

## DEVELOPMENT WORKFLOW

### **Making Changes**
1. **Modify Code**: Edit processing scripts or configuration
2. **Test Locally**: Run affected pipeline stages
3. **Check Logs**: Verify processing in `logs/` directory
4. **Validate Output**: Inspect parquet files
5. **Update Documentation**: Add entry to changelog
6. **Commit**: Use descriptive commit messages

### **Adding New Data Sources**
1. Create folder structure: `new_source/raw data/`, `new_source/parquet/`, `new_source/processed data/`
2. Create processor script: `new_source/new_source_processor.py`
3. Add stage to pipeline manager: `src/orchestrator/pipeline_manager.py`
4. Update configuration: `config/config.yaml`
5. Add logging: Use LogManager pattern
6. Test thoroughly before deployment

### **Code Standards**
- **Python Style**: PEP 8 compliance
- **Error Handling**: Always use try/except with informative messages
- **Logging**: Use LogManager for all output
- **File Reading**: Use `safe_read_csv()` for CSV files
- **Documentation**: Comprehensive docstrings

---

## EMERGENCY PROCEDURES

### **Pipeline Completely Broken**
1. **Check Recent Changes**: Review git log
2. **Restore Configuration**: Reset `config/config.yaml` to known good state
3. **Clear Locks**: Remove any `.lock` files
4. **Restart Environment**: `poetry shell` in new terminal
5. **Run Individual Stages**: Test each stage separately

### **Data Corruption**
1. **Don't Panic**: Raw data is preserved
2. **Check Backups**: Look for `.bak` files
3. **Regenerate**: Delete parquet files and rerun pipeline
4. **Validate**: Compare record counts with previous runs

### **Performance Issues**
1. **Check System Resources**: Memory and CPU usage
2. **Review Logs**: Look for error patterns
3. **Reduce Scope**: Run individual stages
4. **Clear Cache**: Delete temporary files

---

## CONTACT & ESCALATION

### **For Technical Issues**
1. **Check This Guide**: Common solutions above
2. **Review Logs**: All processing logs in `logs/` directory
3. **Check Changelog**: `change logs/project_changelog.md` for recent changes
4. **Test Individual Components**: Run stages separately

### **For Data Issues**
1. **Validate Input**: Check raw data files
2. **Compare Outputs**: Review record counts and data ranges
3. **Check Processing**: Review processor-specific logs
4. **Verify Configuration**: Ensure config matches data format

---

## USEFUL COMMANDS REFERENCE

```bash
# Environment
poetry install                          # Install dependencies
poetry shell                           # Activate environment
poetry env info                        # Environment information

# Pipeline Execution
poetry run python run_pipe.py          # Full pipeline
poetry run python run_pipe.py --menu   # Interactive menu
poetry run python run_pipe.py --help   # Show all options

# Individual Processors
poetry run python universe/universe_raw_to_parquet.py
poetry run python portfolio/portfolio_excel_to_parquet.py
poetry run python "historical g spread/g_z.py"
poetry run python "historical g spread/g_z.py"
poetry run python runs/excel_to_df_debug.py
poetry run python runs/run_monitor.py

# Data Inspection
poetry run python -c "import pandas as pd; df=pd.read_parquet('universe/universe.parquet'); print(f'Shape: {df.shape}'); print(df.head())"

# Log Monitoring
Get-Content logs/g_spread_processor.log -Tail 50    # PowerShell
tail -f logs/g_spread_processor.log                 # Linux/Mac

# Git Operations
git status                              # Check repository status
git log --oneline -10                   # Recent commits
git diff                                # Show changes
```

---

**This guide covers 95% of daily development tasks. For detailed technical information, see `change logs/project_changelog.md`.** 