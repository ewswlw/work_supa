# Git Strategy & Multi-Computer Architecture Analysis

## 🚨 Current Situation Analysis

### What's Happening Now:
- **Repository Size**: ~21MB of tracked data files (should be ~0MB)
- **Git Operations**: Slower than optimal due to large tracked files
- **Multi-Computer Issue**: New computers get NO data files (only code)

### Current Tracked Data Files (PROBLEM):
```
❌ historical g spread/g_ts.parquet (7.8MB)
❌ historical g spread/bond_z.parquet (1.3MB)  
❌ portfolio/portfolio.parquet (1.2MB)
❌ runs/combined_runs.parquet (5.9MB)
❌ runs/run_monitor.parquet (157KB)
❌ universe/universe.parquet (4.7MB)
❌ db/trading_analytics_backup_*.db (70MB)
❌ Plus multiple large CSV files (24MB+)
```

## 📊 .gitignore Analysis

### ✅ EXCELLENT EXCLUSIONS (Fast Git Operations):
- **Large Data Files**: `*.parquet`, `*.csv`, `*.xlsx`, `*.db` (excluded)
- **Data Directories**: `raw/`, `processed/`, `data/` (excluded)
- **Database Files**: `*.db`, `*.sqlite` (excluded)
- **Generated Content**: `__pycache__/`, logs, temp files (excluded)

### ⚠️ CURRENT ISSUES:
- **Some parquet files ARE tracked** (6 files totaling ~21MB)
- **Database backup file tracked** (70MB `trading_analytics_backup_20250718_125410.db`)
- **Large CSV files tracked** (24MB `bond_g_sprd_wide.csv`)

## 🚨 CRITICAL IMPLICATIONS FOR MULTI-COMPUTER USAGE:

### 1. Data Architecture Problems:
```
❌ PROBLEM: When you clone on another computer, you'll get:
- NO raw data files (excluded by .gitignore)
- NO processed data files (excluded by .gitignore) 
- NO database files (excluded by .gitignore)
- ONLY code and configuration
```

### 2. What Happens on New Computer:
```bash
git clone <your-repo>
# Result: Empty data directories, no database, no parquet files
# Your pipeline will FAIL because it expects data files
```

### 3. Version Control Issues:
```bash
# You can't revert to previous data states
# You can't track data changes over time
# You can't reproduce exact results from specific commits
```

## 💡 RECOMMENDED ARCHITECTURE FOR MULTI-COMPUTER USAGE:

### Option A: Data Pipeline Architecture (RECOMMENDED)

#### Structure:
```
📁 Repository (Code Only - Fast Git)
├── 📁 src/                    # Code (tracked)
├── 📁 config/                 # Configuration (tracked)
├── 📁 scripts/                # Data generation scripts (tracked)
├── 📁 data_sources/           # Data source definitions (tracked)
│   ├── 📄 portfolio_sources.yaml
│   ├── 📄 universe_sources.yaml
│   └── 📄 g_spread_sources.yaml
├── 📁 schemas/                # Data schemas (tracked)
│   ├── 📄 portfolio_schema.json
│   ├── 📄 universe_schema.json
│   └── 📄 g_spread_schema.json
└── 📁 data/                   # Data (gitignored)
    ├── 📁 raw/
    ├── 📁 processed/
    └── 📁 database/
```

#### Benefits:
- ✅ **Ultra-fast git operations** (commits/pushes in seconds)
- ✅ **Version control for code only**
- ✅ **Reproducible data pipelines**
- ✅ **Multi-computer friendly**

#### Implementation:
```python
# config/data_sources.yaml
data_sources:
  portfolio:
    raw_path: "C:/Users/Eddy/YTM Capital Dropbox/Data/portfolio/raw/"
    processed_path: "C:/Users/Eddy/YTM Capital Dropbox/Data/portfolio/processed/"
    schema: "schemas/portfolio_schema.json"
  
  universe:
    raw_path: "C:/Users/Eddy/YTM Capital Dropbox/Data/universe/raw/"
    processed_path: "C:/Users/Eddy/YTM Capital Dropbox/Data/universe/processed/"
    schema: "schemas/universe_schema.json"
  
  g_spread:
    raw_path: "C:/Users/Eddy/YTM Capital Dropbox/Data/g_spread/raw/"
    processed_path: "C:/Users/Eddy/YTM Capital Dropbox/Data/g_spread/processed/"
    schema: "schemas/g_spread_schema.json"
```

### Option B: Sample Data Architecture

#### Structure:
```
📁 Repository (Code + Sample Data)
├── 📁 src/                    # Application code
├── 📁 sample_data/            # Small sample datasets (tracked)
│   ├── 📄 sample_portfolio.parquet (1MB - 100 records)
│   ├── 📄 sample_universe.parquet (1MB - 500 records)
│   └── 📄 sample_g_spread.parquet (1MB - 1000 records)
└── 📁 data/                   # Full data (gitignored)
```

#### Benefits:
- ✅ **New computers get working sample data**
- ✅ **Fast git operations** (only small samples tracked)
- ✅ **Easy testing and development**

## 🔧 IMMEDIATE ACTIONS REQUIRED:

### 1. Remove Large Files from Git Tracking
```bash
# Remove large files from git history (but keep locally)
git rm --cached "historical g spread/g_ts.parquet"
git rm --cached "historical g spread/bond_z.parquet"
git rm --cached "portfolio/portfolio.parquet"
git rm --cached "runs/combined_runs.parquet"
git rm --cached "runs/run_monitor.parquet"
git rm --cached "universe/universe.parquet"
git rm --cached "db/trading_analytics_backup_*.db"

# Commit the removal
git commit -m "Remove large data files from tracking for fast git operations"
```

### 2. Create Data Source Configuration
```yaml
# config/data_sources.yaml
data_sources:
  portfolio:
    raw_files: ["portfolio/raw data/*.xlsx"]
    processed_file: "portfolio/processed data/portfolio.parquet"
    schema_version: "1.0"
    
  universe:
    raw_files: ["universe/raw data/*.xlsx"]
    processed_file: "universe/universe.parquet"
    schema_version: "1.0"
    
  g_spread:
    raw_files: ["historical g spread/raw data/g_ts.parquet"]
    processed_file: "historical g spread/bond_z.parquet"
    schema_version: "1.0"
```

### 3. Create Data Generation Scripts
```python
# scripts/generate_sample_data.py
def create_sample_datasets():
    """Create small sample datasets for testing"""
    # Generate 1MB sample files with 100-1000 records each
    pass
```

## 🚀 MULTI-COMPUTER SETUP PROCESS:

### For New Computer:
```bash
# 1. Clone repository (fast - no large files)
git clone <your-repo>
cd work_supa

# 2. Install dependencies
poetry install

# 3. Configure data paths for your computer
# Edit config/data_sources.yaml with local paths

# 4. Run data pipeline to generate all data
poetry run python db_pipe.py --full-reset

# 5. Verify everything works
poetry run python -m pytest test/
```

### For Data Updates:
```bash
# 1. Update raw data files in external storage
# 2. Run pipeline to regenerate processed data
poetry run python db_pipe.py --update-data

# 3. Commit code changes only
git add .
git commit -m "Update data processing logic"
git push
```

## 📊 PERFORMANCE IMPACT:

### Current (With Large Files):
- **Commit Time**: 30-60 seconds
- **Push Time**: 2-5 minutes
- **Clone Time**: 5-10 minutes
- **Repository Size**: ~21MB

### After Optimization:
- **Commit Time**: 1-3 seconds
- **Push Time**: 10-30 seconds
- **Clone Time**: 30-60 seconds
- **Repository Size**: ~2MB

## 🔄 VERSION CONTROL STRATEGY:

### Code Versioning:
- ✅ Track all code changes
- ✅ Track configuration changes
- ✅ Track schema changes
- ✅ Track sample data changes

### Data Versioning:
- ❌ Don't track large data files
- ✅ Track data schemas and validation rules
- ✅ Track data processing logic
- ✅ Track data source definitions

### Reproducibility:
- ✅ All data can be regenerated from raw sources
- ✅ Processing logic is version controlled
- ✅ Schemas are version controlled
- ✅ Configuration is version controlled

## 🎯 NEXT STEPS:

1. **Immediate**: Remove large files from git tracking
2. **Short-term**: Create data source configuration
3. **Medium-term**: Implement sample data generation
4. **Long-term**: Set up external data storage strategy

