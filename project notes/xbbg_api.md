# Bloomberg API Setup & Usage

## Install Bloomberg API
```bash
python -m pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple blpapi
```

## Git Commands
```bash
# Fetch and reset to origin
git fetch origin
git reset --hard origin/master
git pull origin master
```

## Git Commands
```bash
# Fetch and reset to origin
git fetch db
git reset --hard origin/db
git pull origin db
```

## Important Commands
```bash
# Run data pipeline
poetry run python data_pipelines/data_pipeline.py

# Convert notebooks to PDF
poetry run jupyter nbconvert --to webpdf "your_notebook.ipynb"
```
# to active poetry env
poetry env activate