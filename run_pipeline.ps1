#!/usr/bin/env powershell

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Data Pipeline - Excel to Supabase" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Check if Poetry is available
try {
    $poetryVersion = poetry --version 2>$null
    Write-Host "Found Poetry: $poetryVersion" -ForegroundColor Green
}
catch {
    Write-Host "ERROR: Poetry is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Poetry first: https://python-poetry.org/docs/#installation" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Available options:" -ForegroundColor Yellow
Write-Host "1. Run complete pipeline (Excel -> Parquet -> Supabase)"
Write-Host "2. Process Excel files only"
Write-Host "3. Upload to Supabase only"
Write-Host "4. Test connections"
Write-Host "5. Exit"
Write-Host ""

$choice = Read-Host "Enter your choice (1-5)"

switch ($choice) {
    "1" {
        Write-Host "Running complete pipeline..." -ForegroundColor Green
        poetry run python scripts/run_pipeline.py
    }
    "2" {
        Write-Host "Processing Excel files only..." -ForegroundColor Green
        poetry run python scripts/run_pipeline.py --excel-only
    }
    "3" {
        Write-Host "Uploading to Supabase only..." -ForegroundColor Green
        poetry run python scripts/run_pipeline.py --supabase-only
    }
    "4" {
        Write-Host "Testing connections..." -ForegroundColor Green
        poetry run python scripts/run_pipeline.py --test
    }
    "5" {
        Write-Host "Exiting..." -ForegroundColor Yellow
        exit 0
    }
    default {
        Write-Host "Invalid choice. Please run the script again." -ForegroundColor Red
        Read-Host "Press Enter to exit"
        exit 1
    }
}

Write-Host ""
Write-Host "Pipeline execution completed." -ForegroundColor Green
Write-Host "Check the log file: logs/pipeline.log for details." -ForegroundColor Cyan
Write-Host ""
Read-Host "Press Enter to exit"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Pipeline execution failed." -ForegroundColor Red
    Write-Host "Check the log file: logs/pipeline.log for details." -ForegroundColor Cyan
    exit 1
} 