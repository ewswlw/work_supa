@echo off
echo ======================================
echo Data Pipeline - Excel to Supabase
echo ======================================
echo.

REM Check if Poetry is available
poetry --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Poetry is not installed or not in PATH
    echo Please install Poetry first: https://python-poetry.org/docs/#installation
    pause
    exit /b 1
)

echo Available options:
echo 1. Run complete pipeline (Excel -> Parquet -> Supabase)
echo 2. Process Excel files only
echo 3. Upload to Supabase only
echo 4. Test connections
echo 5. Exit
echo.

set /p choice="Enter your choice (1-5): "

if "%choice%"=="1" (
    echo Running complete pipeline...
    poetry run python scripts/run_pipeline.py
) else if "%choice%"=="2" (
    echo Processing Excel files only...
    poetry run python scripts/run_pipeline.py --excel-only
) else if "%choice%"=="3" (
    echo Uploading to Supabase only...
    poetry run python scripts/run_pipeline.py --supabase-only
) else if "%choice%"=="4" (
    echo Testing connections...
    poetry run python scripts/run_pipeline.py --test
) else if "%choice%"=="5" (
    echo Exiting...
    exit /b 0
) else (
    echo Invalid choice. Please run the script again.
    pause
    exit /b 1
)

echo.
echo Pipeline execution completed.
echo Check the log file: logs/pipeline.log for details.
echo.
pause 