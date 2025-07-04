import pandas as pd
from typing import Dict, Any

class DataReporter:
    """Generates formatted string reports from validation results."""

    @staticmethod
    def format_header(title: str) -> str:
        """Creates a standardized header for a report section."""
        return f"\n{'=' * 25}\n=== {title.upper()} ===\n{'=' * 25}"

    @staticmethod
    def generate_summary_report(df: pd.DataFrame) -> str:
        """Generates a high-level summary of the DataFrame."""
        report = [DataReporter.format_header("Final Data Snapshot")]
        report.append(f"Total Rows: {df.shape[0]}")
        report.append(f"Total Columns: {df.shape[1]}")
        
        # Memory usage
        mem_usage_mb = df.memory_usage(deep=True).sum() / 1024 ** 2
        report.append(f"Memory Usage: {mem_usage_mb:.2f} MB")
        
        # DataFrame Info
        report.append("\n--- DataFrame Info ---")
        # To get the info string, we need to capture stdout
        import io
        buffer = io.StringIO()
        df.info(buf=buffer, verbose=False)
        report.append(buffer.getvalue())
        
        return "\n".join(report)

    @staticmethod
    def generate_data_quality_report(validation_results: Dict[str, Any]) -> str:
        """Generates a detailed data quality report."""
        report = [DataReporter.format_header("Data Quality Report")]

        # Null value analysis
        if 'null_analysis' in validation_results:
            report.append("\n--- Null Value Analysis (Top 5) ---")
            null_df = pd.DataFrame.from_dict(validation_results['null_analysis'], orient='index')
            null_df = null_df.sort_values(by='percentage', ascending=False)
            for col, stats in null_df.head(5).iterrows():
                report.append(f"  - {col}: {stats['count']} nulls ({stats['percentage']:.2f}%)")
        
        # Statistical summary
        if 'statistical_summary' in validation_results:
            report.append("\n--- Statistical Summary (Numeric Columns) ---")
            stats_df = pd.DataFrame(validation_results['statistical_summary']).T
            # Format the numbers for readability
            for col in stats_df.columns:
                 if pd.api.types.is_numeric_dtype(stats_df[col]):
                    stats_df[col] = stats_df[col].apply(lambda x: f'{x:,.2f}')
            report.append(stats_df.to_string(justify='right'))

        # Categorical distribution
        if 'categorical_distribution' in validation_results:
            report.append("\n--- Categorical Value Distribution (Sample) ---")
            for col, data in list(validation_results['categorical_distribution'].items())[:3]: # Report on first 3
                report.append(f"  - Column '{col}':")
                for value, count in list(data['value_counts'].items())[:3]: # Report on top 3 values
                    report.append(f"    '{value}': {count} ({data['percentages'].get(value, 0):.2f}%)")
        
        return "\n".join(report)
        
    @staticmethod
    def generate_validation_error_report(validation_errors: Dict[str, Any]) -> str:
        """Generates a report for data that failed validation checks."""
        report = [DataReporter.format_header("Validation Errors")]
        
        if not validation_errors:
            report.append("[OK] All data validation checks passed.")
            return "\n".join(report)

        if 'non_numeric_rows' in validation_errors and validation_errors['non_numeric_rows']:
            report.append("\n--- Non-Numeric Data in Numeric Columns ---")
            for col, rows in validation_errors['non_numeric_rows'].items():
                report.append(f"  - Column '{col}': Found {len(rows)} non-numeric entries.")
        
        # Add more error reporting sections here as needed

        return "\n".join(report) 