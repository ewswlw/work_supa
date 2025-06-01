"""
Main pipeline script for orchestrating the complete data processing workflow.

This script:
1. Processes Excel files into a cleaned DataFrame
2. Saves the data to Parquet format
3. Uploads the data to Supabase
"""
import sys
import os
from pathlib import Path
from datetime import datetime

# Add src to Python path
project_root = Path(__file__).parent.parent
src_path = str(project_root / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Now import with absolute paths from src
from pipeline.excel_processor import ExcelProcessor
from pipeline.parquet_processor import ParquetProcessor
from pipeline.supabase_processor import SupabaseProcessor
from utils.config import ConfigManager
from utils.logging import LogManager
from models.data_models import ProcessingResult


class DataPipeline:
    """Main data pipeline orchestrator"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the pipeline with configuration"""
        try:
            # Load configuration
            self.config_manager = ConfigManager(config_path)
            
            # Setup logging
            self.logger = LogManager(
                log_file=self.config_manager.pipeline_config.log_file,
                log_level=self.config_manager.logging_config.level,
                log_format=self.config_manager.logging_config.format
            )
            
            # Initialize processors
            self.excel_processor = ExcelProcessor(
                self.config_manager.pipeline_config, 
                self.logger
            )
            self.parquet_processor = ParquetProcessor(
                self.config_manager.pipeline_config, 
                self.logger
            )
            self.supabase_processor = SupabaseProcessor(
                self.config_manager.supabase_config, 
                self.logger
            )
            
            self.logger.info("Pipeline initialized successfully")
            
        except Exception as e:
            print(f"Failed to initialize pipeline: {e}")
            sys.exit(1)
    
    def run(self) -> bool:
        """Run the complete pipeline"""
        start_time = datetime.now()
        self.logger.info(f"Starting complete data pipeline at {start_time}")
        
        try:
            # Step 1: Process Excel files
            self.logger.info("=" * 50)
            self.logger.info("STEP 1: Processing Excel files")
            self.logger.info("=" * 50)
            
            excel_result = self.excel_processor.process()
            
            if not excel_result.success:
                self.logger.error("Excel processing failed")
                if excel_result.error:
                    self.logger.error(f"Error: {excel_result.error}")
                return False
            
            # Check if there's data to process
            if excel_result.data is None:
                self.logger.info("No new data to process. Pipeline completed.")
                return True
            
            # Step 2: Save to Parquet
            self.logger.info("=" * 50)
            self.logger.info("STEP 2: Saving to Parquet")
            self.logger.info("=" * 50)
            
            parquet_result = self.parquet_processor.save_to_parquet(excel_result.data)
            
            if not parquet_result.success:
                self.logger.error("Parquet save failed")
                if parquet_result.error:
                    self.logger.error(f"Error: {parquet_result.error}")
                return False
            
            # Step 3: Upload to Supabase
            self.logger.info("=" * 50)
            self.logger.info("STEP 3: Uploading to Supabase")
            self.logger.info("=" * 50)
            
            upload_result = self.supabase_processor.process(parquet_result.data, clear_table=False)
            
            if not upload_result.success:
                self.logger.error("Supabase upload failed")
                if upload_result.error:
                    self.logger.error(f"Error: {upload_result.error}")
                return False
            
            # Pipeline completed successfully
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("=" * 50)
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 50)
            self.logger.info(f"Total duration: {duration}")
            
            # Log summary statistics
            self._log_pipeline_summary(excel_result, parquet_result, upload_result)
            
            return True
            
        except Exception as e:
            self.logger.error("Pipeline failed with unexpected error", e)
            return False
    
    def _log_pipeline_summary(self, excel_result, parquet_result, upload_result):
        """Log a summary of the pipeline execution"""
        self.logger.info("PIPELINE SUMMARY:")
        
        # Excel processing summary
        if excel_result.metadata:
            self.logger.info(f"  Excel files processed: {excel_result.metadata.get('files_processed', 0)}")
            self.logger.info(f"  Rows processed: {excel_result.metadata.get('rows_processed', 0)}")
        
        # Parquet summary
        if parquet_result.metadata:
            self.logger.info(f"  Rows saved to Parquet: {parquet_result.metadata.get('rows_saved', 0)}")
            self.logger.info(f"  Parquet file: {parquet_result.metadata.get('file_path', 'N/A')}")
        
        # Supabase summary
        if upload_result.metadata:
            self.logger.info(f"  Rows uploaded to Supabase: {upload_result.metadata.get('rows_uploaded', 0)}")
            self.logger.info(f"  Supabase table: {upload_result.metadata.get('table', 'N/A')}")
    
    def test_connections(self) -> bool:
        """Test all connections and configurations"""
        self.logger.info("Testing pipeline connections and configurations")
        
        try:
            # Test file system access
            self.logger.info("Testing file system access...")
            if not os.path.exists(self.config_manager.pipeline_config.input_dir):
                self.logger.error(f"Input directory not accessible: {self.config_manager.pipeline_config.input_dir}")
                return False
            
            # Test Parquet file access
            self.logger.info("Testing Parquet file access...")
            parquet_info = self.parquet_processor.get_file_info()
            self.logger.info(f"Parquet file info: {parquet_info}")
            
            # Test Supabase connection
            self.logger.info("Testing Supabase connection...")
            if not self.supabase_processor.test_connection():
                self.logger.error("Supabase connection test failed")
                return False
            
            # Get Supabase table info
            table_info = self.supabase_processor.get_table_info()
            self.logger.info(f"Supabase table info: {table_info}")
            
            self.logger.info("All connection tests passed")
            return True
            
        except Exception as e:
            self.logger.error("Connection test failed", e)
            return False
    
    def run_excel_only(self) -> bool:
        """Run only the Excel processing step"""
        self.logger.info("Running Excel processing only")
        
        excel_result = self.excel_processor.process()
        
        if excel_result.success:
            self.logger.info("Excel processing completed successfully")
            if excel_result.data is not None:
                self.logger.info(f"Processed {len(excel_result.data)} rows")
            return True
        else:
            self.logger.error("Excel processing failed")
            return False
    
    def run_supabase_only(self) -> bool:
        """Run only the Supabase upload step (from existing Parquet file)"""
        self.logger.info("Running Supabase upload only")
        
        # Load data from Parquet
        parquet_result = self.parquet_processor.load_from_parquet()
        
        if not parquet_result.success or parquet_result.data is None:
            self.logger.error("Failed to load data from Parquet file")
            return False
        
        # Upload to Supabase (table should be manually cleared first)
        upload_result = self.supabase_processor.process(parquet_result.data, clear_table=False)
        
        if upload_result.success:
            self.logger.info("Supabase upload completed successfully")
            return True
        else:
            self.logger.error("Supabase upload failed")
            return False


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the data pipeline")
    parser.add_argument("--test", action="store_true", help="Test connections only")
    parser.add_argument("--excel-only", action="store_true", help="Run Excel processing only")
    parser.add_argument("--supabase-only", action="store_true", help="Run Supabase upload only")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config file")
    
    args = parser.parse_args()
    
    try:
        # Initialize pipeline
        pipeline = DataPipeline(args.config)
        
        # Run based on arguments
        if args.test:
            success = pipeline.test_connections()
        elif args.excel_only:
            success = pipeline.run_excel_only()
        elif args.supabase_only:
            success = pipeline.run_supabase_only()
        else:
            success = pipeline.run()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 