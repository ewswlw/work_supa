"""
Base classes for pipeline processors.
"""
from abc import ABC, abstractmethod
from typing import Any, Optional
from datetime import datetime
import time

from ..models.data_models import ProcessingResult, ProcessingStats
from ..utils.config import PipelineConfig
from ..utils.logging import LogManager


class BaseProcessor(ABC):
    """Base class for all processors"""
    
    def __init__(self, config: Any, logger: LogManager):
        self.config = config
        self.logger = logger
        self.stats = ProcessingStats(
            files_processed=0,
            rows_processed=0,
            rows_loaded=0,
            rows_duplicated=0,
            rows_invalid=0,
            processing_time=0.0,
            start_time=datetime.now(),
            end_time=datetime.now()
        )
    
    @abstractmethod
    def process(self, *args, **kwargs) -> ProcessingResult:
        """Process the data"""
        pass
    
    def validate_config(self) -> bool:
        """Validate the processor configuration"""
        return True
    
    def _start_timer(self):
        """Start the processing timer"""
        self.stats.start_time = datetime.now()
        self._start_time = time.time()
    
    def _stop_timer(self):
        """Stop the processing timer"""
        self.stats.end_time = datetime.now()
        self.stats.processing_time = time.time() - self._start_time
    
    def log_stats(self):
        """Log processing statistics"""
        self.logger.info("Processing Statistics:")
        self.logger.info(f"  Files processed: {self.stats.files_processed}")
        self.logger.info(f"  Rows processed: {self.stats.rows_processed}")
        self.logger.info(f"  Rows loaded: {self.stats.rows_loaded}")
        self.logger.info(f"  Rows duplicated: {self.stats.rows_duplicated}")
        self.logger.info(f"  Rows invalid: {self.stats.rows_invalid}")
        self.logger.info(f"  Processing time: {self.stats.processing_time:.2f} seconds")
        self.logger.info(f"  Start time: {self.stats.start_time}")
        self.logger.info(f"  End time: {self.stats.end_time}")


class ProcessingError(Exception):
    """Custom exception for processing errors"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error


class ValidationError(ProcessingError):
    """Custom exception for validation errors"""
    pass


class ConfigurationError(ProcessingError):
    """Custom exception for configuration errors"""
    pass 