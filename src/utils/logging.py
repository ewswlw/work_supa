"""
Logging configuration and utilities.
"""
import logging
from pathlib import Path
from typing import Optional
import sys


class LogManager:
    """Manages logging configuration and utilities"""
    
    def __init__(self, log_file: str, log_level: str = "INFO", log_format: str = None):
        self.log_file = log_file
        self.log_level = getattr(logging, log_level.upper())
        self.log_format = log_format or "[%(asctime)s] %(levelname)s: %(message)s"
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        # Ensure log directory exists
        Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
        
        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # Setup new logging configuration
        logging.basicConfig(
            level=self.log_level,
            format=self.log_format,
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Get logger for the pipeline
        self.logger = logging.getLogger("pipeline")
        self.logger.setLevel(self.log_level)
    
    def info(self, msg: str):
        """Log an info message"""
        self.logger.info(msg)
    
    def error(self, msg: str, exc: Optional[Exception] = None):
        """Log an error message with optional exception"""
        if exc:
            self.logger.error(f"{msg}: {str(exc)}")
        else:
            self.logger.error(msg)
    
    def warning(self, msg: str):
        """Log a warning message"""
        self.logger.warning(msg)
    
    def debug(self, msg: str):
        """Log a debug message"""
        self.logger.debug(msg)
    
    @staticmethod
    def log_error(msg: str, exc: Optional[Exception] = None):
        """Static method to log an error message with optional exception"""
        if exc:
            logging.error(f"{msg}: {str(exc)}")
        else:
            logging.error(msg) 