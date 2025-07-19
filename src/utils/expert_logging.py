"""
Expert logging configuration and utilities for the pipeline.
"""
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import sys


def setup_logging(log_file: str = "logs/pipeline.log", 
                 log_level: str = "INFO", 
                 log_format: str = None,
                 config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Setup expert logging configuration for the pipeline.
    
    Args:
        log_file: Path to log file
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        config: Optional configuration dictionary
        
    Returns:
        Configured logger instance
    """
    # Ensure log directory exists
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Set default format if not provided
    if log_format is None:
        log_format = "[%(asctime)s] %(name)s - %(levelname)s: %(message)s"
    
    # Get log level
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Setup root logger
    logging.basicConfig(
        level=level,
        handlers=[file_handler, console_handler]
    )
    
    # Get logger for the pipeline
    logger = logging.getLogger("pipeline")
    logger.setLevel(level)
    
    # Log initial setup
    logger.info(f"Expert logging setup completed - Level: {log_level}, File: {log_file}")
    
    return logger


class ExpertLogManager:
    """Advanced logging manager with additional features"""
    
    def __init__(self, log_file: str, log_level: str = "INFO", log_format: str = None):
        self.log_file = log_file
        self.log_level = getattr(logging, log_level.upper())
        self.log_format = log_format or "[%(asctime)s] %(name)s - %(levelname)s: %(message)s"
        self.logger = setup_logging(log_file, log_level, log_format)
    
    def info(self, msg: str):
        """Log an info message"""
        self.logger.info(msg)
    
    def error(self, msg: str, exc: Optional[Exception] = None):
        """Log an error message with optional exception"""
        if exc:
            self.logger.error(f"{msg}: {str(exc)}", exc_info=True)
        else:
            self.logger.error(msg)
    
    def warning(self, msg: str):
        """Log a warning message"""
        self.logger.warning(msg)
    
    def debug(self, msg: str):
        """Log a debug message"""
        self.logger.debug(msg)
    
    def critical(self, msg: str, exc: Optional[Exception] = None):
        """Log a critical message with optional exception"""
        if exc:
            self.logger.critical(f"{msg}: {str(exc)}", exc_info=True)
        else:
            self.logger.critical(msg)
    
    @staticmethod
    def log_error(msg: str, exc: Optional[Exception] = None):
        """Static method to log an error message with optional exception"""
        logger = logging.getLogger("pipeline")
        if exc:
            logger.error(f"{msg}: {str(exc)}", exc_info=True)
        else:
            logger.error(msg) 