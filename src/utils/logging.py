"""
🔒 SECURE LOGGING CONFIGURATION
===============================
Secure logging configuration and utilities with data sanitization
and audit trail capabilities for financial trading systems.
"""
import logging
import logging.handlers
from pathlib import Path
from typing import Optional
import sys
import os
from datetime import datetime

from .security import InputValidator


class SecureLogManager:
    """
    Secure logging manager with data sanitization and audit capabilities.
    
    Features:
    - Automatic sensitive data sanitization
    - Secure log rotation with retention policies
    - Audit trail separation
    - Performance monitoring
    - Security event logging
    """
    
    def __init__(self, log_file: str, log_level: str = "INFO", log_format: str = None,
                 enable_audit: bool = True, max_file_size: int = 10 * 1024 * 1024,
                 backup_count: int = 5):
        """
        Initialize secure logging manager.
        
        Args:
            log_file: Main log file path
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_format: Custom log format
            enable_audit: Enable separate audit logging
            max_file_size: Maximum log file size in bytes before rotation
            backup_count: Number of backup files to keep
        """
        self.log_file = log_file
        self.log_level = getattr(logging, log_level.upper())
        self.log_format = log_format or "[%(asctime)s] %(name)s - %(levelname)s: %(message)s"
        self.enable_audit = enable_audit
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        
        # Setup logging components
        self._setup_logging()
        
        # Initialize audit logger if enabled
        if enable_audit:
            self._setup_audit_logging()
    
    def _setup_logging(self):
        """Setup secure logging configuration with rotation"""
        # Ensure log directory exists
        log_path = Path(self.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # Create rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Create formatter
        formatter = logging.Formatter(self.log_format)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Setup root logger
        logging.basicConfig(
            level=self.log_level,
            handlers=[file_handler, console_handler]
        )
        
        # Get logger for the pipeline
        self.logger = logging.getLogger("pipeline")
        self.logger.setLevel(self.log_level)
        
        # Security logger for security events
        self.security_logger = logging.getLogger("security")
        self.security_logger.setLevel(logging.WARNING)
    
    def _setup_audit_logging(self):
        """Setup separate audit logging"""
        audit_file = self.log_file.replace('.log', '_audit.log')
        audit_path = Path(audit_file)
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create audit logger
        self.audit_logger = logging.getLogger("audit")
        self.audit_logger.setLevel(logging.INFO)
        
        # Create audit file handler with rotation
        audit_handler = logging.handlers.RotatingFileHandler(
            audit_file,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count * 2,  # Keep more audit logs
            encoding='utf-8'
        )
        
        # Audit log format includes more details
        audit_formatter = logging.Formatter(
            "%(asctime)s - AUDIT - %(levelname)s - %(message)s"
        )
        audit_handler.setFormatter(audit_formatter)
        
        # Don't propagate audit logs to root logger
        self.audit_logger.propagate = False
        self.audit_logger.addHandler(audit_handler)
    
    def _sanitize_message(self, msg: str) -> str:
        """Sanitize log message to remove sensitive information"""
        return InputValidator.sanitize_log_message(msg)
    
    def info(self, msg: str, sanitize: bool = True):
        """Log an info message with optional sanitization"""
        if sanitize:
            msg = self._sanitize_message(msg)
        self.logger.info(msg)
    
    def error(self, msg: str, exc: Optional[Exception] = None, sanitize: bool = True):
        """Log an error message with optional exception and sanitization"""
        if sanitize:
            msg = self._sanitize_message(msg)
        
        if exc:
            # In production, don't log full exception details
            if self.log_level <= logging.DEBUG:
                self.logger.error(f"{msg}: {str(exc)}", exc_info=True)
            else:
                self.logger.error(f"{msg}: {type(exc).__name__}")
        else:
            self.logger.error(msg)
    
    def warning(self, msg: str, sanitize: bool = True):
        """Log a warning message with optional sanitization"""
        if sanitize:
            msg = self._sanitize_message(msg)
        self.logger.warning(msg)
    
    def debug(self, msg: str, sanitize: bool = True):
        """Log a debug message with optional sanitization"""
        if sanitize:
            msg = self._sanitize_message(msg)
        self.logger.debug(msg)
    
    def critical(self, msg: str, sanitize: bool = True):
        """Log a critical message with optional sanitization"""
        if sanitize:
            msg = self._sanitize_message(msg)
        self.logger.critical(msg)
    
    def security_event(self, event: str, details: Optional[dict] = None, 
                      severity: str = "WARNING"):
        """
        Log security events with special handling.
        
        Args:
            event: Security event description
            details: Additional event details (will be sanitized)
            severity: Event severity level
        """
        # Always sanitize security events
        sanitized_event = self._sanitize_message(event)
        
        # Format security log entry
        log_entry = f"SECURITY EVENT: {sanitized_event}"
        if details:
            sanitized_details = {k: self._sanitize_message(str(v)) for k, v in details.items()}
            log_entry += f" | Details: {sanitized_details}"
        
        # Log to security logger
        security_level = getattr(logging, severity.upper(), logging.WARNING)
        self.security_logger.log(security_level, log_entry)
        
        # Also log to audit if enabled
        if self.enable_audit:
            self.audit_logger.info(log_entry)
    
    def audit_financial_operation(self, operation: str, user_id: str, 
                                 amount: float, currency: str, result: str,
                                 additional_data: Optional[dict] = None):
        """
        Log financial operations to audit trail.
        
        Args:
            operation: Type of financial operation
            user_id: User performing operation
            amount: Financial amount
            currency: Currency code
            result: Operation result
            additional_data: Additional operation data
        """
        if not self.enable_audit:
            return
        
        # Create audit entry
        audit_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'operation': operation,
            'user_id': user_id,
            'amount': amount,
            'currency': currency,
            'result': result,
            'session_id': os.getenv('SESSION_ID', 'unknown'),
            'ip_address': os.getenv('CLIENT_IP', 'unknown')
        }
        
        if additional_data:
            audit_entry.update(additional_data)
        
        # Log to audit trail
        self.audit_logger.info(f"FINANCIAL_OPERATION: {audit_entry}")
    
    def performance_metric(self, metric_name: str, value: float, unit: str = "ms"):
        """
        Log performance metrics.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit of measurement
        """
        metric_entry = f"PERFORMANCE: {metric_name}={value}{unit}"
        self.logger.info(metric_entry)
    
    @staticmethod
    def log_error(msg: str, exc: Optional[Exception] = None):
        """Static method to log an error message with optional exception"""
        sanitized_msg = InputValidator.sanitize_log_message(msg)
        if exc:
            logging.error(f"{sanitized_msg}: {type(exc).__name__}")
        else:
            logging.error(sanitized_msg)


# For backward compatibility
class LogManager(SecureLogManager):
    """Backward compatibility wrapper for existing code"""
    
    def __init__(self, log_file: str, log_level: str = "INFO", log_format: str = None):
        super().__init__(log_file, log_level, log_format) 