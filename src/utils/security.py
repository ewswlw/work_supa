"""
🔒 SECURITY UTILITIES
====================
Comprehensive security utilities for input validation, path sanitization,
and secure error handling in financial trading systems.
"""

import os
import re
import hashlib
import base64
from pathlib import Path
from typing import Dict, Any, Optional, List
from enum import Enum
from datetime import datetime
import logging


class ErrorSeverity(Enum):
    """Error severity levels for security classification"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class SecureException(Exception):
    """Base class for secure exceptions with severity classification"""
    
    def __init__(self, message: str, severity: ErrorSeverity = ErrorSeverity.MEDIUM, 
                 details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.severity = severity
        self.details = details or {}
        super().__init__(self.message)


class SecurityError(SecureException):
    """Security-related errors with critical severity"""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, ErrorSeverity.CRITICAL, details)


class ValidationError(SecureException):
    """Input validation errors"""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, ErrorSeverity.HIGH, details)


class PathSanitizer:
    """Secure path sanitization and validation"""
    
    def __init__(self, project_root: Optional[Path] = None):
        self.project_root = project_root or Path.cwd().resolve()
        self.allowed_extensions = {'.py', '.yaml', '.yml', '.json', '.csv', '.parquet', '.xlsx', '.xls'}
    
    def sanitize_script_path(self, script_path: str) -> str:
        """
        Sanitize and validate script path to prevent path traversal attacks.
        
        Args:
            script_path: Path to validate
            
        Returns:
            Sanitized absolute path
            
        Raises:
            SecurityError: If path is unsafe
        """
        if not script_path:
            raise SecurityError("Script path cannot be empty")
        
        # Convert to Path object and resolve
        try:
            path = Path(script_path).resolve()
        except (OSError, ValueError) as e:
            raise SecurityError(f"Invalid path format: {script_path}") from e
        
        # Ensure path is within project directory
        if not str(path).startswith(str(self.project_root)):
            raise SecurityError(f"Script path outside project directory: {script_path}")
        
        # Validate file extension
        if path.suffix not in self.allowed_extensions:
            raise SecurityError(f"Invalid script extension: {path.suffix}")
        
        # Ensure file exists
        if not path.exists():
            raise FileNotFoundError(f"Script not found: {path}")
        
        # Additional security checks
        if path.is_symlink():
            raise SecurityError(f"Symbolic links not allowed: {path}")
        
        return str(path)
    
    def sanitize_file_path(self, file_path: str, must_exist: bool = True) -> str:
        """
        Sanitize and validate file path.
        
        Args:
            file_path: Path to validate
            must_exist: Whether file must exist
            
        Returns:
            Sanitized absolute path
            
        Raises:
            SecurityError: If path is unsafe
        """
        if not file_path:
            raise SecurityError("File path cannot be empty")
        
        try:
            path = Path(file_path).resolve()
        except (OSError, ValueError) as e:
            raise SecurityError(f"Invalid path format: {file_path}") from e
        
        # Ensure path is within project directory
        if not str(path).startswith(str(self.project_root)):
            raise SecurityError(f"File path outside project directory: {file_path}")
        
        # Check if file exists when required
        if must_exist and not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        # Security checks
        if path.is_symlink():
            raise SecurityError(f"Symbolic links not allowed: {path}")
        
        return str(path)


class InputValidator:
    """Comprehensive input validation for financial data"""
    
    # Patterns to sanitize from logs
    SENSITIVE_PATTERNS = [
        (r'password["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'password=***'),
        (r'token["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'token=***'),
        (r'key["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'key=***'),
        (r'secret["\']?\s*[:=]\s*["\']?([^"\'\\s]+)', r'secret=***'),
        (r'(\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4})', r'****-****-****-****'),  # Credit cards
        (r'(\d{3}-\d{2}-\d{4})', r'***-**-****'),  # SSN
        (r'(\d{9})', r'*********'),  # Account numbers
    ]
    
    @staticmethod
    def validate_numeric_range(value: float, min_val: Optional[float] = None, max_val: Optional[float] = None, 
                              field_name: str = "value") -> float:
        """
        Validate numeric value within specified range.
        
        Args:
            value: Value to validate
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            field_name: Field name for error messages
            
        Returns:
            Validated value
            
        Raises:
            ValidationError: If value is out of range
        """
        if not isinstance(value, (int, float)):
            raise ValidationError(f"{field_name} must be numeric, got {type(value).__name__}")
        
        if min_val is not None and value < min_val:
            raise ValidationError(f"{field_name} must be >= {min_val}, got {value}")
        
        if max_val is not None and value > max_val:
            raise ValidationError(f"{field_name} must be <= {max_val}, got {value}")
        
        return float(value)
    
    @staticmethod
    def validate_string_length(value: str, min_len: int = 0, max_len: int = 1000,
                              field_name: str = "value") -> str:
        """
        Validate string length.
        
        Args:
            value: String to validate
            min_len: Minimum length
            max_len: Maximum length
            field_name: Field name for error messages
            
        Returns:
            Validated string
            
        Raises:
            ValidationError: If string length is invalid
        """
        if not isinstance(value, str):
            raise ValidationError(f"{field_name} must be string, got {type(value).__name__}")
        
        if len(value) < min_len:
            raise ValidationError(f"{field_name} must be at least {min_len} characters")
        
        if len(value) > max_len:
            raise ValidationError(f"{field_name} must be at most {max_len} characters")
        
        return value
    
    @staticmethod
    def sanitize_sql_input(value: str) -> str:
        """
        Sanitize input to prevent SQL injection.
        
        Args:
            value: String to sanitize
            
        Returns:
            Sanitized string
        """
        if not isinstance(value, str):
            return str(value)
        
        # Remove dangerous SQL keywords and characters
        dangerous_patterns = [
            r'(union|select|insert|update|delete|drop|create|alter|exec|execute)',
            r'[;\'\"\\]',
            r'--',
            r'/\*.*\*/',
        ]
        
        sanitized = value
        for pattern in dangerous_patterns:
            sanitized = re.sub(pattern, '', sanitized, flags=re.IGNORECASE)
        
        return sanitized.strip()
    
    @staticmethod
    def sanitize_log_message(message: str) -> str:
        """
        Sanitize log message to remove sensitive information.
        
        Args:
            message: Log message to sanitize
            
        Returns:
            Sanitized message
        """
        sanitized = message
        for pattern, replacement in InputValidator.SENSITIVE_PATTERNS:
            sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
        
        return sanitized
    
    @staticmethod
    def validate_financial_amount(amount: float, currency: str = "USD") -> float:
        """
        Validate financial amount with currency-specific rules.
        
        Args:
            amount: Amount to validate
            currency: Currency code
            
        Returns:
            Validated amount
            
        Raises:
            ValidationError: If amount is invalid
        """
        if not isinstance(amount, (int, float)):
            raise ValidationError(f"Amount must be numeric, got {type(amount).__name__}")
        
        # Check for reasonable financial limits
        if abs(amount) > 1e12:  # 1 trillion limit
            raise ValidationError(f"Amount too large: {amount}")
        
        # Check for precision (max 4 decimal places for most currencies)
        if currency in ["USD", "EUR", "GBP", "CAD"]:
            if round(amount, 4) != amount:
                raise ValidationError(f"Amount has too many decimal places: {amount}")
        
        return amount


class AuditLogger:
    """Comprehensive audit logging for financial operations"""
    
    def __init__(self, audit_file: str):
        self.audit_file = Path(audit_file)
        self.audit_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Setup secure audit logging
        self.audit_logger = logging.getLogger("audit")
        self.audit_logger.setLevel(logging.INFO)
        
        # Create file handler for audit log
        handler = logging.FileHandler(self.audit_file, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        self.audit_logger.addHandler(handler)
    
    def log_financial_operation(self, operation: str, user_id: str, 
                               data: Dict[str, Any], result: str,
                               ip_address: Optional[str] = None, session_id: Optional[str] = None):
        """
        Log financial operation with complete audit trail.
        
        Args:
            operation: Type of operation
            user_id: User performing operation
            data: Operation data (will be hashed)
            result: Operation result
            ip_address: Client IP address
            session_id: Session identifier
        """
        audit_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'operation': operation,
            'user_id': user_id,
            'data_hash': self._hash_data(data),
            'result': result,
            'ip_address': ip_address or 'unknown',
            'session_id': session_id or 'unknown'
        }
        
        self.audit_logger.info(f"AUDIT: {audit_entry}")
    
    def _hash_data(self, data: Dict[str, Any]) -> str:
        """
        Create hash of sensitive data for audit trail.
        
        Args:
            data: Data to hash
            
        Returns:
            SHA-256 hash of data
        """
        import json
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()


def secure_error_handler(func):
    """
    Decorator for secure error handling that prevents information disclosure.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function with secure error handling
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SecureException as e:
            # Log security errors with appropriate severity
            logger = logging.getLogger("security")
            logger.error(f"Security error in {func.__name__}: {e.message}")
            
            if e.severity == ErrorSeverity.CRITICAL:
                # Log to security audit log
                security_logger = logging.getLogger("security.audit")
                security_logger.critical(f"SECURITY BREACH: {e.message}")
            
            raise
        except Exception as e:
            # Log unexpected errors without exposing internal details
            logger = logging.getLogger("security")
            logger.error(f"Unexpected error in {func.__name__}: {type(e).__name__}")
            
            # Don't expose internal details to caller
            raise SecureException(f"Internal error in {func.__name__}", ErrorSeverity.HIGH)
    
    return wrapper


class DataEncryption:
    """Encrypt sensitive financial data using Fernet encryption"""
    
    def __init__(self, key: Optional[bytes] = None):
        """
        Initialize encryption with key from environment or provided key.
        
        Args:
            key: Optional encryption key (will use environment if not provided)
        """
        if key:
            self.key = key
        else:
            self.key = self._get_encryption_key()
        
        # Import Fernet here to avoid dependency issues if not installed
        try:
            from cryptography.fernet import Fernet  # type: ignore
            self.cipher = Fernet(self.key)
        except ImportError:
            raise ImportError("cryptography package required for encryption. Install with: pip install cryptography")
    
    def _get_encryption_key(self) -> bytes:
        """
        Get encryption key from environment or generate new one.
        
        Returns:
            Encryption key
            
        Raises:
            SecurityError: If no key available and cannot generate
        """
        key_b64 = os.getenv('ENCRYPTION_KEY')
        if key_b64:
            try:
                return base64.b64decode(key_b64)
            except Exception:
                raise SecurityError("Invalid ENCRYPTION_KEY in environment")
        else:
            # Generate new key (store securely!)
            from cryptography.fernet import Fernet  # type: ignore
            key = Fernet.generate_key()
            logging.warning(f"Generated new encryption key: {base64.b64encode(key).decode()}")
            logging.warning("Store this key securely in ENCRYPTION_KEY environment variable")
            return key
    
    def encrypt_string(self, plaintext: str) -> str:
        """
        Encrypt a string value.
        
        Args:
            plaintext: String to encrypt
            
        Returns:
            Encrypted string (base64 encoded)
        """
        if not isinstance(plaintext, str):
            plaintext = str(plaintext)
        
        encrypted = self.cipher.encrypt(plaintext.encode())
        return encrypted.decode()
    
    def decrypt_string(self, ciphertext: str) -> str:
        """
        Decrypt a string value.
        
        Args:
            ciphertext: Encrypted string to decrypt
            
        Returns:
            Decrypted string
        """
        decrypted = self.cipher.decrypt(ciphertext.encode())
        return decrypted.decode()
    
    def encrypt_dataframe_columns(self, df, sensitive_columns: List[str]):
        """
        Encrypt sensitive columns in a DataFrame.
        
        Args:
            df: DataFrame to encrypt
            sensitive_columns: List of columns to encrypt
            
        Returns:
            DataFrame with encrypted columns
        """
        import pandas as pd  # type: ignore
        
        df_encrypted = df.copy()
        
        for col in sensitive_columns:
            if col in df_encrypted.columns:
                df_encrypted[col] = df_encrypted[col].apply(
                    lambda x: self.encrypt_string(str(x)) if pd.notna(x) else x
                )
        
        return df_encrypted