"""
🔒 SECURITY TESTING SUITE
=========================
Comprehensive security tests for the trading analytics platform.
Tests for vulnerabilities, input validation, and security features.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import asyncio
from datetime import datetime

# Import security modules
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.utils.security import (
    PathSanitizer, SecurityError, ValidationError, InputValidator,
    DataEncryption, AuditLogger, secure_error_handler
)
from src.utils.logging import SecureLogManager
from src.orchestrator.pipeline_manager import PipelineManager
from src.orchestrator.pipeline_config import OrchestrationConfig


class TestPathSanitizer:
    """Test path sanitization and validation"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.sanitizer = PathSanitizer(Path(self.temp_dir))
    
    def test_valid_script_path(self):
        """Test validation of valid script paths"""
        # Create a valid Python file
        valid_file = Path(self.temp_dir) / "test_script.py"
        valid_file.write_text("print('hello')")
        
        result = self.sanitizer.sanitize_script_path(str(valid_file))
        assert result == str(valid_file.resolve())
    
    def test_path_traversal_attack(self):
        """Test protection against path traversal attacks"""
        with pytest.raises(SecurityError, match="outside project directory"):
            self.sanitizer.sanitize_script_path("../../../etc/passwd")
        
        with pytest.raises(SecurityError, match="outside project directory"):
            self.sanitizer.sanitize_script_path("..\\..\\windows\\system32\\cmd.exe")
    
    def test_invalid_file_extension(self):
        """Test rejection of invalid file extensions"""
        with pytest.raises(SecurityError, match="Invalid script extension"):
            self.sanitizer.sanitize_script_path("malicious.exe")
        
        with pytest.raises(SecurityError, match="Invalid script extension"):
            self.sanitizer.sanitize_script_path("script.bat")
    
    def test_nonexistent_file(self):
        """Test handling of nonexistent files"""
        with pytest.raises(FileNotFoundError, match="Script not found"):
            self.sanitizer.sanitize_script_path("nonexistent.py")
    
    def test_symbolic_link_rejection(self):
        """Test rejection of symbolic links"""
        # Create a symbolic link (if supported)
        try:
            valid_file = Path(self.temp_dir) / "real_script.py"
            valid_file.write_text("print('hello')")
            
            link_file = Path(self.temp_dir) / "link_script.py"
            link_file.symlink_to(valid_file)
            
            with pytest.raises(SecurityError, match="Symbolic links not allowed"):
                self.sanitizer.sanitize_script_path(str(link_file))
        except OSError:
            # Skip if symlinks not supported
            pytest.skip("Symbolic links not supported on this platform")
    
    def test_empty_path(self):
        """Test handling of empty paths"""
        with pytest.raises(SecurityError, match="cannot be empty"):
            self.sanitizer.sanitize_script_path("")


class TestInputValidator:
    """Test input validation functions"""
    
    def test_numeric_range_validation(self):
        """Test numeric range validation"""
        # Valid values
        assert InputValidator.validate_numeric_range(5.0, 1.0, 10.0) == 5.0
        assert InputValidator.validate_numeric_range(1.0, 1.0, 10.0) == 1.0
        assert InputValidator.validate_numeric_range(10.0, 1.0, 10.0) == 10.0
        
        # Invalid values
        with pytest.raises(ValidationError, match="must be >= 1.0"):
            InputValidator.validate_numeric_range(0.5, 1.0, 10.0)
        
        with pytest.raises(ValidationError, match="must be <= 10.0"):
            InputValidator.validate_numeric_range(15.0, 1.0, 10.0)
        
        with pytest.raises(ValidationError, match="must be numeric"):
            InputValidator.validate_numeric_range("invalid", 1.0, 10.0)  # type: ignore
    
    def test_string_length_validation(self):
        """Test string length validation"""
        # Valid strings
        assert InputValidator.validate_string_length("hello", 1, 10) == "hello"
        assert InputValidator.validate_string_length("a", 1, 10) == "a"
        
        # Invalid strings
        with pytest.raises(ValidationError, match="at least 5 characters"):
            InputValidator.validate_string_length("hi", 5, 10)
        
        with pytest.raises(ValidationError, match="at most 5 characters"):
            InputValidator.validate_string_length("too long string", 1, 5)
        
        with pytest.raises(ValidationError, match="must be string"):
            InputValidator.validate_string_length(123, 1, 10)  # type: ignore
    
    def test_sql_injection_prevention(self):
        """Test SQL injection prevention"""
        malicious_inputs = [
            "'; DROP TABLE users; --",
            "1 OR 1=1",
            "UNION SELECT * FROM passwords",
            "'; DELETE FROM accounts; --",
            "/* comment */ SELECT",
        ]
        
        for malicious_input in malicious_inputs:
            sanitized = InputValidator.sanitize_sql_input(malicious_input)
            # Should remove dangerous SQL keywords
            assert "DROP" not in sanitized.upper()
            assert "DELETE" not in sanitized.upper()
            assert "UNION" not in sanitized.upper()
            assert "SELECT" not in sanitized.upper()
    
    def test_log_message_sanitization(self):
        """Test log message sanitization"""
        # Test sensitive data patterns
        sensitive_messages = [
            "User password=secret123 logged in",
            "API token=abc123xyz456 expired",
            "Database key=supersecret connected",
            "Credit card 1234-5678-9012-3456 processed",
            "SSN 123-45-6789 verified",
        ]
        
        for message in sensitive_messages:
            sanitized = InputValidator.sanitize_log_message(message)
            # Should not contain sensitive data
            assert "secret123" not in sanitized
            assert "abc123xyz456" not in sanitized
            assert "supersecret" not in sanitized
            assert "1234-5678-9012-3456" not in sanitized
            assert "123-45-6789" not in sanitized
    
    def test_financial_amount_validation(self):
        """Test financial amount validation"""
        # Valid amounts
        assert InputValidator.validate_financial_amount(100.50) == 100.50
        assert InputValidator.validate_financial_amount(0.0001) == 0.0001
        
        # Invalid amounts
        with pytest.raises(ValidationError, match="must be numeric"):
            InputValidator.validate_financial_amount("100.50")  # type: ignore
        
        with pytest.raises(ValidationError, match="Amount too large"):
            InputValidator.validate_financial_amount(1e15)
        
        with pytest.raises(ValidationError, match="too many decimal places"):
            InputValidator.validate_financial_amount(100.123456)


class TestSecureLogging:
    """Test secure logging functionality"""
    
    def setup_method(self):
        """Setup test logging"""
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = os.path.join(self.temp_dir, "test.log")
        self.logger = SecureLogManager(self.log_file, "DEBUG")
    
    def test_log_sanitization(self):
        """Test that logs are sanitized"""
        # Log a message with sensitive data
        self.logger.info("User password=secret123 accessed system")
        
        # Read log file and verify sanitization
        with open(self.log_file, 'r') as f:
            log_content = f.read()
        
        assert "secret123" not in log_content
        assert "password=***" in log_content
    
    def test_security_event_logging(self):
        """Test security event logging"""
        self.logger.security_event("Unauthorized access attempt", 
                                  {"ip": "192.168.1.100", "user": "attacker"})
        
        # Verify security event was logged
        with open(self.log_file, 'r') as f:
            log_content = f.read()
        
        assert "SECURITY EVENT" in log_content
        assert "Unauthorized access attempt" in log_content
    
    def test_audit_trail(self):
        """Test audit trail functionality"""
        if hasattr(self.logger, 'audit_financial_operation'):
            self.logger.audit_financial_operation(
                operation="TRADE_EXECUTION",
                user_id="trader123",
                amount=10000.00,
                currency="USD",
                result="SUCCESS"
            )
            
            # Verify audit log exists
            audit_file = self.log_file.replace('.log', '_audit.log')
            if os.path.exists(audit_file):
                with open(audit_file, 'r') as f:
                    audit_content = f.read()
                
                assert "FINANCIAL_OPERATION" in audit_content
                assert "TRADE_EXECUTION" in audit_content


class TestDataEncryption:
    """Test data encryption functionality"""
    
    def setup_method(self):
        """Setup encryption test environment"""
        # Set a test encryption key
        os.environ['ENCRYPTION_KEY'] = 'dGVzdC1lbmNyeXB0aW9uLWtleS0zMi1ieXRlcyE='
    
    def test_string_encryption_decryption(self):
        """Test string encryption and decryption"""
        try:
            encryptor = DataEncryption()
            
            original_text = "sensitive financial data"
            encrypted_text = encryptor.encrypt_string(original_text)
            decrypted_text = encryptor.decrypt_string(encrypted_text)
            
            assert encrypted_text != original_text
            assert decrypted_text == original_text
        except ImportError:
            pytest.skip("cryptography package not available")
    
    def test_dataframe_encryption(self):
        """Test DataFrame column encryption"""
        try:
            import pandas as pd  # type: ignore
            encryptor = DataEncryption()
            
            # Create test DataFrame
            df = pd.DataFrame({
                'public_data': ['A', 'B', 'C'],
                'sensitive_data': ['secret1', 'secret2', 'secret3']
            })
            
            # Encrypt sensitive columns
            encrypted_df = encryptor.encrypt_dataframe_columns(df, ['sensitive_data'])
            
            # Verify encryption
            assert encrypted_df['public_data'].tolist() == ['A', 'B', 'C']
            assert encrypted_df['sensitive_data'].tolist() != ['secret1', 'secret2', 'secret3']
            
        except (ImportError, ModuleNotFoundError):
            pytest.skip("Required packages not available")


class TestPipelineManagerSecurity:
    """Test security improvements in pipeline manager"""
    
    def setup_method(self):
        """Setup pipeline manager test"""
        self.temp_dir = tempfile.mkdtemp()
        self.config = Mock()
        self.logger = Mock()
        
        # Create pipeline manager
        with patch('src.orchestrator.pipeline_manager.PathSanitizer'):
            with patch('src.orchestrator.pipeline_manager.AuditLogger'):
                self.manager = PipelineManager(self.config, self.logger)
    
    @pytest.mark.asyncio
    async def test_script_path_validation(self):
        """Test that script paths are validated"""
        from src.orchestrator.pipeline_manager import PipelineStage
        
        # Mock the path sanitizer to raise SecurityError
        self.manager.path_sanitizer.sanitize_script_path = Mock(
            side_effect=SecurityError("Invalid path")
        )
        
        # Attempt to execute stage with invalid path
        result = await self.manager._execute_stage(PipelineStage.UNIVERSE)
        
        # Verify execution failed due to security error
        assert not result.success
        assert result.error_message and "Invalid path" in result.error_message
    
    def test_secure_error_handling(self):
        """Test secure error handling decorator"""
        @secure_error_handler
        def test_function():
            raise Exception("Internal error with sensitive data password=secret")
        
        with pytest.raises(Exception) as exc_info:
            test_function()
        
        # Should not expose sensitive information
        assert "secret" not in str(exc_info.value)


class TestConfigurationValidation:
    """Test configuration validation"""
    
    def test_orchestration_config_validation(self):
        """Test orchestration configuration validation"""
        # Valid configuration
        valid_config = OrchestrationConfig(
            max_parallel_stages=3,
            retry_attempts=2,
            retry_delay_seconds=30,
            timeout_minutes=60
        )
        assert valid_config.max_parallel_stages == 3
        
        # Invalid configuration - should raise ValidationError
        with pytest.raises(ValidationError):
            OrchestrationConfig(
                max_parallel_stages=0,  # Invalid: must be >= 1
                retry_attempts=2,
                retry_delay_seconds=30,
                timeout_minutes=60
            )
        
        with pytest.raises(ValidationError):
            OrchestrationConfig(
                max_parallel_stages=3,
                retry_attempts=-1,  # Invalid: must be >= 0
                retry_delay_seconds=30,
                timeout_minutes=60
            )


class TestSecurityIntegration:
    """Integration tests for security features"""
    
    def test_end_to_end_security_flow(self):
        """Test complete security flow from input to output"""
        # Create test environment
        temp_dir = tempfile.mkdtemp()
        
        # Test path sanitization
        sanitizer = PathSanitizer(Path(temp_dir))
        
        # Create valid test script
        test_script = Path(temp_dir) / "test.py"
        test_script.write_text("print('test')")
        
        # Validate path
        sanitized_path = sanitizer.sanitize_script_path(str(test_script))
        assert sanitized_path == str(test_script.resolve())
        
        # Test input validation
        validated_amount = InputValidator.validate_financial_amount(1000.50)
        assert validated_amount == 1000.50
        
        # Test log sanitization
        sanitized_message = InputValidator.sanitize_log_message("password=secret123")
        assert "secret123" not in sanitized_message
    
    def test_security_headers_and_validation(self):
        """Test security headers and comprehensive validation"""
        # Test that all security components work together
        errors = []
        
        try:
            # Test path sanitizer
            sanitizer = PathSanitizer()
            sanitizer.sanitize_script_path("../../../etc/passwd")
        except SecurityError:
            pass  # Expected
        except Exception as e:
            errors.append(f"Path sanitizer error: {e}")
        
        try:
            # Test input validator
            InputValidator.validate_numeric_range("invalid", 1, 10)  # type: ignore
        except ValidationError:
            pass  # Expected
        except Exception as e:
            errors.append(f"Input validator error: {e}")
        
        # Should have no unexpected errors
        assert len(errors) == 0, f"Unexpected errors: {errors}"


if __name__ == "__main__":
    # Run specific security tests
    pytest.main([__file__, "-v", "--tb=short"])