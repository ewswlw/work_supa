"""
🔧 SECURE PIPELINE CONFIGURATION
=================================
Centralized configuration management for pipeline orchestration with
comprehensive input validation and security features.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
import yaml
import os
from datetime import datetime

try:
    from pydantic import BaseModel, validator, Field  # type: ignore
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    # Fallback to basic validation
    class BaseModel:
        pass

from ..utils.security import InputValidator, ValidationError, PathSanitizer


# Define schema classes based on whether Pydantic is available
if HAS_PYDANTIC:
    class OrchestrationConfigSchema(BaseModel):
        """Pydantic schema for orchestration configuration with validation"""
        max_parallel_stages: int = Field(default=3, ge=1, le=10, description="Maximum parallel stages")
        retry_attempts: int = Field(default=2, ge=0, le=10, description="Number of retry attempts")
        retry_delay_seconds: int = Field(default=30, ge=1, le=300, description="Delay between retries")
        timeout_minutes: int = Field(default=60, ge=1, le=1440, description="Operation timeout")
        enable_monitoring: bool = Field(default=True, description="Enable monitoring")
        notification_endpoints: List[str] = Field(default_factory=list, description="Notification endpoints")
        checkpoint_interval: int = Field(default=5, ge=1, le=60, description="Checkpoint interval")
        fail_fast: bool = Field(default=False, description="Fail fast on errors")
        continue_on_warnings: bool = Field(default=True, description="Continue on warnings")
        save_partial_results: bool = Field(default=True, description="Save partial results")
        
        @validator('notification_endpoints')
        def validate_notification_endpoints(cls, v):
            """Validate notification endpoints"""
            valid_endpoints = ['console', 'email', 'slack', 'webhook']
            for endpoint in v:
                if endpoint not in valid_endpoints:
                    raise ValueError(f"Invalid notification endpoint: {endpoint}")
            return v
        
        @validator('max_parallel_stages')
        def validate_max_parallel_stages(cls, v):
            """Validate max parallel stages"""
            if v < 1 or v > 10:
                raise ValueError("max_parallel_stages must be between 1 and 10")
            return v
        
        @validator('timeout_minutes')
        def validate_timeout_minutes(cls, v):
            """Validate timeout minutes"""
            if v < 1 or v > 1440:  # Max 24 hours
                raise ValueError("timeout_minutes must be between 1 and 1440")
            return v
        
        class Config:
            """Pydantic configuration"""
            validate_assignment = True
            extra = 'forbid'  # Forbid extra fields
            
    class SecurityConfigSchema(BaseModel):
        """Security configuration schema"""
        enable_encryption: bool = Field(default=True, description="Enable data encryption")
        encryption_key: Optional[str] = Field(default=None, description="Encryption key (use environment variable)")
        enable_audit_logging: bool = Field(default=True, description="Enable audit logging")
        audit_log_retention_days: int = Field(default=365, ge=30, le=2555, description="Audit log retention")
        max_log_file_size_mb: int = Field(default=10, ge=1, le=100, description="Max log file size")
        log_backup_count: int = Field(default=5, ge=1, le=20, description="Number of log backups")
        
        @validator('encryption_key')
        def validate_encryption_key(cls, v):
            """Validate encryption key format"""
            if v is not None and len(v) < 32:
                raise ValueError("Encryption key must be at least 32 characters")
            return v
        
        class Config:
            """Pydantic configuration"""
            validate_assignment = True
            extra = 'forbid'


@dataclass
class OrchestrationConfig:
    """Pipeline orchestration configuration (legacy compatibility)"""
    max_parallel_stages: int = 3
    retry_attempts: int = 2
    retry_delay_seconds: int = 30
    timeout_minutes: int = 60
    enable_monitoring: bool = True
    notification_endpoints: List[str] = field(default_factory=list)
    checkpoint_interval: int = 5  # minutes
    fail_fast: bool = False
    continue_on_warnings: bool = True
    save_partial_results: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        self._validate_config()
    
    def _validate_config(self):
        """Validate configuration values"""
        errors = []
        
        # Validate max_parallel_stages
        if not isinstance(self.max_parallel_stages, int) or self.max_parallel_stages < 1 or self.max_parallel_stages > 10:
            errors.append("max_parallel_stages must be integer between 1 and 10")
        
        # Validate retry_attempts
        if not isinstance(self.retry_attempts, int) or self.retry_attempts < 0:
            errors.append("retry_attempts must be non-negative integer")
        
        # Validate retry_delay_seconds
        if not isinstance(self.retry_delay_seconds, int) or self.retry_delay_seconds < 1:
            errors.append("retry_delay_seconds must be positive integer")
        
        # Validate timeout_minutes
        if not isinstance(self.timeout_minutes, int) or self.timeout_minutes < 1 or self.timeout_minutes > 1440:
            errors.append("timeout_minutes must be integer between 1 and 1440")
        
        # Validate checkpoint_interval
        if not isinstance(self.checkpoint_interval, int) or self.checkpoint_interval < 1:
            errors.append("checkpoint_interval must be positive integer")
        
        if errors:
            raise ValidationError(f"Configuration validation failed: {'; '.join(errors)}")


@dataclass
class SecurityConfig:
    """Security configuration"""
    enable_encryption: bool = True
    encryption_key: Optional[str] = None
    enable_audit_logging: bool = True
    audit_log_retention_days: int = 365
    max_log_file_size_mb: int = 10
    log_backup_count: int = 5
    
    def __post_init__(self):
        """Validate security configuration"""
        self._validate_config()
    
    def _validate_config(self):
        """Validate security configuration values"""
        errors = []
        
        if self.encryption_key is not None and len(self.encryption_key) < 32:
            errors.append("encryption_key must be at least 32 characters")
        
        if self.audit_log_retention_days < 30:
            errors.append("audit_log_retention_days must be at least 30")
        
        if self.max_log_file_size_mb < 1:
            errors.append("max_log_file_size_mb must be at least 1")
        
        if self.log_backup_count < 1:
            errors.append("log_backup_count must be at least 1")
        
        if errors:
            raise ValidationError(f"Security configuration validation failed: {'; '.join(errors)}")


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    orchestration: OrchestrationConfig
    universe: Dict[str, Any]
    portfolio: Dict[str, Any]
    g_spread: Dict[str, Any]
    runs: Dict[str, Any]
    logging: Dict[str, Any]
    
    @classmethod
    def load_from_file(cls, config_path: str) -> 'PipelineConfig':
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        # Add orchestration defaults if not present
        orchestration_config = config_dict.get('orchestration', {})
        orchestration = OrchestrationConfig(**orchestration_config)
        
        return cls(
            orchestration=orchestration,
            universe=config_dict.get('universe_processor', {}),
            portfolio=config_dict.get('portfolio_processor', {}),
            g_spread=config_dict.get('g_spread_processor', {}),
            runs=config_dict.get('pipeline', {}),  # Legacy runs config
            logging=config_dict.get('logging', {})
        )
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of issues."""
        issues = []
        
        # Check required directories exist
        required_dirs = [
            "universe/raw data",
            "portfolio/raw data", 
            "historical g spread/raw data",
            "runs/raw",
            "logs"
        ]
        
        for dir_path in required_dirs:
            if not Path(dir_path).exists():
                issues.append(f"Required directory missing: {dir_path}")
        
        # Validate orchestration settings
        if self.orchestration.max_parallel_stages < 1:
            issues.append("max_parallel_stages must be >= 1")
        
        if self.orchestration.retry_attempts < 0:
            issues.append("retry_attempts must be >= 0")
        
        if self.orchestration.timeout_minutes <= 0:
            issues.append("timeout_minutes must be > 0")
        
        return issues 