"""
🔧 PIPELINE CONFIGURATION
=========================
Centralized configuration management for pipeline orchestration.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
import yaml


@dataclass
class OrchestrationConfig:
    """Pipeline orchestration configuration."""
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