"""
Tests for pipeline configuration management.
"""

import pytest
import tempfile
import yaml
from pathlib import Path
import sys
from unittest.mock import patch

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.orchestrator.pipeline_config import PipelineConfig, OrchestrationConfig


class TestOrchestrationConfig:
    """Test OrchestrationConfig class."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = OrchestrationConfig()
        
        assert config.max_parallel_stages == 3
        assert config.retry_attempts == 2
        assert config.retry_delay_seconds == 30
        assert config.timeout_minutes == 60
        assert config.enable_monitoring is True
        assert config.notification_endpoints == []
        assert config.checkpoint_interval == 5
        assert config.fail_fast is False
        assert config.continue_on_warnings is True
        assert config.save_partial_results is True
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = OrchestrationConfig(
            max_parallel_stages=5,
            retry_attempts=3,
            notification_endpoints=["console", "email"]
        )
        
        assert config.max_parallel_stages == 5
        assert config.retry_attempts == 3
        assert config.notification_endpoints == ["console", "email"]


class TestPipelineConfig:
    """Test PipelineConfig class."""
    
    def test_load_from_valid_file(self):
        """Test loading configuration from valid YAML file."""
        config_data = {
            'orchestration': {
                'max_parallel_stages': 4,
                'enable_monitoring': False
            },
            'universe_processor': {
                'columns_to_keep': ['Date', 'CUSIP']
            },
            'portfolio_processor': {
                'columns_to_drop': ['Unnamed: 0']
            },
            'g_spread_processor': {
                'input_file': 'test.csv'
            },
            'pipeline': {
                'input_dir': 'test_dir'
            },
            'logging': {
                'level': 'DEBUG'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        try:
            config = PipelineConfig.load_from_file(temp_path)
            
            assert config.orchestration.max_parallel_stages == 4
            assert config.orchestration.enable_monitoring is False
            assert config.universe == {'columns_to_keep': ['Date', 'CUSIP']}
            assert config.portfolio == {'columns_to_drop': ['Unnamed: 0']}
            assert config.g_spread == {'input_file': 'test.csv'}
            assert config.runs == {'input_dir': 'test_dir'}
            assert config.logging == {'level': 'DEBUG'}
        finally:
            Path(temp_path).unlink()
    
    def test_load_from_nonexistent_file(self):
        """Test loading configuration from nonexistent file."""
        with pytest.raises(FileNotFoundError):
            PipelineConfig.load_from_file("nonexistent.yaml")
    
    def test_load_with_missing_sections(self):
        """Test loading configuration with missing sections."""
        config_data = {
            'logging': {
                'level': 'INFO'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name
        
        try:
            config = PipelineConfig.load_from_file(temp_path)
            
            # Should use defaults for missing sections
            assert config.orchestration.max_parallel_stages == 3
            assert config.universe == {}
            assert config.portfolio == {}
            assert config.g_spread == {}
            assert config.runs == {}
            assert config.logging == {'level': 'INFO'}
        finally:
            Path(temp_path).unlink()
    
    def test_validate_with_missing_directories(self):
        """Test validation with missing required directories."""
        config = PipelineConfig(
            orchestration=OrchestrationConfig(),
            universe={},
            portfolio={},
            g_spread={},
            runs={},
            logging={}
        )
        
        # Mock Path.exists to return False for all directories
        with patch('pathlib.Path.exists', return_value=False):
            issues = config.validate()
        
        # Should find missing directories
        expected_dirs = [
            "universe/raw data",
            "portfolio/raw data", 
            "historical g spread/raw data",
            "runs/raw",
            "logs"
        ]
        
        for dir_path in expected_dirs:
            assert any(dir_path in issue for issue in issues)
    
    def test_validate_with_invalid_orchestration_settings(self):
        """Test validation with invalid orchestration settings."""
        config = PipelineConfig(
            orchestration=OrchestrationConfig(
                max_parallel_stages=0,
                retry_attempts=-1,
                timeout_minutes=0
            ),
            universe={},
            portfolio={},
            g_spread={},
            runs={},
            logging={}
        )
        
        issues = config.validate()
        
        assert any("max_parallel_stages must be >= 1" in issue for issue in issues)
        assert any("retry_attempts must be >= 0" in issue for issue in issues)
        assert any("timeout_minutes must be > 0" in issue for issue in issues) 