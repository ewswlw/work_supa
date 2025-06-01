"""
Configuration management for the pipeline.
"""
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import yaml
from dotenv import load_dotenv


@dataclass
class PipelineConfig:
    """Pipeline configuration settings"""
    input_dir: str
    file_pattern: str
    output_parquet: str
    last_processed_file: str
    date_format: str
    time_format: str
    parallel_load: bool
    n_workers: int
    show_rows: int
    log_file: str
    chunk_size: int = 10000


@dataclass
class SupabaseConfig:
    """Supabase configuration settings"""
    url: str
    key: str
    table: str
    batch_size: int = 1000


@dataclass
class LoggingConfig:
    """Logging configuration settings"""
    level: str = "INFO"
    format: str = "[%(asctime)s] %(levelname)s: %(message)s"


class ConfigManager:
    """Manages configuration loading and validation"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        load_dotenv()
        self.config_path = config_path
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        # Get project root directory
        project_root = Path(__file__).parent.parent.parent
        
        # Pipeline config
        pipeline_config = config_dict['pipeline']
        self.pipeline_config = PipelineConfig(
            input_dir=str(project_root / pipeline_config['input_dir']),
            file_pattern=pipeline_config['file_pattern'],
            output_parquet=str(project_root / pipeline_config['output_parquet']),
            last_processed_file=str(project_root / pipeline_config['last_processed_file']),
            date_format=pipeline_config['date_format'],
            time_format=pipeline_config['time_format'],
            parallel_load=pipeline_config['parallel_load'],
            n_workers=pipeline_config['n_workers'],
            show_rows=pipeline_config['show_rows'],
            log_file=str(project_root / pipeline_config['log_file']),
            chunk_size=pipeline_config.get('chunk_size', 10000)
        )
        
        # Supabase config
        supabase_config = config_dict['supabase']
        self.supabase_config = SupabaseConfig(
            url=os.getenv("SUPABASE_URL"),
            key=os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
            table=os.getenv("SUPABASE_TABLE", "runs"),
            batch_size=supabase_config.get('batch_size', 1000)
        )
        
        # Logging config
        logging_config = config_dict.get('logging', {})
        self.logging_config = LoggingConfig(
            level=logging_config.get('level', 'INFO'),
            format=logging_config.get('format', '[%(asctime)s] %(levelname)s: %(message)s')
        )
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self):
        """Validate configuration settings"""
        # Validate Supabase credentials
        if not self.supabase_config.url:
            raise ValueError("SUPABASE_URL environment variable is required")
        if not self.supabase_config.key:
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY environment variable is required")
        
        # Validate input directory exists
        if not os.path.exists(self.pipeline_config.input_dir):
            raise ValueError(f"Input directory does not exist: {self.pipeline_config.input_dir}")
        
        # Create output directories if they don't exist
        os.makedirs(os.path.dirname(self.pipeline_config.output_parquet), exist_ok=True)
        os.makedirs(os.path.dirname(self.pipeline_config.log_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.pipeline_config.last_processed_file), exist_ok=True) 