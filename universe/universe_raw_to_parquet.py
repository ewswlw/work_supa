import sys
from pathlib import Path
import yaml

# This allows the script to import modules from the 'src' directory
sys.path.append(str(Path(__file__).parent.parent))

from src.pipeline.universe_processor import process_universe_files
from src.utils.logging import LogManager

if __name__ == "__main__":
    """
    This script acts as a simple runner for the universe processing pipeline.
    The core logic is located in the `src/pipeline/universe_processor.py` module.
    """
    # Load logging configuration
    config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    log_config = config.get('logging', {})
    log_file = Path(__file__).parent.parent / 'runs' / 'logs' / 'universe_processor.log'
    
    # Setup logger
    logger = LogManager(
        log_file=str(log_file),
        log_level=log_config.get('level', 'INFO'),
        log_format=log_config.get('format')
    )
    
    logger.info("Starting universe processing pipeline...")
    try:
        process_universe_files(logger)
        logger.info("Universe processing pipeline finished successfully.")
    except Exception as e:
        logger.error("Universe processing pipeline failed.", exc=e) 