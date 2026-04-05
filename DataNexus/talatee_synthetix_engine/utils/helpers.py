import json
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path
from datetime import datetime

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("utils_helpers")

def load_json_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Safely loads a JSON configuration file from a given path.
    
    :param config_path: Path to the JSON file.
    :return: Dictionary of config data or empty dict if failure.
    """
    path = Path(config_path)
    if not path.exists():
        logger.error(f"Configuration file not found at: {path}")
        return {}
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.debug(f"Successfully loaded config from {path.name}")
            return data
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in {path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error loading config {path}: {e}")
        return {}

def ensure_dir(directory_path: Union[str, Path]) -> Path:
    """
    Ensures a directory exists, creating it if necessary.
    
    :param directory_path: Path to the directory.
    :return: Path object of the directory.
    """
    path = Path(directory_path)
    try:
        path.mkdir(parents=True, exist_ok=True)
        return path
    except Exception as e:
        logger.error(f"Could not create directory {path}: {e}")
        raise

def get_current_timestamp(format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Returns a formatted string of the current system time.
    """
    return datetime.now().strftime(format_str)

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Performs division with zero-check handling for analytical metrics.
    """
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except Exception:
        return default

def validate_client_config(config: Dict[str, Any]) -> bool:
    """
    Validates that a client configuration contains the minimum required keys
    to run the ingestion and cleaning pipeline.
    """
    required_keys = ["client_id", "marketplaces", "cleaning_rules", "output_settings"]
    missing = [key for key in required_keys if key not in config]
    
    if missing:
        logger.error(f"Client config validation failed. Missing keys: {missing}")
        return False
    
    return True