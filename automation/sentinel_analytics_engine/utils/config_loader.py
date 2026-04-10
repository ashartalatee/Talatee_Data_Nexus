import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

class ConfigLoader:
    """
    Handles the loading and validation of client-specific JSON configurations.
    """
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def load_json(self, path: Path, is_global: bool = False) -> Dict[str, Any]:
        """Generic JSON loader with conditional validation."""
        if not path.exists():
            self.logger.error(f"Configuration file not found at {path}")
            return {}

        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Global config tidak perlu divalidasi dengan struktur client
            if is_global:
                return config
                
            if self._validate_base_structure(config):
                return config
            return {}

        except json.JSONDecodeError as e:
            self.logger.critical(f"Invalid JSON format in {path.name}: {str(e)}")
            return {}
        except Exception as e:
            self.logger.critical(f"Unexpected error loading config: {str(e)}", exc_info=True)
            return {}

    def _validate_base_structure(self, config: Dict[str, Any]) -> bool:
        """Verifies mandatory sections for client-specific pipelines."""
        required_keys = ["client_id", "ingestion", "transformation", "output"]
        missing_keys = [key for key in required_keys if key not in config]

        if missing_keys:
            # Info saja, karena bisa jadi ini file global yang tidak sengaja lewat sini
            self.logger.debug(f"Config is missing client sections: {missing_keys}")
            return False
        return True

# --- Standalone Functions for Orchestrator (main.py) ---

def load_global_config(config_path: Path) -> Dict[str, Any]:
    """
    Loads global settings without client-structure validation.
    """
    loader = ConfigLoader()
    return loader.load_json(config_path, is_global=True)

def load_client_config(config_path: Path) -> Dict[str, Any]:
    """
    Loads client settings with full structural validation.
    """
    loader = ConfigLoader()
    return loader.load_json(config_path, is_global=False)