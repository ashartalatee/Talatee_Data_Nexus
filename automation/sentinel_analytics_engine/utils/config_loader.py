import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

class ConfigLoader:
    """
    Handles the loading and validation of client-specific JSON configurations.
    Ensures the Talatee Sentinel Engine operates with correct client parameters.
    """
    def __init__(self, config_dir: Path, logger: Optional[logging.Logger] = None):
        self.config_dir = config_dir
        self.logger = logger or logging.getLogger(__name__)

    def load_client_config(self, client_id: str) -> Optional[Dict[str, Any]]:
        """
        Loads a JSON configuration file for a specific client.
        """
        config_path = self.config_dir / f"{client_id}.json"
        
        if not config_path.exists():
            self.logger.error(f"Configuration file not found for client: {client_id} at {config_path}")
            return None

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            if self._validate_base_structure(config):
                self.logger.info(f"Successfully loaded configuration for client: {client_id}")
                return config
            else:
                self.logger.error(f"Configuration validation failed for client: {client_id}")
                return None

        except json.JSONDecodeError as e:
            self.logger.critical(f"Invalid JSON format in {config_path.name}: {str(e)}")
            return None
        except Exception as e:
            self.logger.critical(f"Unexpected error loading config for {client_id}: {str(e)}", exc_info=True)
            return None

    def _validate_base_structure(self, config: Dict[str, Any]) -> bool:
        """
        Verifies that the configuration contains the mandatory sections 
        required for pipeline execution.
        """
        required_keys = ["client_id", "ingestion", "validation", "transformation", "output"]
        missing_keys = [key for key in required_keys if key not in config]

        if missing_keys:
            self.logger.error(f"Config is missing mandatory sections: {missing_keys}")
            return False
            
        return True

    @staticmethod
    def get_default_config() -> Dict[str, Any]:
        """
        Returns a template structure for new client onboarding.
        """
        return {
            "client_id": "template_client",
            "strict_mode": True,
            "ingestion": {
                "source_type": "csv",
                "file_pattern": "*.csv"
            },
            "validation": {
                "required_columns": [],
                "data_types": {}
            },
            "cleaning": {
                "remove_duplicates": True,
                "handle_missing": {"strategy": "fill", "columns": {}}
            },
            "transformation": {
                "column_mapping": {},
                "timezone": "UTC"
            },
            "analysis": {
                "metrics": ["gmv", "total_orders"],
                "granularity": "daily"
            },
            "output": {
                "format": "excel"
            }
        }