import logging
import os
import json
from logging.handlers import RotatingFileHandler

class CustomLogger:
    def __init__(self, name: str, config_path: str = "config/global_config.json"):
        self.logger = logging.getLogger(name)
        self.config = self._load_global_config(config_path)
        
        # Avoid adding multiple handlers if logger already exists
        if not self.logger.handlers:
            self._setup_logging()

    def _load_global_config(self, path: str) -> dict:
        """Loads logging settings from the global configuration file."""
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        # Fallback default configuration
        return {
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "log_to_file": True,
                "max_bytes": 10485760,
                "backup_count": 5
            },
            "storage": {"log_dir": "logs"}
        }

    def _setup_logging(self):
        log_cfg = self.config.get("logging", {})
        storage_cfg = self.config.get("storage", {})
        
        # Set level
        level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
        self.logger.setLevel(level)

        # Formatter
        formatter = logging.Formatter(log_cfg.get("format"))

        # Stream Handler (Console)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File Handler (Optional based on config)
        if log_cfg.get("log_to_file", True):
            log_dir = storage_cfg.get("log_dir", "logs")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            log_file = os.path.join(log_dir, "engine.log")
            file_handler = RotatingFileHandler(
                log_file, 
                maxBytes=log_cfg.get("max_bytes", 10485760),
                backupCount=log_cfg.get("backup_count", 5)
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def get_logger(self) -> logging.Logger:
        """Returns the configured logger instance."""
        return self.logger