import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

def setup_custom_logger(module_name: str, log_level: int = logging.INFO, log_file: Optional[str] = None) -> logging.Logger:
    """
    Sets up a production-grade logger with both console and rotating file handlers.
    
    :param module_name: Name of the module for the logger instance.
    :param log_level: Logging level (default: INFO).
    :param log_file: Optional specific filename. Defaults to logs/{module_name}.log.
    :return: Configured logging.Logger instance.
    """
    logger = logging.getLogger(module_name)
    
    # Avoid duplicate handlers if logger is already initialized
    if logger.hasHandlers():
        return logger

    logger.setLevel(log_level)

    # Standard production format: timestamp | level | module | message
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Rotating File Handler
    try:
        log_dir = Path("logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        target_file = log_file if log_file else f"{module_name}.log"
        log_path = log_dir / target_file

        # Rotate at 10MB, keep 5 backup files
        file_handler = RotatingFileHandler(
            log_path, 
            maxBytes=10 * 1024 * 1024, 
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        # Fallback if filesystem is read-only or inaccessible
        logger.error(f"Failed to initialize file logging: {e}")

    return logger