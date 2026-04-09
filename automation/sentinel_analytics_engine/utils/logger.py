import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

def setup_logger(client_id: str, log_dir: Path, level: int = logging.INFO) -> logging.Logger:
    """
    Configures a production-grade logger with both console and rotating file handlers.
    Each client execution is isolated via unique logger names and file paths.
    """
    # Ensure log directory exists
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{client_id}_pipeline.log"

    # Create logger
    logger = logging.getLogger(f"TalateeSentinel.{client_id}")
    logger.setLevel(level)

    # Prevent duplicate handlers if logger is re-initialized in the same process
    if logger.hasHandlers():
        return logger

    # Formatting: [Timestamp] [Level] [Module] - Message
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Rotating File Handler (Production safety: 5MB per file, keep 3 backups)
    try:
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=5 * 1024 * 1024, 
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        # Fallback to console only if file system is read-only or permission denied
        logger.error(f"Failed to initialize file logger at {log_file}: {str(e)}")

    return logger

def get_module_logger(module_name: str, client_id: str) -> logging.Logger:
    """
    Returns a child logger for specific modules to maintain hierarchy.
    """
    return logging.getLogger(f"TalateeSentinel.{client_id}.{module_name}")