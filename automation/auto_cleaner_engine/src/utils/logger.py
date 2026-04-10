import logging
import os
import sys
from datetime import datetime

def setup_logger(name: str = "AutoCleaner") -> logging.Logger:
    """
    Configures a production-ready logger that outputs to both console and a log file.
    Logs are stored in the /logs directory with daily timestamps.
    """
    # 1. Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 2. Define log format
    log_format = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 3. Setup File Handler (Daily Log)
    log_filename = f"{log_dir}/engine_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(log_format)
    file_handler.setLevel(logging.INFO)

    # 4. Setup Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    console_handler.setLevel(logging.INFO)

    # 5. Initialize Logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if setup_logger is called multiple times
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    # Disable propagation to avoid double logging in certain environments
    logger.propagate = False

    return logger