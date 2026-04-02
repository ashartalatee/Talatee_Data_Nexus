import logging
import os
from logging.handlers import RotatingFileHandler
import sys

# ========================
# PAKSA STDOUT UTF-8 (Windows)
# ========================
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ========================
# BASIC LOGGER CONFIG
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def setup_logger(
    name: str = "app_logger",
    log_file: str = "logs/app.log",
    level: int = logging.INFO,
    max_bytes: int = 5 * 1024 * 1024,  # 5 MB
    backup_count: int = 3,
) -> logging.Logger:
    """
    Production-ready logger:
    - Prevent duplicate handlers
    - File rotation (biar log gak numpuk)
    - Console + file output
    - Flexible level (INFO, DEBUG, ERROR)
    - Fully Unicode-safe (Windows/UTF-8)
    """

    # ========================
    # 1. CREATE LOG FOLDER
    # ========================
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # ========================
    # 2. AVOID DUPLICATE HANDLER
    # ========================
    if logger.handlers:
        return logger

    # ========================
    # 3. FORMATTER (STRUCTURED)
    # ========================
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
    )

    # ========================
    # 4. FILE HANDLER (ROTATING)
    # ========================
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"  # penting supaya file log bisa tulis Unicode
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # ========================
    # 5. CONSOLE HANDLER
    # ========================
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # ========================
    # 6. ADD HANDLERS
    # ========================
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # ========================
    # 7. PREVENT PROPAGATION
    # ========================
    logger.propagate = False

    logger.info("Logger initialized (Unicode safe, rotating file ready)")

    return logger