# =========================
#  LOGGER SETUP (PRODUCTION READY)
# =========================

import logging
import os

#  IMPORT CONFIG
from config.settings import LOGGING_CONFIG


def setup_logger(name: str = "engine_logger"):
    """
    Setup logger with console + file handler
    Prevent duplicate handlers
    """

    logger = logging.getLogger(name)

    #  Hindari duplicate handler
    if logger.hasHandlers():
        return logger

    # =========================
    # LOG LEVEL
    # =========================
    log_level = getattr(logging, LOGGING_CONFIG.get("log_level", "INFO"))
    logger.setLevel(log_level)

    # =========================
    #  FORMAT
    # =========================
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # =========================
    # 🖥️ CONSOLE HANDLER
    # =========================
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # =========================
    #  FILE HANDLER (OPTIONAL)
    # =========================
    if LOGGING_CONFIG.get("log_to_file", False):
        log_file_path = LOGGING_CONFIG.get("log_file_path", "logs/engine.log")

        # create folder if not exists
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    # =========================
    #  PROPAGATION OFF
    # =========================
    logger.propagate = False

    return logger