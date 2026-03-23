import logging
import sys
import os
from logging.handlers import RotatingFileHandler

# ==============================
# CONFIG
# ==============================
LOG_DIR = "data/output/logs"
os.makedirs(LOG_DIR, exist_ok=True)


# ==============================
# LOGGER SETUP (FINAL)
# ==============================
def setup_logger(
    name="MSIDE",
    log_file="mside.log",
    console_level=logging.WARNING,   # 🔥 terminal minimal
    file_level=logging.INFO          # 🔥 file tetap lengkap
):
    logger = logging.getLogger(name)

    # Hindari duplicate handler
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)  # master level (biar fleksibel)

    log_path = os.path.join(LOG_DIR, log_file)

    # ==========================
    # FORMAT
    # ==========================
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_formatter = logging.Formatter(
        "%(message)s"  # 🔥 biar clean di terminal
    )

    # ==========================
    # FILE HANDLER (FULL LOG)
    # ==========================
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8"
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)

    # ==========================
    # CONSOLE HANDLER (RINGKAS)
    # ==========================
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)

    # ==========================
    # ADD HANDLERS
    # ==========================
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ==============================
# SAFE LOG (ANTI WINDOWS ERROR)
# ==============================
def safe_log(message: str):
    return message.encode("ascii", errors="ignore").decode()


# ==============================
# HELPER MODE (BIAR FLEXIBLE)
# ==============================
def get_logger(mode="normal"):
    """
    mode:
    - silent → hampir tidak ada output terminal
    - normal → hanya warning & hasil penting
    - debug  → semua tampil
    """

    if mode == "silent":
        return setup_logger(console_level=logging.ERROR)

    elif mode == "debug":
        return setup_logger(console_level=logging.INFO)

    else:  # normal
        return setup_logger(console_level=logging.WARNING)