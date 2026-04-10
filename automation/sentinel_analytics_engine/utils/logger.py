import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

def setup_logger(name: str, log_file: Path, level: int = logging.INFO) -> logging.Logger:
    """
    Standardized logger for Talatee Sentinel.
    Disesuaikan agar menerima Path file lengkap (log_file) sesuai logika main.py.
    """
    # Inisialisasi Logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Mencegah duplikasi handler jika logger dipanggil ulang
    if logger.hasHandlers():
        return logger

    # Format: 2026-04-10 13:00:00 | INFO | logger_name | message
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Console Handler (Tampil di Terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Rotating File Handler (Simpan di File)
    try:
        # Pastikan folder tempat file log berada sudah dibuat
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=5 * 1024 * 1024, # 5MB per file
            backupCount=3,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        # Jika gagal menulis file (misal: permission denied), log ke console saja
        print(f"CRITICAL: Gagal inisialisasi file log di {log_file}: {e}")

    return logger

def get_module_logger(module_name: str, client_id: str) -> logging.Logger:
    """
    Membuat child logger agar hierarki log terjaga (misal: sentinel_core.ingestion)
    """
    return logging.getLogger(f"sentinel_core.{client_id}.{module_name}")