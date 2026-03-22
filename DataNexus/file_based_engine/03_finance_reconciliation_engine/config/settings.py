from pathlib import Path
import os

# =========================================
# BASE DIRECTORY
# =========================================
BASE_DIR = Path(__file__).resolve().parent.parent

# =========================================
# MAIN CONFIG
# =========================================
CONFIG = {

    # ======================
    # PATH MANAGEMENT
    # ======================
    "paths": {
        "base_dir": BASE_DIR,
        "input_dir": BASE_DIR / "input",
        "bank_dir": BASE_DIR / "input/bank",
        "invoice_dir": BASE_DIR / "input/invoice",
        "output_dir": BASE_DIR / "output",
        "log_dir": BASE_DIR / "logs",
    },

    # ======================
    # FILE CONFIG
    # ======================
    "files": {
        "bank_file": "bank_statement.csv",
        "invoice_file": "invoice.csv",
        "output_file": "reconciliation_report.csv",
    },

    # ======================
    # LOGGING CONFIG ( DIPISAH — PENTING)
    # ======================
    "logging": {
        "log_file": "engine.log",
        "log_level": "INFO"
    },

    # ======================
    # ENGINE SETTINGS
    # ======================
    "engine": {
        "mode": "single",          # "single" / "batch"
        "scheduler_interval": 10,  # seconds
        "max_runs": None           # None = infinite | atau angka (misal 5)
    },

    # ======================
    # DATA QUALITY RULES
    # ======================
    "data_quality": {
        "critical_columns": ["amount"],
        "allow_negative": False,
    }
}


# =========================================
# HELPER FUNCTIONS
# =========================================

def get_path(key: str):
    return CONFIG["paths"][key]


def get_file(key: str):
    return CONFIG["files"][key]


def get_full_path(path_key: str, file_key: str):
    return get_path(path_key) / get_file(file_key)


def get_log_path():
    log_dir = CONFIG["paths"]["log_dir"]
    log_file = CONFIG["logging"]["log_file"]
    return log_dir / log_file


def ensure_directories():
    for path in CONFIG["paths"].values():
        if isinstance(path, Path):
            os.makedirs(path, exist_ok=True)


def get_log_level():
    level_str = CONFIG["logging"].get("log_level", "INFO").upper()
    return getattr(__import__("logging"), level_str, 20)  # default INFO