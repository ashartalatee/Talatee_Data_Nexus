# =========================
# 🚫 MISSING VALUE HANDLER (PRODUCTION READY)
# =========================

import pandas as pd
from config.settings import CLEANING_CONFIG
from utils.logger import setup_logger

logger = setup_logger("missing_handler")


# =========================
# 🔍 CHECK MISSING VALUES
# =========================

def check_missing(df: pd.DataFrame) -> pd.DataFrame:
    missing_summary = df.isnull().sum()
    logger.info("Missing Values Summary:")
    for col, val in missing_summary.items():
        logger.info(f"{col}: {val}")
    return df


# =========================
# 🧹 HANDLE MISSING VALUES
# =========================

def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # Drop critical columns (product_name, quantity, price)
    critical_cols = ["product_name", "quantity", "price"]
    existing_critical = [col for col in critical_cols if col in df.columns]
    if existing_critical:
        df = df.dropna(subset=existing_critical)

    after = len(df)
    logger.info(f"Dropped {before - after} rows due to missing critical values")

    # Fill default values
    fill_defaults = CLEANING_CONFIG.get("fill_defaults", {"source": "unknown"})
    for col, default_val in fill_defaults.items():
        if col in df.columns:
            df[col] = df[col].fillna(default_val)

    # Optional strategy: mean / median / mode
    strategy_cols = CLEANING_CONFIG.get("fill_strategy", {})
    for col, strategy in strategy_cols.items():
        if col not in df.columns:
            continue
        if strategy == "mean":
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "median":
            df[col] = df[col].fillna(df[col].median())
        elif strategy == "mode":
            df[col] = df[col].fillna(df[col].mode()[0])

    logger.info("Missing handling completed")
    return df


# =========================
# 🚀 FULL MISSING PIPELINE
# =========================

def missing_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = check_missing(df)
    df = handle_missing(df)
    return df