# =========================
#  STANDARDIZATION MODULE (FINAL - ARCHITECT LEVEL)
# =========================

import pandas as pd

# CONFIG
from config.settings import STANDARD_SCHEMA, REQUIRED_COLUMNS, COLUMN_MAPPING

# LOGGER
from utils.logger import setup_logger

logger = setup_logger("standardize")


# =========================
#  NORMALIZE COLUMN NAMES
# =========================

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"\s+", "_", regex=True)
    )
    return df


# =========================
#  SMART COLUMN MAPPING (UPGRADE)
# =========================

def build_smart_mapping(columns):
    """
    Build dynamic mapping:
    - Exact match from COLUMN_MAPPING
    - Partial match (contains keyword)
    """
    mapping = {}

    for col in columns:
        mapped = None

        # 1. EXACT MATCH (PRIORITY)
        if col in COLUMN_MAPPING:
            mapped = COLUMN_MAPPING[col]

        # 2. PARTIAL MATCH (FALLBACK)
        else:
            for key, val in COLUMN_MAPPING.items():
                if key in col:
                    mapped = val
                    break

        if mapped:
            mapping[col] = mapped

    return mapping


# =========================
#  STANDARDIZE COLUMNS
# =========================

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Columns BEFORE mapping -> {list(df.columns)}")

    smart_map = build_smart_mapping(df.columns)

    df = df.rename(columns=smart_map)

    logger.info(f"Applied mapping -> {smart_map}")
    logger.info(f"Columns AFTER mapping -> {list(df.columns)}")

    return df


# =========================
#  VALIDATE REQUIRED COLUMNS
# =========================

def validate_columns(df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing:
        logger.warning(f"Missing required columns: {missing}")
    else:
        logger.info("All required columns are present")

    return df


# =========================
#  ENFORCE FINAL SCHEMA
# =========================

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    existing = [col for col in STANDARD_SCHEMA if col in df.columns]

    df = df[existing]

    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    logger.info(f"Final schema enforced -> {existing}")

    return df


# =========================
#  FULL STANDARDIZATION PIPELINE
# =========================

def standardization_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_column_names(df)
    df = standardize_columns(df)
    df = validate_columns(df)
    df = enforce_schema(df)

    return df