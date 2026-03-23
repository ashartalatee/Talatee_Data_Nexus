import pandas as pd
from src.utils.logger import get_logger


# ==============================
# CONFIG (LEBIH FLEXIBLE)
# ==============================
CRITICAL_COLUMNS = {
    "shopee": ["order_id"],
    "tokopedia": ["invoice"],
    "tiktok": ["id"]
}

OPTIONAL_COLUMNS = {
    "shopee": ["product_name", "price", "quantity", "order_date"],
    "tokopedia": ["product", "total_price", "qty", "date"],
    "tiktok": ["name", "price_per_item", "items_sold", "created_at"]
}


# ==============================
# NORMALIZE COLUMN NAMES
# ==============================
def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


# ==============================
# VALIDATE COLUMNS (SMART)
# ==============================
def validate_columns(df, source, logger):
    critical = CRITICAL_COLUMNS.get(source, [])
    optional = OPTIONAL_COLUMNS.get(source, [])

    missing_critical = [col for col in critical if col not in df.columns]
    missing_optional = [col for col in optional if col not in df.columns]

    # 🔥 CRITICAL → HARUS ADA
    if missing_critical:
        logger.error(f"{source} missing CRITICAL columns: {missing_critical}")
    else:
        logger.debug(f"{source} critical columns OK")

    # 🟡 OPTIONAL → TIDAK MASALAH
    if missing_optional:
        logger.warning(f"{source} missing optional columns: {missing_optional}")

    return df


# ==============================
# VALIDATE MISSING VALUES
# ==============================
def validate_missing_values(df, source, logger):
    missing = df.isnull().sum()
    missing = missing[missing > 0]

    if not missing.empty:
        logger.warning(f"{source} has missing values")
        logger.debug(f"\n{missing}")
    else:
        logger.debug(f"{source} no missing values")

    return df


# ==============================
# VALIDATE DATA TYPES
# ==============================
def validate_types(df, source, logger):
    logger.debug(f"{source} data types:")
    logger.debug(f"\n{df.dtypes}")
    return df


# ==============================
# DATA QUALITY SCORE (🔥 BARU)
# ==============================
def calculate_data_quality(df):
    total_cells = df.size
    missing_cells = df.isnull().sum().sum()

    if total_cells == 0:
        return 0

    score = 100 - (missing_cells / total_cells * 100)
    return round(score, 2)


# ==============================
# VALIDATE SINGLE DF
# ==============================
def validate_single_df(df, source, logger):
    df = normalize_columns(df)

    df = validate_columns(df, source, logger)
    df = validate_missing_values(df, source, logger)
    df = validate_types(df, source, logger)

    quality_score = calculate_data_quality(df)
    logger.info(f"{source} data quality score: {quality_score}%")

    return df


# ==============================
# VALIDATE ALL DATA
# ==============================
def validate_all(dfs):
    logger = get_logger()
    validated_dfs = []

    for df in dfs:
        if df is None or df.empty:
            logger.warning("Empty dataframe skipped")
            continue

        # 🔥 FIX SOURCE DETECTION
        if "source" in df.columns:
            source = df["source"].iloc[0]
        else:
            source = "unknown"

        logger.info(f"Validating {source}")

        try:
            validated_df = validate_single_df(df, source, logger)
            validated_dfs.append(validated_df)
        except Exception as e:
            logger.error(f"Validation failed for {source}: {str(e)}")

    logger.info(f"Validated datasets: {len(validated_dfs)}")

    return validated_dfs