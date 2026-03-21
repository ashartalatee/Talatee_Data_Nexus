import pandas as pd
from src.utils.logger import get_logger


# ==============================
# REQUIRED COLUMNS CONFIG
# ==============================
REQUIRED_COLUMNS = {
    "shopee": ["order_id", "product_name", "price", "quantity", "order_date"],
    "tokopedia": ["invoice", "product", "total_price", "qty", "date"],
    "tiktok": ["id", "name", "price_per_item", "items_sold", "created_at"]
}


# ==============================
# VALIDATE COLUMNS
# ==============================
def validate_columns(df, source, logger):
    expected_cols = REQUIRED_COLUMNS.get(source, [])
    missing_cols = [col for col in expected_cols if col not in df.columns]

    if missing_cols:
        logger.warning(f"{source} missing columns: {missing_cols}")
    else:
        logger.debug(f"{source} columns valid")

    return df


# ==============================
# VALIDATE MISSING VALUES
# ==============================
def validate_missing_values(df, source, logger):
    missing = df.isnull().sum()
    missing = missing[missing > 0]

    if not missing.empty:
        logger.warning(f"{source} missing values detected")
        logger.debug(f"\n{missing}")
    else:
        logger.debug(f"{source} no missing values")

    return df


# ==============================
# VALIDATE DATA TYPES
# ==============================
def validate_types(df, source, logger):
    logger.debug(f"Checking data types for {source}")
    logger.debug(f"\n{df.dtypes}")
    return df


# ==============================
# VALIDATE SINGLE DATAFRAME
# ==============================
def validate_single_df(df, source, logger):
    df = validate_columns(df, source, logger)
    df = validate_missing_values(df, source, logger)
    df = validate_types(df, source, logger)
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

        source = df.get("source", pd.Series(["unknown"])).iloc[0]

        logger.info(f"Validating {source}")

        validated_df = validate_single_df(df, source, logger)
        validated_dfs.append(validated_df)

    # ❗ PENTING: JANGAN pakai WARNING di sini
    logger.info(f"Validated datasets: {len(validated_dfs)}")

    return validated_dfs