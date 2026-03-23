import pandas as pd
import os

from src.utils.logger import get_logger


# ==============================
# 🔥 SAFE CONCAT (ANTI CRASH)
# ==============================
def merge_data(standardized_dfs, logger):
    logger.info("Merging datasets...")

    # Filter dataframe yang valid saja
    valid_dfs = []
    for i, df in enumerate(standardized_dfs):
        if df is None:
            logger.warning(f"Dataset index {i} is None, skipped")
            continue
        if df.empty:
            logger.warning(f"Dataset index {i} is empty, skipped")
            continue
        valid_dfs.append(df)

    if not valid_dfs:
        logger.error("No valid dataframes to merge")
        return pd.DataFrame()

    master_df = pd.concat(valid_dfs, ignore_index=True)

    logger.info(f"Merged rows: {len(master_df)}")
    return master_df


# ==============================
# 🔥 ENSURE REQUIRED COLUMNS
# ==============================
def ensure_schema(df, logger):
    required_columns = {
        "order_id": None,
        "date": None,
        "revenue": 0,
        "product": "UNKNOWN_PRODUCT",
        "quantity": 0,
        "source": "UNKNOWN_SOURCE"
    }

    for col, default in required_columns.items():
        if col not in df.columns:
            logger.warning(f"Column missing: {col}, filling with default")
            df[col] = default

    return df


# ==============================
# 🔥 REMOVE DUPLICATES (SAFE)
# ==============================
def remove_duplicates(df, logger):
    logger.info("Removing duplicates...")

    df = ensure_schema(df, logger)

    before = len(df)

    subset_cols = ["date", "product", "revenue", "quantity", "source"]
    subset_cols = [col for col in subset_cols if col in df.columns]

    df = df.drop_duplicates(subset=subset_cols)

    after = len(df)
    removed = before - after

    logger.info(f"Duplicates removed: {removed}")
    return df


# ==============================
# 🔥 DATA VALIDATION (NON-BLOCKING)
# ==============================
def validate_master_data(df, logger):
    logger.info("Validating master dataset...")

    if df.empty:
        logger.warning("Master dataset is empty")
        return df

    if "date" in df.columns and df["date"].isnull().any():
        logger.warning("Missing date detected")

    if "revenue" in df.columns and (df["revenue"] < 0).any():
        logger.warning("Negative revenue detected")

    return df


# ==============================
# 🔥 SORTING (SAFE)
# ==============================
def sort_master_data(df, logger):
    logger.info("Sorting dataset...")

    if "date" in df.columns:
        df = df.sort_values("date")
    else:
        logger.warning("Date column missing, skip sorting")

    return df


# ==============================
# 🔥 SAVE OUTPUT (ANTI ERROR)
# ==============================
def save_master_data(df, logger, path="data/output/master_data.csv"):
    logger.info("Saving master dataset...")

    if df.empty:
        logger.warning("Data kosong, tetap disimpan untuk debugging")

    os.makedirs(os.path.dirname(path), exist_ok=True)

    try:
        df.to_csv(path, index=False)
        logger.info(f"Saved to {path}")
    except Exception as e:
        logger.error(f"Failed to save file: {e}")


# ==============================
# 🔥 MAIN PIPELINE
# ==============================
def merge_all(standardized_dfs):
    logger = get_logger()

    df = merge_data(standardized_dfs, logger)

    if df.empty:
        logger.warning("Pipeline lanjut walaupun data kosong")

    df = ensure_schema(df, logger)

    df = remove_duplicates(df, logger)

    df = validate_master_data(df, logger)

    df = sort_master_data(df, logger)

    save_master_data(df, logger)

    logger.info("✅ MERGE PIPELINE SUCCESS")

    return df