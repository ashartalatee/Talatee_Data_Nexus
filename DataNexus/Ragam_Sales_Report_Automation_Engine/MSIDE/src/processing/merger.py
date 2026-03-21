import pandas as pd
import os

from src.utils.logger import get_logger


# ==============================
# 🔥 MERGE ALL DATA
# ==============================
def merge_data(standardized_dfs, logger):
    logger.info("Merging datasets...")

    master_df = pd.concat(standardized_dfs, ignore_index=True)

    logger.debug(f"Merged rows: {len(master_df)}")

    return master_df


# ==============================
# 🔥 REMOVE DUPLICATES
# ==============================
def remove_duplicates(df, logger):
    logger.info("Removing duplicates...")

    before = len(df)

    df = df.drop_duplicates(
        subset=["date", "product", "price", "quantity", "source"]
    )

    after = len(df)
    removed = before - after

    logger.info(f"Duplicates removed: {removed}")
    logger.debug(f"Remaining rows: {after}")

    return df


# ==============================
# 🔥 DATA INTEGRITY CHECK
# ==============================
def validate_master_data(df, logger):
    logger.info("Validating master dataset...")

    if df.empty:
        logger.error("Master dataset is empty!")
        return df

    if df["date"].isnull().any():
        logger.warning("Missing date detected")

    if (df["revenue"] < 0).any():
        logger.warning("Negative revenue detected")

    return df


# ==============================
# 🔥 FINAL SORTING
# ==============================
def sort_master_data(df, logger):
    logger.info("Sorting dataset by date...")

    df = df.sort_values("date")

    return df


# ==============================
# 🔥 SAVE OUTPUT
# ==============================
def save_master_data(df, logger, path="data/output/master_data.csv"):
    logger.info("Saving master dataset...")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

    logger.info(f"Saved to {path}")


# ==============================
# 🔥 MAIN MERGE PIPELINE
# ==============================
def merge_all(standardized_dfs):
    logger = get_logger()

    df = merge_data(standardized_dfs, logger)

    df = remove_duplicates(df, logger)

    df = validate_master_data(df, logger)

    df = sort_master_data(df, logger)

    save_master_data(df, logger)

    return df