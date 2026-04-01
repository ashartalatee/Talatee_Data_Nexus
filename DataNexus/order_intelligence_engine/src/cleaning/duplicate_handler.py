import pandas as pd
from config.settings import DUPLICATE_CONFIG
from utils.logger import setup_logger

logger = setup_logger("duplicate_handler")


# =========================
# 🔍 CHECK DUPLICATES
# =========================

def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    exact_duplicates = df.duplicated().sum()

    subset_cols = DUPLICATE_CONFIG.get("exact", {}).get("subset", ["product_name", "price"])
    existing_subset = [col for col in subset_cols if col in df.columns]
    subset_duplicates = df.duplicated(subset=existing_subset).sum() if existing_subset else 0

    logger.info(f"Exact duplicates: {exact_duplicates}")
    logger.info(f"Subset duplicates ({existing_subset}): {subset_duplicates}")
    return df


# =========================
# 🧹 REMOVE DUPLICATES
# =========================

def remove_exact_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    logger.info(f"Removed {before - after} exact duplicate rows")
    return df


def remove_duplicates_subset(df: pd.DataFrame) -> pd.DataFrame:
    subset_cols = DUPLICATE_CONFIG.get("exact", {}).get("subset", ["product_name", "price"])
    missing_cols = [col for col in subset_cols if col not in df.columns]

    if missing_cols:
        logger.warning(f"Missing columns for deduplication: {missing_cols}")
        return df

    before = len(df)
    df = df.drop_duplicates(subset=subset_cols)
    after = len(df)
    logger.info(f"Removed {before - after} logical duplicate rows (subset={subset_cols})")
    return df


# =========================
# 🚀 FULL DUPLICATE PIPELINE
# =========================

def duplicate_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = check_duplicates(df)

    strategy = DUPLICATE_CONFIG.get("method", "both")

    if strategy == "exact":
        df = remove_exact_duplicates(df)
    elif strategy == "subset":
        df = remove_duplicates_subset(df)
    elif strategy == "both":
        df = remove_exact_duplicates(df)
        df = remove_duplicates_subset(df)
    else:
        logger.warning(f"Unknown duplicate strategy: {strategy}")

    return df