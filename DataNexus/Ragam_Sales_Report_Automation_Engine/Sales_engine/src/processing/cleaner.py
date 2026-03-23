import pandas as pd
from src.utils.logger import get_logger

logger = get_logger()


# ==============================
# 🔥 LOGGING PERUBAHAN
# ==============================
def log_cleaning_changes(before, after, source):
    logger.info(f"{source} rows before: {len(before)}, after: {len(after)}")


# ==============================
# CLEANING FUNCTIONS
# ==============================

def clean_missing_values(df, source):
    logger.info(f"Cleaning missing values: {source}")

    df = df.copy()

    # Drop row yang terlalu banyak missing
    df = df.dropna(thresh=int(len(df.columns) * 0.5))

    # Numeric → isi median
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    # Object → isi "Unknown"
    object_cols = df.select_dtypes(include=['object']).columns
    for col in object_cols:
        df[col] = df[col].fillna("Unknown")

    return df


def clean_dates(df, source):
    logger.info(f"Standardizing date: {source}")

    df = df.copy()

    date_columns = ["order_date", "date", "created_at"]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df


def clean_category(df, source):
    logger.info(f"Cleaning category: {source}")

    df = df.copy()

    if "category" in df.columns:
        df["category"] = (
            df["category"]
            .astype(str)
            .str.lower()
            .str.strip()
            .replace({
                "fashon": "fashion",
                "gadjet": "gadget"
            })
        )

    return df


def clean_numeric(df, source):
    logger.info(f"Cleaning numeric: {source}")

    df = df.copy()

    for col in df.columns:
        if any(keyword in col.lower() for keyword in ["price", "qty", "quantity"]):
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# ==============================
# NORMALIZE TEXT
# ==============================
def normalize_text(df, source):
    logger.info(f"Normalizing text: {source}")

    df = df.copy()

    text_columns = ["product_name", "product", "name"]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    return df


# ==============================
# CLEAN PIPELINE WRAPPER
# ==============================
def clean_single_df(df, source):
    before = df.copy()

    df = clean_missing_values(df, source)
    df = clean_dates(df, source)
    df = clean_category(df, source)
    df = clean_numeric(df, source)
    df = normalize_text(df, source)

    log_cleaning_changes(before, df, source)

    return df


# ==============================
# MAIN CLEAN FUNCTION
# ==============================
def clean_all(validated_dfs):
    cleaned_dfs = []

    for df in validated_dfs:
        if df.empty:
            continue

        source = df["source"].iloc[0]

        logger.info(f"Cleaning {source}")

        cleaned_df = clean_single_df(df, source)
        cleaned_dfs.append(cleaned_df)

    return cleaned_dfs