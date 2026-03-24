import pandas as pd
from src.utils.logger import get_logger

logger = get_logger()


# ==============================
# EXTRACTION HELPERS
# ==============================

def extract_price(df):
    for col in df.columns:
        if "price" in col.lower():
            return pd.to_numeric(df[col], errors='coerce')
    return pd.Series([0] * len(df))


def extract_quantity(df):
    for col in df.columns:
        if any(x in col.lower() for x in ["qty", "quantity", "items_sold"]):
            return pd.to_numeric(df[col], errors='coerce')
    return pd.Series([1] * len(df))


def extract_date(df):
    for col in ["order_date", "date", "created_at"]:
        if col in df.columns:
            return pd.to_datetime(df[col], errors='coerce')
    return pd.Series([pd.NaT] * len(df))


def extract_product(df):
    for col in ["product_name", "product", "name"]:
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series(["UNKNOWN_PRODUCT"] * len(df))


# ==============================
# TRANSFORM SINGLE DF (FINAL)
# ==============================

def transform_single_df(df, source):
    logger.info(f"Transforming {source}")

    df = df.copy()

    # ==============================
    # EXTRACTION
    # ==============================
    df["price"] = extract_price(df)
    df["quantity"] = extract_quantity(df)
    df["date"] = extract_date(df)
    df["product"] = extract_product(df)

    # ==============================
    #  HARD VALIDATION 1: DATE
    # ==============================
    if df["date"].isna().all():
        raise ValueError(f"{source}: CRITICAL → all date invalid")

    # ==============================
    # CORE METRIC
    # ==============================
    df["revenue"] = df["price"] * df["quantity"]

    # ==============================
    #  HARD VALIDATION 2: REVENUE
    # ==============================
    if df["revenue"].fillna(0).sum() == 0:
        raise ValueError(f"{source}: CRITICAL → revenue all zero")

    # ==============================
    # CLEANING
    # ==============================
    df = df[df["revenue"] >= 0]

    df["price"] = df["price"].fillna(0)
    df["quantity"] = df["quantity"].fillna(0)
    df["revenue"] = df["revenue"].fillna(0)
    df["product"] = df["product"].fillna("UNKNOWN_PRODUCT")

    # ==============================
    # FEATURE ENGINEERING
    # ==============================
    df["day_of_week"] = df["date"].dt.day_name()

    # ==============================
    # ANOMALY DETECTION
    # ==============================
    mean_revenue = df["revenue"].mean()

    df["is_anomaly"] = (
        df["revenue"] > mean_revenue * 5
        if pd.notnull(mean_revenue) and mean_revenue > 0
        else False
    )

    # ==============================
    # FINAL CLEAN
    # ==============================
    df = df.dropna(subset=["date"])
    df = df.sort_values("date")

    df["source"] = source

    logger.info(f"{source}: success ({len(df)} rows)")

    return df[[
        "date",
        "day_of_week",
        "product",
        "price",
        "quantity",
        "revenue",
        "is_anomaly",
        "source"
    ]]


# ==============================
# MULTI DATA TRANSFORM
# ==============================

def transform_all(cleaned_dfs):
    transformed_dfs = []

    logger.info("Starting transformation process...")

    for i, df in enumerate(cleaned_dfs):

        if df is None:
            logger.warning(f"Dataset index {i} is None, skipped")
            continue

        if df.empty:
            logger.warning(f"Dataset index {i} is empty, skipped")
            continue

        if "source" not in df.columns:
            logger.error("Missing 'source' column")
            continue

        source = df["source"].iloc[0]

        try:
            transformed_df = transform_single_df(df, source)
            transformed_dfs.append(transformed_df)

        except Exception as e:
            logger.error(f"{source}: FAILED → {str(e)}")

    logger.info(f"Transformation finished: {len(transformed_dfs)} datasets")

    return transformed_dfs