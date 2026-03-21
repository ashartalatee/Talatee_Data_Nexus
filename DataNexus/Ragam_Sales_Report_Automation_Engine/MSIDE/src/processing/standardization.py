import pandas as pd
from src.utils.config import load_mapping
from src.utils.logger import get_logger


logger = get_logger()


# ==============================
# CORE STANDARDIZATION FUNCTION
# ==============================

def standardize_single_df(df, source, mapping):
    logger.info(f"Standardizing source: {source}")

    if source not in mapping:
        logger.error(f"No mapping found for source: {source}")
        return pd.DataFrame()

    map_config = mapping[source]

    try:
        standardized_df = pd.DataFrame()

        # ==============================
        # BASIC TRANSFORM
        # ==============================
        standardized_df["date"] = pd.to_datetime(
            df[map_config["date"]], errors="coerce"
        )

        standardized_df["product"] = df[map_config["product"]].astype(str)

        standardized_df["price"] = pd.to_numeric(
            df[map_config["price"]], errors="coerce"
        )

        standardized_df["quantity"] = pd.to_numeric(
            df[map_config["quantity"]], errors="coerce"
        )

        # ==============================
        # CORE METRIC
        # ==============================
        standardized_df["revenue"] = (
            standardized_df["price"] * standardized_df["quantity"]
        )

        # ==============================
        # CLEAN NEGATIVE
        # ==============================
        before_count = len(standardized_df)

        standardized_df = standardized_df[
            standardized_df["revenue"] >= 0
        ]

        after_count = len(standardized_df)
        removed = before_count - after_count

        if removed > 0:
            logger.debug(f"{source}: removed {removed} negative revenue rows")

        # ==============================
        # BUSINESS FEATURE
        # ==============================
        standardized_df["day_of_week"] = standardized_df["date"].dt.day_name()

        # ==============================
        # ANOMALY DETECTION
        # ==============================
        mean_rev = standardized_df["revenue"].mean()

        if pd.notnull(mean_rev) and mean_rev != 0:
            standardized_df["is_anomaly"] = (
                standardized_df["revenue"] > mean_rev * 5
            )
        else:
            standardized_df["is_anomaly"] = False

        # ==============================
        # FINAL CLEAN
        # ==============================
        standardized_df = standardized_df.fillna({
            "price": 0,
            "quantity": 0,
            "revenue": 0,
            "product": "unknown"
        })

        standardized_df["source"] = source

        # ==============================
        # SORT
        # ==============================
        standardized_df = standardized_df.sort_values("date")

        logger.debug(f"{source}: standardization complete ({len(standardized_df)} rows)")

        return standardized_df

    except Exception as e:
        logger.error(f"Error standardizing {source}", exc_info=True)
        return pd.DataFrame()


# ==============================
# MULTI STANDARDIZATION
# ==============================

def standardize_all(cleaned_dfs):
    mapping = load_mapping()
    standardized_dfs = []

    logger.info("Starting standardization process...")

    for df in cleaned_dfs:
        if df.empty:
            logger.debug("Skipping empty dataframe")
            continue

        if "source" not in df.columns:
            logger.error("Missing 'source' column in dataframe")
            continue

        source = df["source"].iloc[0]

        std_df = standardize_single_df(df.copy(), source, mapping)

        if not std_df.empty:
            standardized_dfs.append(std_df)
        else:
            logger.warning(f"{source}: standardization resulted in empty dataframe")

    logger.info(f"Standardization finished: {len(standardized_dfs)} datasets processed")

    return standardized_dfs