import pandas as pd
from src.utils.config import load_mapping
from src.utils.logger import get_logger
from src.utils.column_mapper import smart_map_columns

logger = get_logger()


# ==============================
# SAFE SERIES
# ==============================
def safe_series(df, col, default=None, dtype=None):
    if col and col in df.columns:
        series = df[col]
    else:
        series = pd.Series([default] * len(df))

    if dtype:
        try:
            series = series.astype(dtype)
        except Exception:
            pass

    return series


# ==============================
# SMART DATE PARSER
# ==============================
def parse_date(series):
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)

    if parsed.isna().all():
        parsed = pd.to_datetime(series, errors="coerce", format="%Y-%m-%d")

    if parsed.isna().all():
        parsed = pd.to_datetime(series, errors="coerce", format="%d/%m/%Y")

    return parsed


# ==============================
# SEMANTIC VALIDATION
# ==============================
def semantic_validation(df, map_config, source):
    # DATE
    date_series = safe_series(df, map_config.get("date"))
    parsed_date = parse_date(date_series)

    valid_ratio = parsed_date.notna().mean()

    if valid_ratio < 0.6:
        raise ValueError(
            f"{source}: CRITICAL → invalid date mapping (valid ratio={valid_ratio:.2f})"
        )

    # REVENUE
    revenue_series = pd.to_numeric(
        safe_series(df, map_config.get("revenue")),
        errors="coerce"
    )

    if revenue_series.fillna(0).sum() <= 0:
        raise ValueError(f"{source}: CRITICAL → revenue not valid")

    # PRODUCT
    product_series = safe_series(df, map_config.get("product"))

    if len(product_series) > 0:
        unique_ratio = product_series.nunique() / len(product_series)
        if unique_ratio < 0.01:
            logger.warning(f"{source}: product column low variance (suspicious)")

    return True


# ==============================
# VALIDATE MAPPING
# ==============================
def validate_mapping(map_config, df, source):
    suspicious_keywords = ["id", "order", "number"]

    date_col = map_config.get("date")

    if date_col:
        for word in suspicious_keywords:
            if word in date_col.lower():
                logger.warning(f"{source}: suspicious date column → {date_col}")
                map_config["date"] = None

    if map_config.get("product") == map_config.get("price"):
        logger.warning(f"{source}: product = price detected → reset")
        map_config["product"] = None

    required_fields = ["date", "product", "revenue"]

    missing = [f for f in required_fields if map_config.get(f) is None]

    if missing:
        raise ValueError(f"{source}: CRITICAL missing mapping → {missing}")

    return map_config


# ==============================
# MERGE MAPPING (FINAL LOGIC)
# ==============================
def merge_mapping(auto_map, manual_map, source):
    final_map = auto_map.copy()

    for k, v in manual_map.items():
        if v is None:
            continue

        if auto_map.get(k) != v:
            logger.info(
                f"{source}: override mapping → {k} | auto: {auto_map.get(k)} → manual: {v}"
            )

        final_map[k] = v

    return final_map


# ==============================
# STANDARDIZE SINGLE DF
# ==============================
def standardize_single_df(df, source, mapping):
    logger.info(f"Standardizing source: {source}")

    manual_map = mapping.get(source, {})
    auto_map = smart_map_columns(df)

    logger.info(f"{source} AUTO MAP: {auto_map}")

    # ✅ MERGE MAPPING (FIXED)
    map_config = merge_mapping(auto_map, manual_map, source)

    # HARD VALIDATION
    map_config = validate_mapping(map_config, df, source)

    # SEMANTIC VALIDATION
    semantic_validation(df, map_config, source)

    try:
        standardized_df = pd.DataFrame()

        # CORE
        standardized_df["order_id"] = safe_series(
            df, map_config.get("order_id"), "UNKNOWN_ID", str
        )

        standardized_df["date"] = parse_date(
            safe_series(df, map_config.get("date"))
        )

        standardized_df["product"] = safe_series(
            df, map_config.get("product"), "UNKNOWN_PRODUCT", str
        )

        standardized_df["price"] = pd.to_numeric(
            safe_series(df, map_config.get("price"), 0),
            errors="coerce"
        )

        standardized_df["quantity"] = pd.to_numeric(
            safe_series(df, map_config.get("quantity"), 0),
            errors="coerce"
        )

        standardized_df["revenue"] = pd.to_numeric(
            safe_series(df, map_config.get("revenue")),
            errors="coerce"
        )

        # CRITICAL CHECK
        if standardized_df["date"].isna().all():
            raise ValueError(f"{source}: CRITICAL all date invalid")

        if standardized_df["revenue"].isna().all():
            raise ValueError(f"{source}: CRITICAL revenue unusable")

        # CLEANING
        standardized_df = standardized_df.fillna({
            "price": 0,
            "quantity": 0,
            "revenue": 0,
            "product": "UNKNOWN_PRODUCT"
        })

        standardized_df = standardized_df[
            standardized_df["revenue"] >= 0
        ]

        # FEATURE ENGINEERING
        standardized_df["day_of_week"] = standardized_df["date"].dt.day_name()

        mean_rev = standardized_df["revenue"].mean()

        standardized_df["is_anomaly"] = (
            standardized_df["revenue"] > mean_rev * 5
            if mean_rev > 0 else False
        )

        standardized_df["source"] = source

        standardized_df = standardized_df.dropna(subset=["date"])
        standardized_df = standardized_df.sort_values("date")

        logger.info(f"{source}: SUCCESS ({len(standardized_df)} rows)")

        return standardized_df

    except Exception as e:
        logger.error(f"{source}: FAILED → {str(e)}", exc_info=True)
        return pd.DataFrame()


# ==============================
# STANDARDIZE ALL
# ==============================
def standardize_all(cleaned_dfs):
    mapping = load_mapping()
    standardized_dfs = []

    logger.info("Starting standardization process...")

    for i, df in enumerate(cleaned_dfs):

        if df is None:
            logger.warning(f"Dataset index {i} is None, skipped")
            continue

        if df.empty:
            logger.warning(f"Dataset index {i} is empty, skipped")
            continue

        if "source" not in df.columns:
            logger.error("Missing 'source' column in dataframe")
            continue

        source = df["source"].iloc[0]

        try:
            std_df = standardize_single_df(df.copy(), source, mapping)

            if not std_df.empty:
                standardized_dfs.append(std_df)
            else:
                logger.warning(f"{source}: no usable data")

        except Exception as e:
            logger.error(f"{source}: HARD FAIL → {str(e)}")

    logger.info(
        f"Standardization finished: {len(standardized_dfs)} datasets processed"
    )

    return standardized_dfs