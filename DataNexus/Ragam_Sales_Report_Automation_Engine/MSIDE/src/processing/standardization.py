import pandas as pd
from src.utils.config import load_mapping
from src.utils.logger import get_logger
from src.utils.column_mapper import smart_map_columns

logger = get_logger()


# ==============================
# 🔥 SAFE SERIES
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
# 🔥 SMART DATE PARSER (ANTI GAGAL TOTAL)
# ==============================
def parse_date(series):
    # coba normal parsing dulu
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)

    # kalau gagal total → coba format lain
    if parsed.isna().all():
        parsed = pd.to_datetime(series, errors="coerce", format="%Y-%m-%d")

    if parsed.isna().all():
        parsed = pd.to_datetime(series, errors="coerce", format="%d/%m/%Y")

    return parsed


# ==============================
# 🔥 VALIDATE MAPPING (ANTI SALAH KOLOM)
# ==============================
def validate_mapping(map_config, df, source):
    # ❌ jangan sampai date ambil id
    if map_config.get("date") in ["order_id", "id"]:
        logger.warning(f"{source}: invalid date column detected → reset")
        map_config["date"] = None

    # ❌ jangan sampai product ambil price
    if map_config.get("product") == map_config.get("price"):
        logger.warning(f"{source}: product = price detected → reset product")
        map_config["product"] = None

    return map_config


# ==============================
# 🔥 STANDARDIZE SINGLE DF
# ==============================
def standardize_single_df(df, source, mapping):
    logger.info(f"Standardizing source: {source}")

    manual_map = mapping.get(source, {})
    auto_map = smart_map_columns(df)

    logger.warning(f"{source} AUTO MAP: {auto_map}")

    # AUTO PRIORITY
    map_config = {**manual_map, **auto_map}

    # 🔥 VALIDASI
    map_config = validate_mapping(map_config, df, source)

    try:
        standardized_df = pd.DataFrame()

        # ==============================
        # 🔥 EXTRACTION
        # ==============================
        standardized_df["order_id"] = safe_series(
            df, map_config.get("order_id"), "UNKNOWN_ID", str
        )

        # ==============================
        # 🔥 DATE (SUPER ROBUST)
        # ==============================
        date_col = map_config.get("date")

        standardized_df["date"] = parse_date(
            safe_series(df, date_col, None)
        )

        if standardized_df["date"].isna().all():
            logger.warning(f"{source}: semua date gagal terbaca")

        # ==============================
        # 🔥 CORE DATA
        # ==============================
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

        # ==============================
        # 🔥 REVENUE (SMART FALLBACK)
        # ==============================
        revenue_col = map_config.get("revenue")

        if revenue_col and revenue_col in df.columns:
            standardized_df["revenue"] = pd.to_numeric(
                df[revenue_col], errors="coerce"
            )
        else:
            standardized_df["revenue"] = (
                standardized_df["price"] * standardized_df["quantity"]
            )

        # ==============================
        # 🔥 CLEANING
        # ==============================
        standardized_df["revenue"] = standardized_df["revenue"].fillna(0)

        standardized_df = standardized_df[
            standardized_df["revenue"] >= 0
        ]

        # ==============================
        # 🔥 FEATURE ENGINEERING
        # ==============================
        if standardized_df["date"].notna().sum() > 0:
            standardized_df["day_of_week"] = standardized_df["date"].dt.day_name()
        else:
            standardized_df["day_of_week"] = "UNKNOWN"

        # ==============================
        # 🔥 ANOMALY DETECTION
        # ==============================
        mean_rev = standardized_df["revenue"].mean()

        if pd.notnull(mean_rev) and mean_rev > 0:
            standardized_df["is_anomaly"] = (
                standardized_df["revenue"] > mean_rev * 5
            )
        else:
            standardized_df["is_anomaly"] = False

        # ==============================
        # 🔥 FINAL CLEAN
        # ==============================
        standardized_df = standardized_df.fillna({
            "price": 0,
            "quantity": 0,
            "revenue": 0,
            "product": "UNKNOWN_PRODUCT"
        })

        standardized_df["source"] = source

        # ==============================
        # 🔥 SAFE DATE FILTER
        # ==============================
        valid_dates = standardized_df["date"].notna().sum()

        if valid_dates > 0:
            standardized_df = standardized_df.dropna(subset=["date"])
        else:
            logger.warning(f"{source}: skip drop date (semua invalid)")

        # SORT
        if standardized_df["date"].notna().sum() > 0:
            standardized_df = standardized_df.sort_values("date")

        logger.info(f"{source}: success ({len(standardized_df)} rows)")

        return standardized_df

    except Exception:
        logger.error(f"Error standardizing {source}", exc_info=True)
        return pd.DataFrame()


# ==============================
# 🔥 STANDARDIZE ALL
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

        std_df = standardize_single_df(df.copy(), source, mapping)

        if std_df is not None and not std_df.empty:
            standardized_dfs.append(std_df)
        else:
            logger.warning(f"{source}: no usable data after standardization")

    logger.info(
        f"Standardization finished: {len(standardized_dfs)} datasets processed"
    )

    return standardized_dfs