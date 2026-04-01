# =========================
# 🧹 TEXT CLEANING MODULE (PRODUCTION READY)
# =========================

import pandas as pd
import re

# ✅ IMPORT CONFIG
from config.settings import CLEANING_CONFIG

# =========================
# 🧽 BASIC TEXT CLEANING
# =========================

def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """
    Membersihkan text dasar:
    - lowercase
    - strip whitespace
    - hapus spasi ganda
    """

    if "product_name" not in df.columns:
        return df

    series = df["product_name"].astype(str)

    # =========================
    # LOWERCASE
    # =========================
    if CLEANING_CONFIG.get("lowercase", True):
        series = series.str.lower()

    # =========================
    # STRIP WHITESPACE
    # =========================
    if CLEANING_CONFIG.get("strip_whitespace", True):
        series = series.str.strip()

    # =========================
    # REMOVE MULTIPLE SPACES
    # =========================
    series = series.str.replace(r"\s+", " ", regex=True)

    # =========================
    # TEXT REPLACEMENTS (CONFIG)
    # =========================
    for key, val in CLEANING_CONFIG.get("text_replacements", {}).items():
        series = series.str.replace(key, val, regex=False)

    df["product_name"] = series

    return df


# =========================
# 🔄 NORMALIZE PRODUCT NAME
# =========================

def normalize_product_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalisasi nama produk berdasarkan mapping
    """

    if "product_name" not in df.columns:
        return df

    series = df["product_name"]

    # =========================
    # STATIC NORMALIZATION (CONFIG EXTENDABLE)
    # =========================
    normalization_map = CLEANING_CONFIG.get(
        "product_normalization_map",
        {
            "kaos hitam ": "kaos hitam",
            "kaos  hitam": "kaos hitam",
            "sepatu sport ": "sepatu sport",
        }
    )

    series = series.replace(normalization_map)

    # =========================
    # REMOVE TRAILING SPACES AGAIN (SAFETY)
    # =========================
    series = series.str.strip()

    df["product_name"] = series

    return df


# =========================
# 🚀 FULL TEXT PIPELINE
# =========================

def text_cleaning_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline khusus text cleaning
    """

    df = clean_text(df)
    df = normalize_product_name(df)

    return df