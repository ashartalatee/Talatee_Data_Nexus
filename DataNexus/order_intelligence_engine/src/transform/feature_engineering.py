# =========================
# ⚙️ FEATURE ENGINEERING PIPELINE (PRODUCTION READY)
# =========================

import pandas as pd
import re

# ✅ IMPORT CONFIG
from config.settings import CLEANING_CONFIG


# =========================
# 🔢 NUMERIC CLEANER
# =========================

def clean_numeric(value):
    """
    Membersihkan berbagai format angka:
    '50.000' → 50000
    '150k' → 150000
    'Rp 25,000' → 25000
    """

    if pd.isna(value):
        return None

    value = str(value).lower().strip()

    # =========================
    # TEXT REPLACEMENT (CONFIG DRIVEN)
    # =========================
    for key, val in CLEANING_CONFIG.get("text_replacements", {}).items():
        value = value.replace(key, val)

    # =========================
    # HANDLE "k" FORMAT
    # =========================
    if "k" in value:
        value = value.replace("k", "")
        try:
            return float(value) * 1000
        except:
            return None

    # =========================
    # REMOVE NON NUMERIC
    # =========================
    value = re.sub(r"[^\d]", "", value)

    try:
        return float(value)
    except:
        return None


# =========================
# 🔄 CONVERT DATA TYPES
# =========================

def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    print("[TRANSFORM] Cleaning numeric columns...")

    for col in ["quantity", "price"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric)

    return df


# =========================
# 💰 CREATE REVENUE
# =========================

def create_revenue(df: pd.DataFrame) -> pd.DataFrame:
    print("[TRANSFORM] Creating revenue...")

    if "quantity" in df.columns and "price" in df.columns:
        df["revenue"] = df["quantity"] * df["price"]
    else:
        print("[WARNING] Missing columns for revenue")

    return df


# =========================
# 📊 ADD BUSINESS METRICS
# =========================

def add_basic_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tambahan sederhana tapi bernilai bisnis
    """
    print("[TRANSFORM] Adding basic metrics...")

    # =========================
    # BULK ORDER FLAG
    # =========================
    if "quantity" in df.columns:
        df["is_bulk_order"] = df["quantity"].apply(
            lambda x: 1 if pd.notna(x) and x >= 3 else 0
        )

    # =========================
    # HIGH VALUE ORDER FLAG
    # =========================
    if "revenue" in df.columns:
        df["is_high_value"] = df["revenue"].apply(
            lambda x: 1 if pd.notna(x) and x >= 100000 else 0
        )

    return df


# =========================
# 🧪 FINAL VALIDATION
# =========================

def validate_transformation(df: pd.DataFrame):
    print("[TRANSFORM] Validating transformation output...")

    if "revenue" not in df.columns:
        raise ValueError("Revenue column missing after transformation")

    if df["revenue"].isna().all():
        raise ValueError("All revenue values are NaN")

    return True


# =========================
# 🚀 MAIN PIPELINE
# =========================

def transform_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[TRANSFORM] Starting transformation pipeline...")

    # STEP 1: TYPE CLEANING
    df = convert_data_types(df)

    # STEP 2: FEATURE CREATION
    df = create_revenue(df)

    # STEP 3: BUSINESS METRICS
    df = add_basic_metrics(df)

    # STEP 4: VALIDATION
    validate_transformation(df)

    print("[TRANSFORM] Pipeline completed successfully")

    return df