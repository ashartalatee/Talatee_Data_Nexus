import pandas as pd
import re


def clean_text(text, aggressive=False):
    """
    Clean text safely:
    - lowercase
    - trim
    - normalize spaces
    - optional aggressive cleaning
    """

    if pd.isna(text):
        return text

    text = str(text).lower().strip()

    # normalize spasi
    text = re.sub(r"\s+", " ", text)

    if aggressive:
        # hapus karakter non alphanumeric
        text = re.sub(r"[^a-z0-9\s]", "", text)

    return text


def text_cleaning_pipeline(df, config=None):
    """
    Text cleaning pipeline (production-ready):
    - Safe cleaning (tidak merusak makna)
    - Flexible mapping (multi client)
    - Normalisasi kategori (source & product)
    """

    if df is None or df.empty:
        print("[WARNING] Empty dataframe in text_cleaning_pipeline")
        return df

    # ========================
    # CONFIG
    # ========================
    text_columns = ["product_name", "source"]

    aggressive_clean = config.get("aggressive_clean", False) if config else False
    source_mapping = config.get("source_mapping", {}) if config else {}
    product_mapping = config.get("product_mapping", {}) if config else {}

    # default mapping (fallback)
    default_source_mapping = {
        "shopee indonesia": "shopee",
        "shopee mall": "shopee",
        "tokped": "tokopedia",
        "tiktok shop": "tiktok",
        "wa": "whatsapp"
    }

    # merge mapping
    source_mapping = {**default_source_mapping, **source_mapping}

    # ========================
    # 1. CLEAN TEXT
    # ========================
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: clean_text(x, aggressive=aggressive_clean))

    # ========================
    # 2. NORMALISASI SOURCE
    # ========================
    if "source" in df.columns:
        before_unique = df["source"].nunique()

        df["source"] = df["source"].replace(source_mapping)

        after_unique = df["source"].nunique()

        print(f"[INFO] Source normalized: {before_unique} -> {after_unique} unique values")

    # ========================
    # 3. NORMALISASI PRODUCT (SMART)
    # ========================
    if "product_name" in df.columns:

        # mapping manual dulu
        if product_mapping:
            df["product_name"] = df["product_name"].replace(product_mapping)

        # optional: normalisasi ringan (tanpa hancurin makna)
        df["product_name"] = df["product_name"].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # ========================
    # 4. INSIGHT HOOK (VALUE BISNIS)
    # ========================
    if "product_name" in df.columns:
        unique_products = df["product_name"].nunique()
        print(f"[INFO] Unique products after cleaning: {unique_products}")

    print("[INFO] Text cleaned (safe mode)" if not aggressive_clean else "[INFO] Text cleaned (aggressive mode)")

    return df