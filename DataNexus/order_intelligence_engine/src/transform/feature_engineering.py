import pandas as pd
import re


def clean_numeric(value):
    """
    Membersihkan format angka seperti:
    '50.000' → 50000
    '150k' → 150000
    """
    if pd.isna(value):
        return None

    value = str(value).lower().strip()

    # handle "k" (150k = 150000)
    if "k" in value:
        value = value.replace("k", "")
        try:
            return float(value) * 1000
        except:
            return None

    # remove titik & koma
    value = re.sub(r"[.,]", "", value)

    try:
        return float(value)
    except:
        return None


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    print("[TRANSFORM] Cleaning numeric columns...")

    if "quantity" in df.columns:
        df["quantity"] = df["quantity"].apply(clean_numeric)

    if "price" in df.columns:
        df["price"] = df["price"].apply(clean_numeric)

    return df


def create_revenue(df: pd.DataFrame) -> pd.DataFrame:
    print("[TRANSFORM] Creating revenue...")

    if "quantity" in df.columns and "price" in df.columns:
        df["revenue"] = df["quantity"] * df["price"]
    else:
        print("[WARNING] Missing columns for revenue")

    return df


def add_basic_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tambahan sederhana tapi powerful
    """
    print("[TRANSFORM] Adding basic metrics...")

    # flag order besar
    if "quantity" in df.columns:
        df["is_bulk_order"] = df["quantity"].apply(
            lambda x: 1 if x and x >= 3 else 0
        )

    return df


def transform_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[TRANSFORM] Starting transformation pipeline...")

    df = convert_data_types(df)
    df = create_revenue(df)
    df = add_basic_metrics(df)

    return df