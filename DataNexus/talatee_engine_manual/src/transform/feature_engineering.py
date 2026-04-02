import pandas as pd
from typing import Optional, Dict, Any

def transform_pipeline(df: pd.DataFrame, config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """
    Transform data menjadi business-ready:
        - Hitung revenue
        - Validasi numeric
        - Buat metric tambahan (avg_price, is_valid_order)
        - Siap untuk summary & insight

    Args:
        df (pd.DataFrame): dataframe input
        config (dict, optional): tambahan konfigurasi (future use)

    Returns:
        pd.DataFrame: dataframe yang sudah di-transform
    """
    if df is None or df.empty:
        print("[WARNING] Empty dataframe in transform_pipeline")
        return pd.DataFrame()

    df = df.copy()

    # ========================
    # 1. VALIDASI & CONVERT NUMERIC
    # ========================
    for col in ["quantity", "price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    # ========================
    # 2. HITUNG REVENUE (CORE)
    # ========================
    df["revenue"] = df["quantity"] * df["price"]

    # ========================
    # 3. DERIVED METRICS
    # ========================
    df["avg_price"] = df.apply(
        lambda row: row["revenue"] / row["quantity"] if row["quantity"] > 0 else 0,
        axis=1
    )

    # ========================
    # 4. BASIC BUSINESS FLAGS
    # ========================
    df["is_valid_order"] = df["revenue"] > 0

    # ========================
    # 5. LOGGING SUMMARY
    # ========================
    total_revenue = df["revenue"].sum()
    zero_revenue_rows = (df["revenue"] == 0).sum()

    print(f"[INFO] Transform completed | Rows: {len(df)}")
    print(f"[INFO] Total revenue: {total_revenue}")
    if zero_revenue_rows > 0:
        print(f"[WARNING] {zero_revenue_rows} rows with revenue = 0")

    return df