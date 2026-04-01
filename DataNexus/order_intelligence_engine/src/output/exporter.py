# =========================
# 📤 EXPORT MODULE (FINAL - PRODUCTION READY)
# =========================

import pandas as pd
from pathlib import Path


# =========================
# 📁 ENSURE OUTPUT FOLDER
# =========================

def ensure_output_folder(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


# =========================
# 📊 EXPORT TO CSV
# =========================

def export_to_csv(df: pd.DataFrame, path):
    """
    Export DataFrame ke CSV
    Tidak handle timestamp → harus dari main.py
    """

    path = Path(path)
    ensure_output_folder(path)

    df.to_csv(path, index=False)

    return str(path)


# =========================
# 📈 EXPORT TO EXCEL
# =========================

def export_to_excel(
    df: pd.DataFrame,
    top_products_df: pd.DataFrame,
    path
):
    """
    Export ke Excel multi-sheet
    """

    path = Path(path)
    ensure_output_folder(path)

    summary = generate_summary_dict(df)

    with pd.ExcelWriter(path) as writer:
        # SHEET 1: ALL DATA
        df.to_excel(writer, sheet_name="ALL_DATA", index=False)

        # SHEET 2: TOP PRODUCTS
        if top_products_df is not None:
            top_products_df.to_excel(
                writer,
                sheet_name="TOP_PRODUCTS",
                index=False
            )

        # SHEET 3: SUMMARY
        summary_df = pd.DataFrame([summary])
        summary_df.to_excel(writer, sheet_name="SUMMARY", index=False)

    return str(path)


# =========================
# 📊 SUMMARY GENERATOR
# =========================

def generate_summary_dict(df: pd.DataFrame) -> dict:
    """
    Summary bisnis sederhana tapi bernilai tinggi
    """

    total_revenue = df["revenue"].sum() if "revenue" in df.columns else 0
    total_orders = len(df)
    avg_order_value = df["revenue"].mean() if "revenue" in df.columns else 0

    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "avg_order_value": avg_order_value,
    }