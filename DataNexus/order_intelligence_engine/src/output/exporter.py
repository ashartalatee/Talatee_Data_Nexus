# src/output/exporter.py

import pandas as pd
import os
from datetime import datetime


def ensure_output_folder():
    if not os.path.exists("output"):
        os.makedirs("output")


def export_to_csv(df: pd.DataFrame):
    ensure_output_folder()

    filename = f"output/clean_data.csv"
    df.to_csv(filename, index=False)

    print(f"[EXPORT] CSV saved → {filename}")


def export_to_excel(df: pd.DataFrame, top_products_df: pd.DataFrame):
    ensure_output_folder()

    filename = "output/report.xlsx"

    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Clean Data", index=False)
        top_products_df.to_excel(writer, sheet_name="Top Products", index=False)

    print(f"[EXPORT] Excel report saved → {filename}")


def generate_summary_dict(df):
    return {
        "total_revenue": df["revenue"].sum(),
        "total_orders": len(df),
        "avg_order_value": df["revenue"].mean()
    }