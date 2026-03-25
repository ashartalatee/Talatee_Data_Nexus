import pandas as pd

def top_products(df):
    print("🧠 Analyzing top products...")

    # pastikan kolom ada
    if 'product' not in df.columns or 'revenue' not in df.columns:
        raise ValueError("Kolom 'product' atau 'revenue' tidak ditemukan")

    result = (
        df.groupby('product')['revenue']
        .sum()
        .sort_values(ascending=False)
    )

    return result