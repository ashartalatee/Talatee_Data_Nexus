import pandas as pd

def daily_summary(df):
    print("📊 Aggregating daily summary...")

    # pastikan kolom ada
    if 'date' not in df.columns or 'revenue' not in df.columns:
        raise ValueError("Kolom 'date' atau 'revenue' tidak ditemukan")

    summary = df.groupby('date')['revenue'].sum().reset_index()

    return summary