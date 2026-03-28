import pandas as pd

def daily_summary(df):
    print("\n📊 DAILY SUMMARY (FINAL)")

    # ===== VALIDASI KOLOM =====
    required_cols = ['date', 'revenue', 'quantity']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Kolom '{col}' tidak ditemukan")

    # ===== PASTIKAN FORMAT =====
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    # ===== HAPUS DATA INVALID =====
    df = df.dropna(subset=['date', 'revenue', 'quantity'])

    # ===== AGGREGATION =====
    summary = (
        df.groupby('date')
        .agg({
            'revenue': 'sum',
            'quantity': 'sum'
        })
        .reset_index()
        .sort_values(by='date')
    )

    print("✅ Daily summary berhasil dibuat")
    print(summary.head())

    return summary