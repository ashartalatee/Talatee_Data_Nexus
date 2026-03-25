import pandas as pd

def transform_data(df):
    print("\n🔄 Transforming data...")

    df = df.copy()

    # === 1. HANDLE KOLOM TANGGAL ===

    # Shopee → order_date
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Tokopedia → tanggal
    if "tanggal" in df.columns:
        df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce", dayfirst=True)
        df["order_date"] = df["tanggal"]  # samakan nama

    # === 2. PASTIKAN NUMERIC ===
    df["price"] = pd.to_numeric(df.get("price", df.get("harga")), errors="coerce")
    df["quantity"] = pd.to_numeric(df.get("quantity", df.get("jumlah")), errors="coerce")

    # === 3. HANDLE FEE ===
    if "fee" not in df.columns and "biaya_admin" in df.columns:
        df["fee"] = df["biaya_admin"]

    df["fee"] = pd.to_numeric(df["fee"], errors="coerce").fillna(0)

    # === 4. SAMAKAN NAMA PRODUK ===
    if "product_name" not in df.columns and "nama_produk" in df.columns:
        df["product_name"] = df["nama_produk"]

    print("✅ Transformation selesai")

    return df