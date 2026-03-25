import pandas as pd

def clean_data(df):
    print("\n🧹 Cleaning data...")

    df = df.copy()

    # === 1. Drop Duplicate ===
    before = df.shape[0]
    df = df.drop_duplicates()
    after = df.shape[0]
    print(f"🧾 Removed duplicates: {before - after}")

    # === 2. Handle Missing Value ===
    df["price"] = df["price"].fillna(0)
    df["quantity"] = df["quantity"].fillna(1)

    # === 3. Normalize Text ===
    # Handle jika ada kolom product_name
    if "product_name" in df.columns:
        df["product_name"] = (
            df["product_name"]
            .astype(str)
            .str.lower()
            .str.strip()
        )

    # Handle jika ada kolom nama_produk (tokopedia)
    if "nama_produk" in df.columns:
        df["nama_produk"] = (
            df["nama_produk"]
            .astype(str)
            .str.lower()
            .str.strip()
        )

    print("✅ Cleaning selesai")

    return df