def standardize_data(df):
    print("\n🧩 Standardizing columns...")

    df = df.copy()

    # Mapping kolom
    column_mapping = {
        "invoice": "order_id",
        "nama_produk": "product_name",
        "kategori": "category",
        "harga": "price",
        "jumlah": "quantity",
        "tanggal": "order_date",
        "biaya_admin": "fee"
    }

    # Rename kolom yang cocok
    df = df.rename(columns=column_mapping)

    # Pastikan semua kolom utama ada
    required_columns = [
        "order_id",
        "product_name",
        "category",
        "price",
        "quantity",
        "order_date",
        "fee",
        "source"
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    # Reorder kolom biar rapi
    df = df[required_columns]

    print("✅ Standardization selesai")

    return df