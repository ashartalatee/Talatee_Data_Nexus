# src/cleaning/standardize.py

def standardize_columns(df):
    column_mapping = {
        # product name
        "nama_barang": "product_name",
        "item": "product_name",
        "produk": "product_name",

        # quantity
        "qty": "quantity",
        "jumlah": "quantity",
        "qyt": "quantity",

        # price
        "harga": "price",
        "price": "price"
    }

    # rename kolom
    df = df.rename(columns=column_mapping)

    return df

def validate_columns(df):
    required_columns = ["product_name", "quantity", "price"]

    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        print(f"[WARNING] Missing columns: {missing}")

    return df