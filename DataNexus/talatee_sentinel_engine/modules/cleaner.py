import pandas as pd

def clean_data(df):
    print("🧹 Cleaning & Standardizing data...")

    # =========================
    # 1. RENAME KOLOM
    # =========================
    column_mapping = {
        "order_id": "order_id",
        "invoice": "order_id",
        "id": "order_id",

        "product_name": "product",
        "product": "product",
        "nama_produk": "product",

        "qty": "quantity",
        "quantity": "quantity",
        "jumlah": "quantity",

        "price": "price",
        "total_price": "price",
        "harga": "price",

        "order_date": "date",
        "date": "date",
        "tanggal": "date",

        "stock": "stock",
        "stock_left": "stock",
        "stok": "stock"
    }

    df = df.rename(columns=column_mapping)

    # =========================
    # 2. FILTER KOLOM
    # =========================
    df = df[["order_id", "product", "quantity", "price", "date", "stock", "source"]]

    # =========================
    # 3. HANDLE MISSING VALUE
    # =========================
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0)

    # =========================
    # 4. STANDARDISASI TEXT
    # =========================
    df["product"] = df["product"].astype(str).str.lower().str.strip()

    # Mapping nama produk (opsional tapi powerful)
    product_mapping = {
        "kaos polos": "kaos polos",
        "kaospolos": "kaos polos",
        "kaos": "kaos polos"
    }

    df["product"] = df["product"].replace(product_mapping)

    # =========================
    # 5. HANDLE TANGGAL (LEVEL PRO)
    # =========================
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)

    # drop tanggal rusak
    df = df.dropna(subset=["date"])

    # =========================
    # 6. VALIDASI DATA BISNIS
    # =========================
    df = df[df["quantity"] > 0]
    df = df[df["price"] > 0]

    # =========================
    # 7. TAMBAHAN KOLOM PENTING
    # =========================
    df["revenue"] = df["quantity"] * df["price"]

    # =========================
    # 8. HAPUS DUPLIKAT
    # =========================
    df = df.drop_duplicates()

    print("✅ Data siap digunakan")

    return df