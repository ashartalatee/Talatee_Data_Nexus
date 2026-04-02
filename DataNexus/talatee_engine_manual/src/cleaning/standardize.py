import pandas as pd
import re


def standardization_pipeline(df, config=None):
    """
    Standardization pipeline (production-ready):
    - Normalize column names (clean & safe)
    - Map ke schema baku (flexible)
    - Clean string values (tanpa merusak null)
    - Tambah kolom wajib
    """

    if df is None or df.empty:
        print("[WARNING] Empty dataframe in standardization_pipeline")
        return df

    # ========================
    # CONFIG (FLEXIBLE)
    # ========================
    default_mapping = {
        # product
        "produk": "product_name",
        "nama_produk": "product_name",
        "product": "product_name",

        # quantity
        "qty": "quantity",
        "jumlah": "quantity",

        # price
        "harga": "price",
        "price_per_item": "price",

        # date
        "tanggal": "date",
        "order_date": "date",

        # order id
        "id_pesanan": "order_id",
        "orderid": "order_id",

        # source
        "platform": "source",
        "channel": "source"
    }

    column_mapping = config.get("column_mapping", default_mapping) if config else default_mapping

    required_columns = ["product_name", "quantity", "price", "date", "source"]

    # ========================
    # 1. NORMALIZE COLUMN NAME (ADVANCED)
    # ========================
    def clean_column(col):
        col = col.strip().lower()
        col = re.sub(r"[^\w]+", "_", col)   # ganti semua non-alphanumeric jadi _
        col = re.sub(r"_+", "_", col)       # hindari __
        return col.strip("_")

    df.columns = [clean_column(col) for col in df.columns]

    # ========================
    # 2. RENAME KE SCHEMA BAKU
    # ========================
    df = df.rename(columns=column_mapping)

    # ========================
    # 3. CLEAN STRING VALUE (AMAN)
    # ========================
    object_cols = df.select_dtypes(include="object").columns

    for col in object_cols:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # ========================
    # 4. TAMBAH KOLOM WAJIB
    # ========================
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
            print(f"[WARNING] Column '{col}' missing -> created with None")

    # ========================
    # 5. STANDARDIZE SOURCE
    # ========================
    if "source" in df.columns:
        df["source"] = df["source"].apply(
            lambda x: x.lower() if isinstance(x, str) else x
        )

    # ========================
    # 6. VALIDATION CHECK (IMPORTANT)
    # ========================
    missing_required = [col for col in required_columns if col not in df.columns]

    if missing_required:
        print(f"[ERROR] Missing required columns after standardization: {missing_required}")
    else:
        print(f"[INFO] All required columns available")

    print(f"[INFO] Columns standardized: {list(df.columns)}")

    return df