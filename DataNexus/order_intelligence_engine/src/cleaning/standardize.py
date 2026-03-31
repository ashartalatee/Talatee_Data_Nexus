# src/cleaning/standardize.py

def normalize_column_names(df):
    """
    Normalisasi nama kolom:
    - lowercase
    - hapus spasi berlebih
    - ganti spasi jadi underscore
    """
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )
    return df


def standardize_columns(df):
    """
    Mapping + MERGE kolom dari berbagai source menjadi schema standar
    (ANTI duplicate column & ANTI data hilang)
    """

    column_groups = {
        "product_name": ["nama_barang", "item", "produk"],
        "quantity": ["qty", "jumlah", "qyt"],
        "price": ["harga", "price"]
    }

    new_df = df.copy()

    for new_col, old_cols in column_groups.items():
        available_cols = [col for col in old_cols if col in new_df.columns]

        if available_cols:
            # 🔥 Ambil nilai pertama yang tidak null dari beberapa kolom
            new_df[new_col] = new_df[available_cols].bfill(axis=1).iloc[:, 0]

    return new_df


def validate_columns(df):
    """
    Validasi apakah kolom wajib tersedia
    """
    required_columns = ["product_name", "quantity", "price"]

    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        print(f"[WARNING] Missing columns: {missing}")
    else:
        print("[INFO] All required columns are present")

    return df


def enforce_schema(df):
    """
    Ambil hanya kolom penting + pastikan tidak ada duplicate column
    """
    expected_columns = ["product_name", "quantity", "price", "source"]

    # ambil kolom yang ada saja
    existing = [col for col in expected_columns if col in df.columns]

    df = df[existing]

    # 🔥 safety: hilangkan duplicate column kalau masih ada
    df = df.loc[:, ~df.columns.duplicated()]

    return df