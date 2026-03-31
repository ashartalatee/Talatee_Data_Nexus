# src/cleaning/missing_handler.py

def check_missing(df):
    print("\n[INFO] Missing Values:")
    print(df.isnull().sum())

def handle_missing(df):
    # Drop data kritis
    df = df.dropna(subset=["product_name", "quantity", "price"])

    # Isi default jika perlu
    if "source" in df.columns:
        df["source"] = df["source"].fillna("unknown")

    return df