import pandas as pd

def validate_data(df):
    print("\n🔍 Validating data...")

    df = df.copy()

    # Tandai error awal
    df["is_error"] = False
    df["error_reason"] = ""

    # === RULE 1: Missing Value ===
    required_columns = ["product_name", "price", "quantity"]

    for col in required_columns:
        mask = df[col].isna()
        df.loc[mask, "is_error"] = True
        df.loc[mask, "error_reason"] += f"{col} missing; "

    # === RULE 2: Tipe Data (paksa numeric) ===
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    # Kalau setelah convert jadi NaN → berarti error
    for col in ["price", "quantity"]:
        mask = df[col].isna()
        df.loc[mask, "is_error"] = True
        df.loc[mask, "error_reason"] += f"{col} invalid; "

    # === Split Data ===
    clean_df = df[df["is_error"] == False]
    error_df = df[df["is_error"] == True]

    print(f"✅ Clean data: {clean_df.shape[0]} rows")
    print(f"❌ Error data: {error_df.shape[0]} rows")

    return clean_df, error_df