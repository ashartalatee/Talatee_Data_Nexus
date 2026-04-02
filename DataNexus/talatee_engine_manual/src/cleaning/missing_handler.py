import pandas as pd


def missing_pipeline(df, config=None):
    """
    Handle missing values (production-ready):
    - Numeric: convert & optional fill
    - Text: fill 'unknown'
    - Date: drop invalid
    - Revenue: NEVER auto fill (must be calculated later)
    """

    if df is None or df.empty:
        print("[WARNING] Empty dataframe in missing_pipeline")
        return df

    initial_rows = len(df)

    # ========================
    # CONFIG (FLEXIBLE)
    # ========================
    numeric_cols = ["quantity", "price"]
    text_cols = ["product_name", "source"]
    date_col = "date"

    fill_numeric = config.get("fill_numeric", True) if config else True
    fill_value = config.get("fill_value", 0) if config else 0

    # ========================
    # 1. HANDLE NUMERIC
    # ========================
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

            missing_before = df[col].isna().sum()

            if fill_numeric:
                df[col] = df[col].fillna(fill_value)

            missing_after = df[col].isna().sum()

            print(f"[INFO] Numeric '{col}' | missing before: {missing_before} -> after: {missing_after}")

    # ========================
    # 2. HANDLE TEXT
    # ========================
    for col in text_cols:
        if col in df.columns:
            missing_before = df[col].isna().sum()

            df[col] = df[col].fillna("unknown")

            print(f"[INFO] Text '{col}' filled: {missing_before}")

    # ========================
    # 3. HANDLE DATE
    # ========================
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        invalid_dates = df[date_col].isna().sum()

        df = df.dropna(subset=[date_col])

        print(f"[INFO] Invalid dates dropped: {invalid_dates}")

    # ========================
    # 4. WARNING CHECK (CRITICAL)
    # ========================
    if "price" in df.columns:
        zero_price = (df["price"] == 0).sum()
        if zero_price > 0:
            print(f"[WARNING] Found {zero_price} rows with price = 0")

    final_rows = len(df)

    # ========================
    # FINAL LOG
    # ========================
    print(f"[INFO] Missing handled | Rows dropped: {initial_rows - final_rows}")
    print(f"[INFO] Rows after missing handling: {final_rows}")

    return df