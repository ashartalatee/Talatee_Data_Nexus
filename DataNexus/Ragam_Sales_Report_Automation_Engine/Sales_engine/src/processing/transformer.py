import pandas as pd


# ==============================
# STANDARD FIELD EXTRACTION
# ==============================

def extract_price(df):
    for col in df.columns:
        if "price" in col:
            return pd.to_numeric(df[col], errors='coerce')
    return pd.Series([0] * len(df))


def extract_quantity(df):
    for col in df.columns:
        if "qty" in col or "quantity" in col or "items_sold" in col:
            return pd.to_numeric(df[col], errors='coerce')
    return pd.Series([1] * len(df))


def extract_date(df):
    for col in ["order_date", "date", "created_at"]:
        if col in df.columns:
            return pd.to_datetime(df[col], errors='coerce')
    return pd.Series([pd.NaT] * len(df))


def extract_product(df):
    for col in ["product_name", "product", "name"]:
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series(["unknown"] * len(df))


# ==============================
# TRANSFORMATION LOGIC
# ==============================

def transform_single_df(df, source):
    print(f"🔄 Transforming {source}")

    # Extract standard fields
    df["price"] = extract_price(df)
    df["quantity"] = extract_quantity(df)
    df["date"] = extract_date(df)
    df["product"] = extract_product(df)

    # 🔥 CORE METRIC
    df["revenue"] = df["price"] * df["quantity"]

    # ==============================
    # 🔥 UPGRADE 1: HANDLE NEGATIVE / ERROR DATA
    # ==============================
    df = df[df["revenue"] >= 0]

    # ==============================
    # 🔥 UPGRADE 2: BUSINESS FEATURE
    # ==============================
    df["day_of_week"] = df["date"].dt.day_name()

    # ==============================
    # 🔥 UPGRADE 3: ANOMALY DETECTION
    # ==============================
    mean_revenue = df["revenue"].mean()

    if pd.notnull(mean_revenue) and mean_revenue != 0:
        df["is_anomaly"] = df["revenue"] > mean_revenue * 5
    else:
        df["is_anomaly"] = False

    # ==============================
    # 🔥 UPGRADE 4: SORT DATA (ANALYTICS READY)
    # ==============================
    df = df.sort_values("date")

    # ==============================
    # 🔥 UPGRADE 5: FILL FINAL NULL (BIAR AMAN)
    # ==============================
    df["price"] = df["price"].fillna(0)
    df["quantity"] = df["quantity"].fillna(0)
    df["revenue"] = df["revenue"].fillna(0)
    df["product"] = df["product"].fillna("unknown")

    return df[[
        "date",
        "day_of_week",
        "product",
        "price",
        "quantity",
        "revenue",
        "is_anomaly",
        "source"
    ]]


# ==============================
# MULTI DATA TRANSFORM
# ==============================

def transform_all(cleaned_dfs):
    transformed_dfs = []

    for df in cleaned_dfs:
        if df.empty:
            continue

        source = df["source"].iloc[0]
        transformed_df = transform_single_df(df.copy(), source)
        transformed_dfs.append(transformed_df)

    return transformed_dfs