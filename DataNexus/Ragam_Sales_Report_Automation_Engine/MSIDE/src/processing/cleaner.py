import pandas as pd


# ==============================
# 🔥 UPGRADE 1: LOGGING PERUBAHAN
# ==============================
def log_cleaning_changes(before, after, source):
    print(f"📊 {source} rows before: {len(before)}, after: {len(after)}")


# ==============================
# CLEANING FUNCTIONS
# ==============================

def clean_missing_values(df, source):
    print(f"🧹 Cleaning missing values: {source}")

    # 🔥 UPGRADE 2: DROP DATA PARAH
    df = df.dropna(thresh=int(len(df.columns) * 0.5))

    # Fill numeric kosong dengan median
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].fillna(df[col].median())

    # Fill string kosong
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna("Unknown")

    return df


def clean_dates(df, source):
    print(f"📅 Standardizing date: {source}")

    date_columns = ["order_date", "date", "created_at"]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df


def clean_category(df, source):
    print(f"🏷️ Cleaning category: {source}")

    if "category" in df.columns:
        df["category"] = df["category"].str.lower().str.strip()

        # Fix typo umum
        df["category"] = df["category"].replace({
            "fashon": "fashion",
            "gadjet": "gadget"
        })

    return df


def clean_numeric(df, source):
    print(f"🔢 Cleaning numeric: {source}")

    for col in df.columns:
        if "price" in col or "qty" in col or "quantity" in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# ==============================
# 🔥 UPGRADE 3: NORMALIZE TEXT
# ==============================
def normalize_text(df, source):
    print(f"📝 Normalizing text: {source}")

    if "product_name" in df.columns:
        df["product_name"] = df["product_name"].str.strip().str.lower()

    if "product" in df.columns:
        df["product"] = df["product"].str.strip().str.lower()

    if "name" in df.columns:
        df["name"] = df["name"].str.strip().str.lower()

    return df


# ==============================
# 🔥 UPGRADE 4: CLEAN PIPELINE WRAPPER
# ==============================
def clean_single_df(df, source):
    before = df.copy()

    df = clean_missing_values(df, source)
    df = clean_dates(df, source)
    df = clean_category(df, source)
    df = clean_numeric(df, source)
    df = normalize_text(df, source)

    log_cleaning_changes(before, df, source)

    return df


# ==============================
# MAIN CLEAN FUNCTION
# ==============================
def clean_all(validated_dfs):
    cleaned_dfs = []

    for df in validated_dfs:
        if df.empty:
            continue

        source = df["source"].iloc[0]
        print(f"\n🚀 Cleaning {source.upper()}")

        cleaned_df = clean_single_df(df, source)
        cleaned_dfs.append(cleaned_df)

    return cleaned_dfs