import pandas as pd

REQUIRED_COLUMNS = {
    "shopee": ["order_id", "product_name", "price", "quantity", "order_date"],
    "tokopedia": ["invoice", "product", "total_price", "qty", "date"],
    "tiktok": ["id", "name", "price_per_item", "items_sold", "created_at"]
}


def validate_columns(df, source):
    expected_cols = REQUIRED_COLUMNS.get(source, [])
    missing_cols = [col for col in expected_cols if col not in df.columns]

    if missing_cols:
        print(f"❌ {source} missing columns: {missing_cols}")
    else:
        print(f"✅ {source} columns valid")

    return df


def validate_missing_values(df, source):
    missing = df.isnull().sum()
    missing = missing[missing > 0]

    if not missing.empty:
        print(f"⚠️ {source} missing values:\n{missing}")
    else:
        print(f"✅ {source} no missing values")

    return df


def validate_types(df, source):
    print(f"🔍 Checking data types for {source}")
    print(df.dtypes)
    return df


def validate_single_df(df, source):
    df = validate_columns(df, source)
    df = validate_missing_values(df, source)
    df = validate_types(df, source)
    return df


def validate_all(dfs):
    validated_dfs = []

    for df in dfs:
        if df.empty:
            continue

        source = df["source"].iloc[0]
        print(f"\n🚀 Validating {source.upper()}")

        validated_df = validate_single_df(df, source)
        validated_dfs.append(validated_df)

    return validated_dfs