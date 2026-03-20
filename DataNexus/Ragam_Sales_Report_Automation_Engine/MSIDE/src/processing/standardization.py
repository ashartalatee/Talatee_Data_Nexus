import pandas as pd
from src.utils.config import load_mapping


# ==============================
# CORE STANDARDIZATION FUNCTION
# ==============================

def standardize_single_df(df, source, mapping):
    print(f"🧩 Standardizing {source}")

    if source not in mapping:
        print(f"❌ No mapping found for {source}")
        return pd.DataFrame()

    map_config = mapping[source]

    try:
        standardized_df = pd.DataFrame()

        standardized_df["date"] = pd.to_datetime(
            df[map_config["date"]], errors='coerce'
        )

        standardized_df["product"] = df[map_config["product"]].astype(str)

        standardized_df["price"] = pd.to_numeric(
            df[map_config["price"]], errors='coerce'
        )

        standardized_df["quantity"] = pd.to_numeric(
            df[map_config["quantity"]], errors='coerce'
        )

        # 🔥 CORE METRIC
        standardized_df["revenue"] = (
            standardized_df["price"] * standardized_df["quantity"]
        )

        # ==============================
        # 🔥 UPGRADE 1: CLEAN NEGATIVE
        # ==============================
        standardized_df = standardized_df[
            standardized_df["revenue"] >= 0
        ]

        # ==============================
        # 🔥 UPGRADE 2: BUSINESS FEATURE
        # ==============================
        standardized_df["day_of_week"] = standardized_df["date"].dt.day_name()

        # ==============================
        # 🔥 UPGRADE 3: ANOMALY DETECTION
        # ==============================
        mean_rev = standardized_df["revenue"].mean()

        if pd.notnull(mean_rev) and mean_rev != 0:
            standardized_df["is_anomaly"] = (
                standardized_df["revenue"] > mean_rev * 5
            )
        else:
            standardized_df["is_anomaly"] = False

        # ==============================
        # 🔥 UPGRADE 4: FINAL CLEAN
        # ==============================
        standardized_df = standardized_df.fillna({
            "price": 0,
            "quantity": 0,
            "revenue": 0,
            "product": "unknown"
        })

        standardized_df["source"] = source

        # ==============================
        # 🔥 UPGRADE 5: SORT
        # ==============================
        standardized_df = standardized_df.sort_values("date")

        return standardized_df

    except Exception as e:
        print(f"❌ Error standardizing {source}: {e}")
        return pd.DataFrame()


# ==============================
# MULTI STANDARDIZATION
# ==============================

def standardize_all(cleaned_dfs):
    mapping = load_mapping()
    standardized_dfs = []

    for df in cleaned_dfs:
        if df.empty:
            continue

        source = df["source"].iloc[0]

        std_df = standardize_single_df(df.copy(), source, mapping)

        if not std_df.empty:
            standardized_dfs.append(std_df)

    return standardized_dfs