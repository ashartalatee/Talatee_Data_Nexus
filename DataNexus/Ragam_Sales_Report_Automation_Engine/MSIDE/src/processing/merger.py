import pandas as pd
import os


# ==============================
# 🔥 UPGRADE 1: MERGE ALL DATA
# ==============================
def merge_data(standardized_dfs):
    print("🔗 Merging all datasets...")

    master_df = pd.concat(standardized_dfs, ignore_index=True)

    return master_df


# ==============================
# 🔥 UPGRADE 2: REMOVE DUPLICATES
# ==============================
def remove_duplicates(df):
    print("🧹 Removing duplicates...")

    before = len(df)

    df = df.drop_duplicates(
        subset=["date", "product", "price", "quantity", "source"]
    )

    after = len(df)

    print(f"📊 Removed duplicates: {before - after}")

    return df


# ==============================
# 🔥 UPGRADE 3: DATA INTEGRITY CHECK
# ==============================
def validate_master_data(df):
    print("🔍 Validating master dataset...")

    if df.empty:
        print("❌ Master dataset is empty!")
        return df

    if df["date"].isnull().any():
        print("⚠️ Warning: Missing date detected")

    if (df["revenue"] < 0).any():
        print("⚠️ Warning: Negative revenue detected")

    return df


# ==============================
# 🔥 UPGRADE 4: FINAL SORTING
# ==============================
def sort_master_data(df):
    print("📅 Sorting master dataset...")

    df = df.sort_values("date")

    return df


# ==============================
# 🔥 UPGRADE 5: SAVE OUTPUT
# ==============================
def save_master_data(df, path="data/output/master_data.csv"):
    print("💾 Saving master dataset...")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

    print(f"✅ Saved to {path}")


# ==============================
# MAIN MERGE PIPELINE
# ==============================
def merge_all(standardized_dfs):
    df = merge_data(standardized_dfs)

    df = remove_duplicates(df)

    df = validate_master_data(df)

    df = sort_master_data(df)

    save_master_data(df)

    return df