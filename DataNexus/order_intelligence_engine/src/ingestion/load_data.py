import pandas as pd
import os

def load_csv(path):
    try:
        df = pd.read_csv(path)
        print(f"[INFO] Loaded: {path} | Shape: {df.shape}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load {path}: {e}")
        return pd.DataFrame()

def load_all_data():
    base_path = "data/raw"

    files = {
        "shopee": "shopee.csv",
        "tokopedia": "tokopedia.csv"
    }

    df_list = []

    for source, filename in files.items():
        path = os.path.join(base_path, filename)
        df = load_csv(path)

        if not df.empty:
            df["source"] = source
            df_list.append(df)

    if not df_list:
        print("[WARNING] No data loaded!")
        return pd.DataFrame()

    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df