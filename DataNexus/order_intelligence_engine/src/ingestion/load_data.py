# src/ingestion/load_data.py

import pandas as pd
import os
from config.settings import DATA_SOURCES  # nanti kita pakai config

def load_single_source(source):
    """
    Load data dari satu source
    Mendukung type: csv, excel, folder
    """
    try:
        if source["type"] == "csv":
            df = pd.read_csv(source["path"])

        elif source["type"] == "excel":
            df = pd.read_excel(source["path"])

        elif source["type"] == "csv_folder":
            # load semua csv di folder
            all_files = []
            for file in os.listdir(source["path"]):
                if file.endswith(".csv"):
                    full_path = os.path.join(source["path"], file)
                    tmp = pd.read_csv(full_path)
                    tmp["source_file"] = file
                    all_files.append(tmp)
            if all_files:
                df = pd.concat(all_files, ignore_index=True)
            else:
                print(f"[WARNING] No CSV found in folder: {source['path']}")
                return pd.DataFrame()

        else:
            raise ValueError(f"Unsupported type: {source['type']}")

        # Tambahkan kolom source
        df["source"] = source["name"]

        print(f"[INFO] Loaded: {source['path']} | Shape: {df.shape}")
        return df

    except Exception as e:
        print(f"[ERROR] Failed to load {source['path']}: {e}")
        return pd.DataFrame()


def load_all_data():
    """
    Load semua data sesuai config DATA_SOURCES
    """
    all_df = []

    for source in DATA_SOURCES:
        df = load_single_source(source)
        if not df.empty:
            all_df.append(df)

    if not all_df:
        print("[WARNING] No data loaded from any source!")
        return pd.DataFrame()

    combined_df = pd.concat(all_df, ignore_index=True)
    print(f"[INFO] Combined dataframe shape: {combined_df.shape}")
    return combined_df


def handle_missing(df):
    """
    Handle missing values secara dasar:
    - drop rows dengan kolom penting missing
    - fill source jika kosong
    """
    before = len(df)

    # drop mandatory columns
    df = df.dropna(subset=["product_name", "quantity", "price"])

    after = len(df)
    print(f"[INFO] Dropped {before - after} rows due to missing values")

    # fill source jika kosong
    if "source" in df.columns:
        df["source"] = df["source"].fillna("unknown")

    return df