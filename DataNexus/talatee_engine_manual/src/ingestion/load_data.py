import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

def load_all_data(data_sources: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    """
    Load semua data dari berbagai source:
        - csv
        - excel
        - folder csv

    Args:
        data_sources (list of dict): 
            Contoh: [
                {"name": "client_a", "path": "data/client_a.csv", "type": "csv", "active": True, "options": {}}
            ]

    Returns:
        pd.DataFrame atau None jika tidak ada data
    """
    all_data: List[pd.DataFrame] = []

    for source in data_sources:
        if not source.get("active", True):
            continue

        name: str = source.get("name", "unknown")
        path: str = source.get("path")
        dtype: str = source.get("type")
        options: Dict[str, Any] = source.get("options", {})

        if not path:
            print(f"[WARNING] No path defined for source: {name}")
            continue

        try:
            path_obj = Path(path)

            if not path_obj.exists():
                print(f"[WARNING] Path not found: {path}")
                continue

            # ========================
            # LOAD DATA
            # ========================
            if dtype == "csv":
                df = pd.read_csv(path_obj, **options)
            elif dtype == "excel":
                df = pd.read_excel(path_obj, **options)
            elif dtype == "csv_folder":
                df = load_csv_folder(path_obj, options)
            else:
                print(f"[WARNING] Unknown type: {dtype}")
                continue

            if df is None or df.empty:
                print(f"[WARNING] Empty data: {path}")
                continue

            # ========================
            # METADATA
            # ========================
            df["source"] = name
            df["ingested_at"] = pd.Timestamp.now()

            all_data.append(df)
            print(f"[INFO] Loaded: {path} | Rows: {len(df)}")

        except Exception as e:
            print(f"[ERROR] Failed load {path}: {e}")

    if not all_data:
        print("[WARNING] No data loaded from all sources")
        return None

    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"[INFO] Total rows combined: {len(combined_df)}")
    return combined_df


def load_csv_folder(folder_path: Union[str, Path], options: Dict[str, Any]) -> pd.DataFrame:
    """
    Load semua file CSV dalam 1 folder

    Args:
        folder_path (str or Path): folder berisi CSV
        options (dict): pd.read_csv options

    Returns:
        pd.DataFrame: gabungan semua file
    """
    folder_path = Path(folder_path)
    all_files = list(folder_path.glob("*.csv"))

    if not all_files:
        print(f"[WARNING] No CSV files in folder: {folder_path}")
        return pd.DataFrame()

    data: List[pd.DataFrame] = []

    for file in all_files:
        try:
            df = pd.read_csv(file, **options)
            if df is None or df.empty:
                print(f"[WARNING] Empty file skipped: {file}")
                continue

            df["file_name"] = file.name  # track asal file
            data.append(df)
            print(f"[INFO] Loaded file: {file} | Rows: {len(df)}")

        except Exception as e:
            print(f"[ERROR] Failed file {file}: {e}")

    if not data:
        return pd.DataFrame()

    combined = pd.concat(data, ignore_index=True)
    print(f"[INFO] Total rows from folder: {len(combined)}")
    return combined