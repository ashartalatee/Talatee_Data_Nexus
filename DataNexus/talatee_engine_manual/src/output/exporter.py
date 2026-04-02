from pathlib import Path
from datetime import datetime


def export_to_csv(df, path, config=None):
    """
    Export dataframe ke CSV (production-ready):
    - Auto create folder
    - Optional timestamp filename
    - Handle empty dataframe
    - Flexible config (delimiter, encoding)
    """

    if df is None or df.empty:
        print("[WARNING] Empty dataframe, nothing exported")
        return None

    path = Path(path)

    # ========================
    # CONFIG
    # ========================
    add_timestamp = config.get("add_timestamp", False) if config else False
    delimiter = config.get("delimiter", ",") if config else ","
    encoding = config.get("encoding", "utf-8") if config else "utf-8"

    # ========================
    # HANDLE TIMESTAMP FILE NAME
    # ========================
    if add_timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = path.with_name(f"{path.stem}_{timestamp}{path.suffix}")

    # ========================
    # CREATE DIRECTORY
    # ========================
    path.parent.mkdir(parents=True, exist_ok=True)

    # ========================
    # EXPORT
    # ========================
    try:
        df.to_csv(
            path,
            index=False,
            sep=delimiter,
            encoding=encoding
        )

        print(f"[INFO] CSV exported successfully")
        print(f"[INFO] File: {path.name}")
        print(f"[INFO] Rows: {len(df)}")

        return path

    except Exception as e:
        print("[ERROR] Failed to export CSV")
        print(f"[ERROR] {str(e)}")
        return None