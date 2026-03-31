# src/cleaning/duplicate_handler.py

import pandas as pd


def check_duplicates(df: pd.DataFrame) -> None:
    """
    Mengecek jumlah exact duplicate (semua kolom sama)
    """
    duplicate_count = df.duplicated().sum()
    print(f"[INFO] Total exact duplicates: {duplicate_count}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Menghapus exact duplicate (semua kolom sama)
    """
    before = len(df)

    df = df.drop_duplicates()

    after = len(df)

    print(f"[INFO] Removed {before - after} exact duplicate rows")

    return df


def remove_duplicates_subset(
    df: pd.DataFrame,
    subset=None
) -> pd.DataFrame:
    """
    Menghapus logical duplicate berdasarkan kolom penting
    Default: product_name, quantity, price
    """

    if subset is None:
        subset = ["product_name", "quantity", "price"]

    # Validasi kolom
    missing_cols = [col for col in subset if col not in df.columns]

    if missing_cols:
        print(f"[WARNING] Missing columns for deduplication: {missing_cols}")
        print("[INFO] Skipping duplicate removal (subset)")
        return df

    before = len(df)

    df = df.drop_duplicates(subset=subset)

    after = len(df)

    print(f"[INFO] Removed {before - after} logical duplicate rows (subset={subset})")

    return df