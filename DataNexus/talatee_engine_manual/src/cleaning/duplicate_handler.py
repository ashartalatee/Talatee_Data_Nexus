import pandas as pd


def duplicate_pipeline(df, config=None):
    """
    Handle duplicate data:
    - Drop full duplicate rows
    - Drop duplicate berdasarkan key columns (prioritized)
    - Keep latest data jika ada kolom timestamp/date
    """

    if df is None or df.empty:
        print("[WARNING] Empty dataframe in duplicate_pipeline")
        return df

    initial_rows = len(df)

    # ========================
    # CONFIG (FLEXIBLE)
    # ========================
    default_keys = ["order_id", "product_name", "date"]

    key_columns = config.get("deduplicate_keys", default_keys) if config else default_keys
    sort_column = config.get("sort_by") if config else None
    keep_strategy = config.get("keep", "last") if config else "last"

    # ========================
    # 1. DROP FULL DUPLICATES
    # ========================
    df = df.drop_duplicates()

    after_full_drop = len(df)

    # ========================
    # 2. HANDLE SORTING (IMPORTANT)
    # ========================
    if sort_column and sort_column in df.columns:
        df = df.sort_values(by=sort_column)

    # ========================
    # 3. DROP DUPLICATE BASED ON KEY COLUMNS
    # ========================
    available_keys = [col for col in key_columns if col in df.columns]

    if available_keys:
        before_subset = len(df)

        df = df.drop_duplicates(
            subset=available_keys,
            keep=keep_strategy  # "first" / "last"
        )

        after_subset = len(df)

        print(f"[INFO] Duplicate (subset) removed: {before_subset - after_subset}")
        print(f"[INFO] Keys used: {available_keys}")
        print(f"[INFO] Keep strategy: {keep_strategy}")

    else:
        print("[WARNING] No valid key columns found for deduplication")

    final_rows = len(df)

    # ========================
    # FINAL LOG
    # ========================
    print(f"[INFO] Duplicate removed (total): {initial_rows - final_rows}")
    print(f"[INFO] Rows after deduplication: {final_rows}")

    return df