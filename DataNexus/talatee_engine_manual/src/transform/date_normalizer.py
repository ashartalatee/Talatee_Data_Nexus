import pandas as pd
def normalize_date_column(df, column, logger=None):
    df = df.copy()

    df[column] = pd.to_datetime(df[column], errors="coerce")

    if logger:
        logger.info(f"[DATE] Normalized '{column}'")

    return df