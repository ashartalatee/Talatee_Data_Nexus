import pandas as pd


def clean_dataset(df):

    cleaned_df = df.copy()

    rows_before = len(cleaned_df)

    # =========================
    # REMOVE DUPLICATES
    # =========================

    cleaned_df = cleaned_df.drop_duplicates()

    rows_after = len(cleaned_df)

    duplicates_removed = rows_before - rows_after

    # =========================
    # NORMALIZE COLUMN NAMES
    # =========================

    cleaned_df.columns = [
        col.strip().lower().replace(" ", "_")
        for col in cleaned_df.columns
    ]

    # =========================
    # HANDLE MISSING VALUES
    # =========================

    for column in cleaned_df.columns:

        if cleaned_df[column].dtype == "object":

            cleaned_df[column] = cleaned_df[column].fillna("unknown")

        else:

            cleaned_df[column] = cleaned_df[column].fillna(0)

    report = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_removed": duplicates_removed
    }

    return cleaned_df, report