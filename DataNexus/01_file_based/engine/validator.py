import pandas as pd


def validate_dataset(df):

    report = {}

    # total rows
    report["total_rows"] = len(df)

    # missing values per column
    report["missing_values"] = df.isnull().sum().to_dict()

    # total missing
    report["total_missing"] = int(df.isnull().sum().sum())

    # duplicate rows
    report["duplicate_rows"] = int(df.duplicated().sum())

    # column types
    report["column_types"] = df.dtypes.astype(str).to_dict()

    return report