import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def load_single_csv(file):

    try:

        file_path = file["path"]
        file_name = file["file_name"]

        df = pd.read_csv(file_path)

        print(f"Loaded CSV: {file_name}")

        return df

    except Exception as e:

        print(f"Failed loading {file.get('file_name', 'unknown')}: {e}")

        return None


def load_csv_files(csv_files):

    if not csv_files:
        return None

    dataframes = []

    with ThreadPoolExecutor(max_workers=4) as executor:

        results = executor.map(load_single_csv, csv_files)

        for df in results:

            if df is not None:
                dataframes.append(df)

    if dataframes:

        return pd.concat(dataframes, ignore_index=True)

    return None