import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def load_single_excel(file):

    try:

        file_path = file["path"]
        file_name = file["file_name"]

        df = pd.read_excel(file_path)

        print(f"Loaded Excel: {file_name}")

        return df

    except Exception as e:

        print(f"Failed loading {file.get('file_name', 'unknown')}: {e}")

        return None


def load_excel_files(excel_files):

    if not excel_files:
        return None

    dataframes = []

    with ThreadPoolExecutor(max_workers=4) as executor:

        results = executor.map(load_single_excel, excel_files)

        for df in results:

            if df is not None:
                dataframes.append(df)

    if dataframes:

        return pd.concat(dataframes, ignore_index=True)

    return None