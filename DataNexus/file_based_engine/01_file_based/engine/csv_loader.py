import pandas as pd


def load_csv_files(csv_files):

    dataframes = []

    for file in csv_files:

        try:

            df = pd.read_csv(file["file_path"])

            df["source_file"] = file["file_name"]

            dataframes.append(df)

            print(f"Loaded CSV: {file['file_name']}")

        except Exception as e:

            print(f"Failed loading {file['file_name']} : {e}")

    if not dataframes:
        return None

    return pd.concat(dataframes, ignore_index=True)