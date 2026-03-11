import pandas as pd


class CSVLoader:

    def __init__(self, csv_files):
        self.csv_files = csv_files

    def load(self):

        dataframes = []

        for file in self.csv_files:

            path = file["file_path"]

            try:
                df = pd.read_csv(path)

                df["source_file"] = file["file_name"]

                dataframes.append(df)

                print(f"Loaded: {file['file_name']}")

            except Exception as e:

                print(f"Error loading {file['file_name']} : {e}")

        if len(dataframes) == 0:
            return None

        combined_df = pd.concat(dataframes, ignore_index=True)

        return combined_df