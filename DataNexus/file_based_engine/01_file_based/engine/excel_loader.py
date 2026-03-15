import pandas as pd


def load_excel_files(excel_files):

    dataframes = []

    for file in excel_files:

        try:

            sheets = pd.read_excel(file["file_path"], sheet_name=None)

            for sheet_name, df in sheets.items():

                df["source_file"] = file["file_name"]
                df["source_sheet"] = sheet_name

                dataframes.append(df)

                print(f"Loaded Excel: {file['file_name']} | Sheet: {sheet_name}")

        except Exception as e:

            print(f"Failed loading {file['file_name']} : {e}")

    if not dataframes:
        return None

    return pd.concat(dataframes, ignore_index=True)