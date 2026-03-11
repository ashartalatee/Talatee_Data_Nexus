import sys
from pathlib import Path
import os
from datetime import datetime
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import INPUT_CSV, INPUT_EXCEL, INPUT_PDF


def scan_folder(folder_path):

    files_data = []
    folder = Path(folder_path)

    if not folder.exists():
        return files_data

    for file in folder.glob("*"):

        if not file.is_file():
            continue

        filename = file.name.lower()

        # ignore temporary / lock files
        if filename.startswith("~") or filename.startswith(".~") or "lock" in filename:
            continue

        metadata = {
            "file_name": file.name,
            "file_type": file.suffix.lower(),
            "file_path": str(file.resolve()),
            "file_size": os.path.getsize(file),
            "created_time": datetime.fromtimestamp(
                os.path.getctime(file)
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }

        files_data.append(metadata)

    return files_data


def classify_files(files):

    csv_files = []
    excel_files = []
    pdf_files = []
    other_files = []

    for file in files:

        file_type = file["file_type"]

        if file_type == ".csv":
            csv_files.append(file)

        elif file_type in [".xlsx", ".xls"]:
            excel_files.append(file)

        elif file_type == ".pdf":
            pdf_files.append(file)

        else:
            other_files.append(file)

    return {
        "csv": sorted(csv_files, key=lambda x: x["file_name"]),
        "excel": sorted(excel_files, key=lambda x: x["file_name"]),
        "pdf": sorted(pdf_files, key=lambda x: x["file_name"]),
        "other": sorted(other_files, key=lambda x: x["file_name"])
    }


def load_csv_files(csv_files):

    dataframes = []

    for file in csv_files:

        path = file["file_path"]

        try:

            df = pd.read_csv(path)

            df["source_file"] = file["file_name"]

            dataframes.append(df)

            print(f"Loaded CSV: {file['file_name']}")

        except Exception as e:

            print(f"Failed loading {file['file_name']} : {e}")

    if not dataframes:
        return None

    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df


def main():

    print("\nFILE DATA AUTOMATION ENGINE")
    print("Scanning input directories...\n")

    folders = [
        INPUT_CSV,
        INPUT_EXCEL,
        INPUT_PDF
    ]

    all_files = []

    for folder in folders:

        print(f"Scanning: {folder}")

        files = scan_folder(folder)

        all_files.extend(files)

    if not all_files:
        print("\nNo valid files found.")
        return

    print("\nTotal Files Found:", len(all_files))

    classified = classify_files(all_files)

    print("\nCSV FILES:")
    for f in classified["csv"]:
        print(f" - {f['file_name']}")

    print("\nEXCEL FILES:")
    for f in classified["excel"]:
        print(f" - {f['file_name']}")

    print("\nPDF FILES:")
    for f in classified["pdf"]:
        print(f" - {f['file_name']}")

    if classified["other"]:
        print("\nOTHER FILES:")
        for f in classified["other"]:
            print(f" - {f['file_name']}")

    print("\nSUMMARY")
    print("CSV   :", len(classified["csv"]))
    print("Excel :", len(classified["excel"]))
    print("PDF   :", len(classified["pdf"]))
    print("Other :", len(classified["other"]))

    # LOAD CSV DATA
    if classified["csv"]:

        print("\nLOADING CSV DATA...\n")

        df = load_csv_files(classified["csv"])

        if df is not None:

            print("\nDATA PREVIEW\n")
            print(df.head())

            print("\nTOTAL ROWS:", len(df))

        else:

            print("No CSV data loaded.")


if __name__ == "__main__":
    main()