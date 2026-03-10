import sys
from pathlib import Path
import os
from datetime import datetime

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
        "csv": csv_files,
        "excel": excel_files,
        "pdf": pdf_files,
        "other": other_files
    }


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

    # CLASSIFY FILES
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


if __name__ == "__main__":
    main()