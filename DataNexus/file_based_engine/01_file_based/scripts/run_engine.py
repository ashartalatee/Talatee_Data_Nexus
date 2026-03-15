import sys
from pathlib import Path
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import INPUT_CSV, INPUT_EXCEL, INPUT_PDF

from engine.file_scanner import scan_folder
from engine.file_classifier import classify_files
from engine.csv_loader import load_csv_files
from engine.excel_loader import load_excel_files
from engine.data_validator import validate_dataset
from engine.data_cleaner import clean_dataset
from engine.data_storage import save_dataset
from config.settings import OUTPUT_DATA

def main():

    print("\nFILE DATA AUTOMATION ENGINE\n")

    folders = [
        INPUT_CSV,
        INPUT_EXCEL,
        INPUT_PDF
    ]

    all_files = []

    # =========================
    # SCAN FILES
    # =========================

    for folder in folders:

        print(f"Scanning: {folder}")

        files = scan_folder(folder)

        all_files.extend(files)

    if not all_files:

        print("\nNo files found.")
        return

    print("\nTotal Files Found:", len(all_files))

    # =========================
    # CLASSIFY FILES
    # =========================

    classified = classify_files(all_files)

    print("\nCSV FILES:", len(classified["csv"]))
    print("EXCEL FILES:", len(classified["excel"]))
    print("PDF FILES:", len(classified["pdf"]))

    # =========================
    # LOAD DATA
    # =========================

    csv_df = load_csv_files(classified["csv"])
    excel_df = load_excel_files(classified["excel"])

    datasets = []

    if csv_df is not None:
        datasets.append(csv_df)

    if excel_df is not None:
        datasets.append(excel_df)

    # =========================
    # MERGE DATASETS
    # =========================

    if datasets:

        unified_df = pd.concat(datasets, ignore_index=True)

        print("\nUNIFIED DATA PREVIEW\n")
        print(unified_df.head())

        print("\nTOTAL ROWS:", len(unified_df))

        # =========================
        # DATA VALIDATION
        # =========================

        print("\nRUNNING DATA VALIDATION...\n")

        report = validate_dataset(unified_df)

        print("DATA QUALITY REPORT")
        print("-------------------")
        print("Total Rows:", report["total_rows"])
        print("Total Missing:", report["total_missing"])
        print("Duplicate Rows:", report["duplicate_rows"])

        print("\nMissing Values Per Column:")

        for col, val in report["missing_values"].items():
            print(f"{col}: {val}")

        print("\nColumn Types:")

        for col, dtype in report["column_types"].items():
            print(f"{col}: {dtype}")

        # =========================
        # DATA CLEANING
        # =========================

        print("\nRUNNING DATA CLEANING...\n")

        cleaned_df, cleaning_report = clean_dataset(unified_df)

        print("CLEANING REPORT")
        print("-------------------")
        print("Rows Before:", cleaning_report["rows_before"])
        print("Rows After :", cleaning_report["rows_after"])
        print("Duplicates Removed:", cleaning_report["duplicates_removed"])

        print("\nCLEANED DATA PREVIEW\n")

        print(cleaned_df.head())
        

    else:

        print("\nNo dataset available.")

        # =========================
        # DATA STORAGE
        # =========================

        print("\nSAVING CLEAN DATASET...\n")

        output_file = save_dataset(cleaned_df, OUTPUT_DATA)

        print("Dataset saved to:")
        print(output_file)


if __name__ == "__main__":
    main()