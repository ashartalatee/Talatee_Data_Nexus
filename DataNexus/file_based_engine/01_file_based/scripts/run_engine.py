import sys
from pathlib import Path
import pandas as pd
import time

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config.settings import (
    INPUT_CSV,
    INPUT_EXCEL,
    INPUT_PDF,
    OUTPUT_DATA,
    PROCESSED_FILES,
    PIPELINE_REPORT
)

from engine.logger import setup_logger
from engine.scheduler import run_scheduler

from engine.file_scanner import scan_folder
from engine.file_classifier import classify_files
from engine.csv_loader import load_csv_files
from engine.excel_loader import load_excel_files
from engine.data_validator import validate_dataset
from engine.data_cleaner import clean_dataset
from engine.data_storage import save_dataset

from engine.file_tracker import (
    load_processed_files,
    update_processed_files
)

from engine.pipeline_reporter import save_pipeline_report


def run_pipeline():

    logger = setup_logger()

    start_time = time.time()

    # =========================
    # PIPELINE METRICS
    # =========================

    files_processed = 0
    rows_processed = 0
    duplicates_removed = 0
    status = "SUCCESS"

    print("\nFILE DATA AUTOMATION ENGINE\n")
    logger.info("ENGINE STARTED")

    folders = [
        INPUT_CSV,
        INPUT_EXCEL,
        INPUT_PDF
    ]

    all_files = []

    try:

        # =========================
        # LOAD FILE HISTORY
        # =========================

        processed_files = load_processed_files(PROCESSED_FILES)

        # =========================
        # SCAN FILES
        # =========================

        for folder in folders:

            print(f"Scanning: {folder}")
            logger.info(f"Scanning folder: {folder}")

            files = scan_folder(folder)
            all_files.extend(files)

        if not all_files:

            print("\nNo files found.")
            logger.warning("No files found")
            return

        # =========================
        # FILTER NEW FILES
        # =========================

        new_files = [
            f for f in all_files
            if f["file_name"] not in processed_files
        ]

        if not new_files:

            print("\nNo new files to process.")
            logger.info("No new files detected")
            return

        print("\nNEW FILES FOUND:", len(new_files))
        logger.info(f"New files detected: {len(new_files)}")

        # =========================
        # CLASSIFY FILES
        # =========================

        classified = classify_files(new_files)

        print("\nCSV FILES:", len(classified["csv"]))
        print("EXCEL FILES:", len(classified["excel"]))
        print("PDF FILES:", len(classified["pdf"]))

        logger.info(f"CSV files: {len(classified['csv'])}")
        logger.info(f"Excel files: {len(classified['excel'])}")
        logger.info(f"PDF files: {len(classified['pdf'])}")

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

        if not datasets:

            print("\nNo dataset available.")
            logger.warning("No dataset available")
            return

        # =========================
        # MERGE DATASETS
        # =========================

        unified_df = pd.concat(datasets, ignore_index=True)

        print("\nUNIFIED DATA PREVIEW\n")
        print(unified_df.head())

        print("\nTOTAL ROWS:", len(unified_df))
        logger.info(f"Unified dataset rows: {len(unified_df)}")

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

        # =========================
        # DATA STORAGE
        # =========================

        print("\nSAVING CLEAN DATASET...\n")

        output_file = save_dataset(cleaned_df, OUTPUT_DATA)

        print("Dataset saved to:")
        print(output_file)

        logger.info(f"Dataset saved to {output_file}")

        # =========================
        # UPDATE METRICS
        # =========================

        files_processed = len(new_files)
        rows_processed = len(cleaned_df)
        duplicates_removed = cleaning_report["duplicates_removed"]

        # =========================
        # UPDATE FILE HISTORY
        # =========================

        update_processed_files(new_files, PROCESSED_FILES)

        logger.info("Processed files metadata updated")

    except Exception as e:

        status = "FAILED"

        logger.error(f"ENGINE ERROR: {str(e)}")
        print("Engine failed:", e)

    finally:

        end_time = time.time()
        runtime = round(end_time - start_time, 2)

        print(f"\nENGINE FINISHED in {runtime} seconds")
        logger.info(f"Engine finished in {runtime} seconds")

        # =========================
        # SAVE PIPELINE REPORT
        # =========================

        report_data = {
            "files_processed": files_processed,
            "rows_processed": rows_processed,
            "duplicates_removed": duplicates_removed,
            "runtime_seconds": runtime,
            "status": status
        }

        report_file = save_pipeline_report(
            PIPELINE_REPORT,
            report_data
        )

        print("\nPIPELINE REPORT SAVED:")
        print(report_file)

        logger.info(f"Pipeline report saved: {report_file}")


def main():

    INTERVAL_SECONDS = 300

    run_scheduler(run_pipeline, INTERVAL_SECONDS)


if __name__ == "__main__":
    main()