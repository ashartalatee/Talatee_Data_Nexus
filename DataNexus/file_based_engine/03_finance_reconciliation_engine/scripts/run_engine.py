import sys
from pathlib import Path
import logging
import time
import os

# ================= PATH SETUP =================
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

# ================= IMPORT =================
from config.settings import CONFIG

from engine.loader import load_data
from engine.validator import validate_bank_data, validate_invoice_data
from engine.cleaner import clean_bank_data, clean_invoice_data
from engine.transformer import transform_bank_data, transform_invoice_data
from engine.merger import merge_data, apply_reconciliation_logic
from engine.reporter import generate_report, save_report, generate_summary
from engine.data_quality import run_all_checks

from config.errors import (
    DataValidationError,
    DataCleaningError,
    DataTransformationError,
    DataMergeError,
    ReportGenerationError
)

# ================= LOGGING SETUP =================

log_dir = CONFIG["paths"]["log_dir"]
log_file = CONFIG["logging"]["log_file"]

os.makedirs(log_dir, exist_ok=True)

LOG_PATH = log_dir / log_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("FinanceEngine")

# ================= PIPELINE STEPS =================

def run_ingestion():
    logger.info("STEP 1: INGESTION STARTED")

    bank_df, invoice_df = load_data()

    run_all_checks(bank_df, "BANK DATA", critical_columns=["amount"])
    run_all_checks(invoice_df, "INVOICE DATA", critical_columns=["amount"])

    return bank_df, invoice_df


def run_validation(bank_df, invoice_df):
    logger.info("STEP 2: VALIDATION STARTED")

    validate_bank_data(bank_df)
    validate_invoice_data(invoice_df)

    logger.info("VALIDATION COMPLETED")


def run_cleaning(bank_df, invoice_df):
    logger.info("STEP 3: CLEANING STARTED")

    bank_df = clean_bank_data(bank_df)
    invoice_df = clean_invoice_data(invoice_df)

    logger.info(f"Cleaned bank shape: {bank_df.shape}")
    logger.info(f"Cleaned invoice shape: {invoice_df.shape}")

    return bank_df, invoice_df


def run_transformation(bank_df, invoice_df):
    logger.info("STEP 4: TRANSFORMATION STARTED")

    bank_df = transform_bank_data(bank_df)
    invoice_df = transform_invoice_data(invoice_df)

    logger.info("TRANSFORMATION COMPLETED")

    return bank_df, invoice_df


def run_merge(bank_df, invoice_df):
    logger.info("STEP 5: MERGE STARTED")

    merged_df = merge_data(invoice_df, bank_df)

    logger.info(f"Merged data shape: {merged_df.shape}")

    return merged_df


def run_reconciliation(merged_df):
    logger.info("STEP 6: RECONCILIATION STARTED")

    df = apply_reconciliation_logic(merged_df)

    logger.info("RECONCILIATION COMPLETED")

    return df


def run_reporting(df):
    logger.info("STEP 7: REPORTING STARTED")

    report_df = generate_report(df)

    generate_summary(report_df)
    save_report(report_df)

    logger.info("REPORTING COMPLETED")


# ================= MAIN ENGINE =================

def main():
    print(" Finance Reconciliation Engine Started")

    mode = CONFIG["engine"]["mode"]
    logger.info(f"ENGINE MODE: {mode}")

    start_time = time.time()

    try:
        # ================= PIPELINE FLOW =================
        bank_df, invoice_df = run_ingestion()

        run_validation(bank_df, invoice_df)

        bank_df, invoice_df = run_cleaning(bank_df, invoice_df)

        bank_df, invoice_df = run_transformation(bank_df, invoice_df)

        merged_df = run_merge(bank_df, invoice_df)

        reconciled_df = run_reconciliation(merged_df)

        run_reporting(reconciled_df)

    # ================= ERROR HANDLING =================

    except DataValidationError as e:
        logger.error(f"[VALIDATION ERROR] {e}")
        print(f" Validation Error: {e}")

    except DataCleaningError as e:
        logger.error(f"[CLEANING ERROR] {e}")
        print(f" Cleaning Error: {e}")

    except DataTransformationError as e:
        logger.error(f"[TRANSFORMATION ERROR] {e}")
        print(f" Transformation Error: {e}")

    except DataMergeError as e:
        logger.error(f"[MERGE ERROR] {e}")
        print(f" Merge Error: {e}")

    except ReportGenerationError as e:
        logger.error(f"[REPORT ERROR] {e}")
        print(f" Report Error: {e}")

    except Exception as e:
        logger.error(f"[UNKNOWN ERROR] {e}")
        print(f" Unexpected Error: {e}")

    finally:
        end_time = time.time()
        execution_time = end_time - start_time

        logger.info(f"Execution Time: {execution_time:.2f} seconds")
        print(f"⏱ Execution Time: {execution_time:.2f} seconds")

    print(" Engine Finished")


if __name__ == "__main__":
    main()