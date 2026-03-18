import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import logging
from config.settings import LOG_FILE

from engine.loader import load_bank_data, load_invoice_data
from engine.validator import validate_bank_data, validate_invoice_data
from engine.cleaner import clean_bank_data, clean_invoice_data
from engine.transformer import transform_bank_data, transform_invoice_data

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    print("Finance Reconciliation Engine Started")

    try:
        logging.info("=== ENGINE STARTED ===")

        # ========================
        # STEP 1: LOAD DATA
        # ========================
        logging.info("STEP 1: LOADING DATA")

        bank_df = load_bank_data()
        invoice_df = load_invoice_data()

        print("\n=== BANK DATA ===")
        print(bank_df.head())

        print("\n=== INVOICE DATA ===")
        print(invoice_df.head())

        # ========================
        # STEP 2: VALIDATION
        # ========================
        logging.info("STEP 2: VALIDATING DATA")

        validate_bank_data(bank_df)
        validate_invoice_data(invoice_df)

        print("\n✅ Validation Passed")
        logging.info("Validation completed successfully")

        # ========================
        # STEP 3: CLEANING
        # ========================
        logging.info("STEP 3: CLEANING DATA")

        bank_df = clean_bank_data(bank_df)
        invoice_df = clean_invoice_data(invoice_df)

        print("\n🧼 Cleaning Completed")

        print("\n=== CLEANED BANK DATA ===")
        print(bank_df.head())

        print("\n=== CLEANED INVOICE DATA ===")
        print(invoice_df.head())

        logging.info("Cleaning completed successfully")

        # STEP 4: TRANSFORM
        bank_df = transform_bank_data(bank_df)
        invoice_df = transform_invoice_data(invoice_df)

        print("\nTransformation Completed ✅")

        print(f"\nBank rows: {len(bank_df)}")
        print(f"Invoice rows: {len(invoice_df)}")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logging.error(f"Pipeline failed: {e}")

    finally:
        logging.info("=== ENGINE FINISHED ===")
        print("\nEngine Finished")


if __name__ == "__main__":
    main()