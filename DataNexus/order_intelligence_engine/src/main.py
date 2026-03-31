# src/main.py

from ingestion.load_data import load_all_data
from cleaning.text_cleaner import clean_text, normalize_product_name
from cleaning.standardize import (
    normalize_column_names,
    standardize_columns,
    validate_columns,
    enforce_schema
)
from cleaning.missing_handler import check_missing, handle_missing
from cleaning.duplicate_handler import check_duplicates, remove_duplicates_subset

from transform.feature_engineering import transform_pipeline
from analysis.metrics import generate_report, get_top_products

from output.exporter import export_to_csv, export_to_excel
from utils.logger import setup_logger


def main():
    logger = setup_logger()

    try:
        logger.info("Starting Order Intelligence Engine v1")

        # =========================
        # STEP 1: LOAD DATA
        # =========================
        df = load_all_data()

        if df.empty:
            logger.error("No data loaded. Stopping pipeline.")
            return

        logger.info(f"Data loaded | Rows: {len(df)}")

        # =========================
        # STEP 2: STANDARDIZATION
        # =========================
        df = normalize_column_names(df)
        df = standardize_columns(df)
        df = validate_columns(df)
        df = enforce_schema(df)

        logger.info(f"Columns after standardization: {df.columns.tolist()}")

        # =========================
        # STEP 3: MISSING HANDLING
        # =========================
        check_missing(df)
        df = handle_missing(df)

        logger.info("Missing values handled")

        # =========================
        # STEP 4: DUPLICATE HANDLING
        # =========================
        check_duplicates(df)
        df = remove_duplicates_subset(df)

        logger.info(f"Rows after deduplication: {len(df)}")

        # =========================
        # STEP 5: TEXT CLEANING
        # =========================
        df = clean_text(df)
        df = normalize_product_name(df)

        logger.info("Text cleaning completed")

        # =========================
        # STEP 6: TRANSFORMATION
        # =========================
        df = transform_pipeline(df)

        # 🔍 VALIDATION KRITIS
        if "revenue" not in df.columns:
            logger.error("Revenue column missing after transform")
            return

        if df["revenue"].isna().all():
            logger.error("All revenue values are NaN. Check transformation logic.")
            return

        logger.info("Transformation completed successfully")

        # =========================
        # STEP 7: ANALYTICS
        # =========================
        print("\n[STEP] Generating business report...")
        generate_report(df)

        top_products_df = get_top_products(df)

        logger.info("Analytics generated")

        # =========================
        # STEP 8: EXPORT
        # =========================
        print("\n[STEP] Exporting results...")
        export_to_csv(df)
        export_to_excel(df, top_products_df)

        logger.info("Export completed successfully")

        # =========================
        # STEP 9: DEBUG SNAPSHOT
        # =========================
        print("\n[DEBUG] Sample Data:")
        print(df.head())

        print("\n[DEBUG] Data Types:")
        print(df.dtypes)

        logger.info("Pipeline finished successfully")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()