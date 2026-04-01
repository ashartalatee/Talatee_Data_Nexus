# =========================
#  ORDER INTELLIGENCE ENGINE (FINAL PRO)
# =========================

from .ingestion.load_data import load_all_data

from .cleaning.standardize import standardization_pipeline
from .cleaning.missing_handler import missing_pipeline
from .cleaning.duplicate_handler import duplicate_pipeline
from .cleaning.text_cleaner import text_cleaning_pipeline

from .transform.feature_engineering import transform_pipeline

from .analysis.metrics import (
    generate_report,
    get_top_products,
    generate_summary
)

from .output.exporter import export_to_csv
from .utils.logger import setup_logger

from .config.settings import (
    APP_NAME,
    VERSION,
    DATA_SOURCES
)

from datetime import datetime
from pathlib import Path
import pandas as pd


# =========================
#  MAIN ENGINE FUNCTION
# =========================

def run_engine():
    logger = setup_logger("main")

    try:
        logger.info(f"Starting {APP_NAME} v{VERSION}")

        # =========================
        # STEP 1: LOAD DATA
        # =========================
        df = load_all_data(DATA_SOURCES)

        if df is None or df.empty:
            logger.error("No data loaded. Stopping pipeline.")
            return

        logger.info(f"Data loaded | Total Rows: {len(df)}")

        # LOG PER SOURCE
        if "source" in df.columns:
            for src in df["source"].unique():
                count = len(df[df["source"] == src])
                logger.info(f"{src} -> {count} rows")

        # =========================
        # STEP 2–6: PIPELINE
        # =========================
        df = standardization_pipeline(df)
        df = missing_pipeline(df)
        df = duplicate_pipeline(df)
        df = text_cleaning_pipeline(df)
        df = transform_pipeline(df)

        if "revenue" not in df.columns or df["revenue"].isna().all():
            logger.error("Revenue invalid")
            return

        logger.info("Data pipeline completed")

        # =========================
        # STEP 7: ANALYTICS
        # =========================
        summary = generate_summary(df)
        generate_report(df)
        top_products_df = get_top_products(df)

        logger.info(f"Revenue: {summary['total_revenue']:,.0f}")
        logger.info(f"Orders: {summary['total_orders']}")

        # =========================
        # STEP 8: EXPORT
        # =========================
        logger.info("Exporting outputs...")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # 1. EXPORT PER SOURCE
        if "source" in df.columns:
            for src in df["source"].unique():
                df_src = df[df["source"] == src]
                path = output_dir / f"final_{src}_{ts}.csv"
                export_to_csv(df_src, path)
                logger.info(f"{src} -> {path}")

        # 2. EXPORT GABUNGAN
        combined_path = output_dir / f"final_semua_{ts}.csv"
        export_to_csv(df, combined_path)
        logger.info(f"combined -> {combined_path}")

        # 3. EXCEL MULTI SHEET
        excel_path = output_dir / f"report_{ts}.xlsx"

        with pd.ExcelWriter(excel_path) as writer:
            df.to_excel(writer, sheet_name="ALL_DATA", index=False)

            if "source" in df.columns:
                for src in df["source"].unique():
                    df[df["source"] == src].to_excel(
                        writer,
                        sheet_name=src[:31],
                        index=False
                    )

            top_products_df.to_excel(
                writer,
                sheet_name="TOP_PRODUCTS",
                index=False
            )

        logger.info(f"excel_report -> {excel_path}")

        logger.info("PIPELINE SUCCESS")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")


# =========================
#  ENTRY POINT
# =========================

if __name__ == "__main__":
    run_engine()