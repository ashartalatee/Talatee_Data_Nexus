import os

from src.utils.config import load_config
from src.utils.logger import get_logger

from src.ingestion.loader import load_all_data
from src.processing.validator import validate_all
from src.processing.cleaner import clean_all
from src.processing.standardization import standardize_all
from src.processing.merger import merge_all

from src.analytics.analytics import run_analytics
from src.alert.alert import run_alerts
from src.intelligence.intelligence_engine import run_intelligence


def main(mode="normal"):
    # ==============================
    # LOGGER INIT
    # ==============================
    logger = get_logger(mode)

    logger.warning("🚀 MSIDE Engine Starting...\n")

    try:
        # ==============================
        # LOAD CONFIG
        # ==============================
        config = load_config()

        # ==============================
        # DATA PIPELINE
        # ==============================
        dfs = load_all_data(config)
        validated = validate_all(dfs)
        cleaned = clean_all(validated)
        standardized = standardize_all(cleaned)

        master_df = merge_all(standardized)

        logger.warning("📊 Data processed")

        # ==============================
        # ANALYTICS
        # ==============================
        analytics = run_analytics(master_df)

        logger.warning("📈 Analytics generated")

        # ==============================
        # ALERT ENGINE
        # ==============================
        alerts = run_alerts(master_df, analytics["daily_revenue"])

        logger.warning(f"🚨 Alerts generated: {len(alerts)}")

        # ==============================
        # INTELLIGENCE ENGINE
        # ==============================
        run_intelligence(master_df, analytics)

        logger.warning("🧠 Intelligence generated")

        # ==============================
        # FINAL SUMMARY
        # ==============================
        logger.warning("\n" + "=" * 45)
        logger.warning("✅ ENGINE COMPLETED")
        logger.warning("📊 Output:")
        logger.warning("→ data/output/intelligence/final_report.csv")
        logger.warning("📁 Logs:")
        logger.warning("→ data/output/logs/mside.log")
        logger.warning("=" * 45)

    except Exception as e:
        logger.error("❌ ENGINE FAILED", exc_info=True)


# ==============================
# ENTRY POINT (CLI SUPPORT)
# ==============================
if __name__ == "__main__":
    import sys

    mode = "normal"

    if len(sys.argv) > 1:
        if sys.argv[1] == "--debug":
            mode = "debug"
        elif sys.argv[1] == "--silent":
            mode = "silent"

    main(mode)