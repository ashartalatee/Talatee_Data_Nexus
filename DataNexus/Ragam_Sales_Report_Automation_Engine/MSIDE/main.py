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


# ==============================
#  SAFE EXECUTOR (ANTI CRASH)
# ==============================
def safe_run(step_name, func, logger, *args, default=None, **kwargs):
    try:
        logger.info(f"Running step: {step_name}")
        result = func(*args, **kwargs)
        logger.info(f"{step_name} completed")
        return result
    except Exception:
        logger.error(f"{step_name} FAILED", exc_info=True)
        return default


# ==============================
#  MAIN ENGINE
# ==============================
def main(mode="normal"):
    logger = get_logger(mode)

    logger.warning("🚀 MSIDE Engine Starting...\n")

    try:
        # ==============================
        # CONFIG
        # ==============================
        config = safe_run("Load Config", load_config, logger, default={})

        # ==============================
        # DATA PIPELINE
        # ==============================
        dfs = safe_run("Load Data", load_all_data, logger, config, default=[])

        validated = safe_run("Validation", validate_all, logger, dfs, default=[])

        cleaned = safe_run("Cleaning", clean_all, logger, validated, default=[])

        standardized = safe_run(
            "Standardization", standardize_all, logger, cleaned, default=[]
        )

        master_df = safe_run(
            "Merging", merge_all, logger, standardized, default=None
        )

        if master_df is None or master_df.empty:
            logger.warning("⚠️ Master data kosong, pipeline lanjut")

        logger.warning("📊 Data processed")

        # ==============================
        # ANALYTICS
        # ==============================
        analytics = safe_run(
            "Analytics", run_analytics, logger, master_df, default={}
        )

        if not analytics:
            logger.warning("⚠️ Analytics kosong")

        logger.warning("📈 Analytics generated")

        # ==============================
        # ALERT ENGINE (SAFE)
        # ==============================
        if analytics and "daily_revenue" in analytics:
            alerts = safe_run(
                "Alerts",
                run_alerts,
                logger,
                master_df,
                analytics["daily_revenue"],
                default=[]
            )
            logger.warning(f"🚨 Alerts generated: {len(alerts)}")
        else:
            logger.warning("⚠️ Alert dilewati (analytics tidak tersedia)")
            alerts = []

        # ==============================
        # INTELLIGENCE ENGINE (SAFE)
        # ==============================
        if analytics:
            safe_run(
                "Intelligence",
                run_intelligence,
                logger,
                master_df,
                analytics,
            )
            logger.warning("🧠 Intelligence generated")
        else:
            logger.warning("⚠️ Intelligence dilewati")

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

    except Exception:
        logger.error("❌ ENGINE FAILED (FATAL)", exc_info=True)


# ==============================
#  ENTRY POINT (CLI)
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