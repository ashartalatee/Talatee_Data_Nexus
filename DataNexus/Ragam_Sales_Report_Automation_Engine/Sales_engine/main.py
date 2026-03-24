import sys
import traceback

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
#  SAFE EXECUTOR (FINAL)
# ==============================
def safe_run(step_name, func, logger, *args, critical=False, default=None, **kwargs):
    try:
        logger.info(f"▶ Running: {step_name}")
        result = func(*args, **kwargs)
        logger.info(f"✅ Completed: {step_name}")
        return result

    except Exception as e:
        logger.error(f"❌ FAILED: {step_name} → {str(e)}", exc_info=True)

        if critical:
            raise RuntimeError(f"CRITICAL FAILURE at step: {step_name}")

        return default


# ==============================
#  MAIN ENGINE
# ==============================
def main(mode="normal"):
    logger = get_logger(mode)

    logger.warning("\n🚀 MSIDE Engine Starting...\n")

    try:
        # ==============================
        # CONFIG
        # ==============================
        config = safe_run(
            "Load Config",
            load_config,
            logger,
            critical=True
        )

        if not config:
            raise ValueError("CRITICAL: Config kosong")

        # ==============================
        # DATA PIPELINE (STRICT MODE)
        # ==============================
        dfs = safe_run(
            "Load Data",
            load_all_data,
            logger,
            config,
            critical=True
        )

        if not dfs:
            raise ValueError("CRITICAL: Tidak ada data yang berhasil diload")

        validated = safe_run(
            "Validation",
            validate_all,
            logger,
            dfs,
            critical=True
        )

        cleaned = safe_run(
            "Cleaning",
            clean_all,
            logger,
            validated,
            critical=True
        )

        standardized = safe_run(
            "Standardization",
            standardize_all,
            logger,
            cleaned,
            critical=True
        )

        if not standardized:
            raise ValueError("CRITICAL: Semua data gagal distandardisasi")

        master_df = safe_run(
            "Merging",
            merge_all,
            logger,
            standardized,
            critical=True
        )

        if master_df is None or master_df.empty:
            raise ValueError("CRITICAL: Master data kosong")

        logger.warning("📊 Data processed successfully")

        # ==============================
        # ANALYTICS
        # ==============================
        analytics = safe_run(
            "Analytics",
            run_analytics,
            logger,
            master_df,
            default={}
        )

        if not analytics:
            logger.warning("⚠️ Analytics kosong → lanjut tanpa insight")

        logger.warning("📈 Analytics generated")

        # ==============================
        # ALERT ENGINE
        # ==============================
        alerts = []

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

        # ==============================
        # INTELLIGENCE ENGINE
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
        logger.warning("\n" + "=" * 50)
        logger.warning("✅ ENGINE COMPLETED SUCCESSFULLY")
        logger.warning("📊 Output:")
        logger.warning("→ data/output/intelligence/final_report.csv")
        logger.warning("📁 Logs:")
        logger.warning("→ data/output/logs/mside.log")
        logger.warning("=" * 50)

    except Exception as e:
        logger.error("\n❌ ENGINE FAILED (FATAL)")
        logger.error(str(e))

        # 🔥 DEBUG MODE TRACE
        if mode == "debug":
            traceback.print_exc()

        logger.error("=" * 50)


# ==============================
# ENTRY POINT
# ==============================
if __name__ == "__main__":
    mode = "normal"

    if len(sys.argv) > 1:
        if sys.argv[1] == "--debug":
            mode = "debug"
        elif sys.argv[1] == "--silent":
            mode = "silent"

    main(mode)