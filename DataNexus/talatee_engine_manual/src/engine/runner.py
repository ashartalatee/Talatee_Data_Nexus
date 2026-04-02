from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# =========================
# IMPORT PIPELINES
# =========================
from src.ingestion.load_data import load_all_data
from src.cleaning.standardize import standardization_pipeline
from src.cleaning.missing_handler import missing_pipeline
from src.cleaning.duplicate_handler import duplicate_pipeline
from src.cleaning.text_cleaner import text_cleaning_pipeline
from src.transform.feature_engineering import transform_pipeline

# ANALYTICS
from src.analysis.metrics import (
    calculate_metrics,
    get_top_products,
    get_low_performing_products
)
from src.analysis.summary import build_summary
from src.analysis.insight import generate_insight

from src.output.exporter import export_to_csv
from src.utils.logger import setup_logger


# =========================================
# VALIDATION
# =========================================
def validate_config(config):
    required_keys = ["client_name", "data_sources", "schedule"]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Config missing required key: {key}")

    if "type" not in config["schedule"]:
        raise ValueError("schedule.type wajib ada")


# =========================================
# SAFE DATAFRAME HANDLER
# =========================================
def safe_df(df):
    if df is None:
        return pd.DataFrame()
    if isinstance(df, pd.DataFrame) and df.empty:
        return pd.DataFrame()
    return df


# =========================================
# DATE FILTER
# =========================================
def apply_date_filter(df, schedule_type):
    if "date" not in df.columns:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    now = datetime.now()

    if schedule_type == "daily":
        return df[df["date"].dt.date == now.date()]

    elif schedule_type == "weekly":
        return df[df["date"] >= now - timedelta(days=7)]

    elif schedule_type == "monthly":
        return df[df["date"] >= now - timedelta(days=30)]

    return df


# =========================================
# CLEANING PIPELINE WRAPPER
# =========================================
def run_cleaning(df, logger):
    steps = [
        ("standardization", standardization_pipeline),
        ("missing", missing_pipeline),
        ("duplicate", duplicate_pipeline),
        ("text_cleaning", text_cleaning_pipeline),
    ]

    for name, func in steps:
        try:
            before = len(df)
            df = func(df)
            logger.info(f"[CLEAN-{name.upper()}] {before} -> {len(df)}")
        except Exception as e:
            logger.exception(f"[ERROR] Cleaning step '{name}' failed: {e}")

    return df


# =========================================
# OUTPUT HANDLER
# =========================================
def save_outputs(df, summary, top_products, low_products, client, schedule_type, logger):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("output") / client / schedule_type
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"{client}_{ts}.csv"
    excel_path = output_dir / f"{client}_{ts}.xlsx"

    export_to_csv(df, csv_path)

    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="ALL_DATA", index=False)
        pd.DataFrame([summary]).to_excel(writer, sheet_name="SUMMARY", index=False)

        if not top_products.empty:
            top_products.to_excel(writer, sheet_name="TOP_PRODUCTS", index=False)

        if not low_products.empty:
            low_products.to_excel(writer, sheet_name="LOW_PRODUCTS", index=False)

    logger.info("[FINAL REPORT]")
    logger.info(f"CSV   : {csv_path.name}")
    logger.info(f"Excel : {excel_path.name}")
    logger.info(f"Path  : {output_dir}")


# =========================================
# MAIN ENGINE
# =========================================
def run_engine(config):
    validate_config(config)

    client = config["client_name"]
    schedule_type = config["schedule"]["type"]

    logger = setup_logger(client)

    try:
        logger.info(f"[START] {client} ({schedule_type})")
        logger.info(f"[CONFIG] {config}")

        # ========================
        # LOAD DATA
        # ========================
        df = load_all_data(config["data_sources"])

        if df is None or df.empty:
            logger.error("[ERROR] No data loaded")
            return

        rows_initial = len(df)
        logger.info(f"[LOAD] Rows: {rows_initial}")

        # ========================
        # CLEANING
        # ========================
        df = run_cleaning(df, logger)

        # ========================
        # TRANSFORM
        # ========================
        try:
            df = transform_pipeline(df)
            logger.info(f"[TRANSFORM] Rows: {len(df)}")
        except Exception as e:
            logger.exception(f"[ERROR] Transform failed: {e}")
            return

        # ========================
        # FILTER
        # ========================
        before_filter = len(df)
        df = apply_date_filter(df, schedule_type)
        after_filter = len(df)

        logger.info(f"[FILTER] {before_filter} -> {after_filter}")

        if df.empty:
            logger.warning("[WARNING] No data after filtering")
            return

        # ========================
        # ANALYTICS
        # ========================
        try:
            metrics = calculate_metrics(df)
            summary = build_summary(metrics)
        except Exception as e:
            logger.exception(f"[ERROR] Metrics/Summary failed: {e}")
            return

        #  FIX AMBIGUOUS ERROR (INI INTI MASALAH LO)
        top_products = safe_df(get_top_products(df))
        low_products = safe_df(get_low_performing_products(df))

        insights = generate_insight(
            summary=summary,
            top_products=top_products.to_dict("records"),
            low_products=low_products.to_dict("records")
        )

        # ========================
        # LOGGING BUSINESS
        # ========================
        logger.info("[BUSINESS SUMMARY]")
        for k, v in summary.items():
            logger.info(f"{k}: {v}")

        logger.info("[INSIGHT]")
        for ins in insights.get("insights", []):
            logger.info(f"- {ins}")

        if not top_products.empty:
            logger.info(f"[TOP PRODUCT] {top_products.iloc[0].to_dict()}")

        # ========================
        # DATA QUALITY CHECK
        # ========================
        filtered_ratio = (after_filter / rows_initial) if rows_initial > 0 else 0

        if filtered_ratio < 0.2:
            logger.error(
                f"[CRITICAL] Data terlalu sedikit: {round(filtered_ratio * 100, 1)}%"
            )
        elif filtered_ratio < 0.5:
            logger.warning(
                f"[DATA QUALITY WARNING] Only {round(filtered_ratio * 100, 1)}% data used"
            )

        # ========================
        # OUTPUT
        # ========================
        save_outputs(
            df,
            summary,
            top_products,
            low_products,
            client,
            schedule_type,
            logger
        )

        logger.info(f"[DONE] {client}")

    except Exception as e:
        logger.exception(f"[FATAL ERROR] {client}: {e}")