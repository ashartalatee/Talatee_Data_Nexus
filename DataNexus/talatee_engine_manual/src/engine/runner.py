from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# =========================
# IMPORT PIPELINES
# =========================
from src.transform.column_mapper import apply_column_mapping
from src.transform.date_normalizer import normalize_date_column
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
# AUTO COLUMN DETECTOR
# =========================================
def auto_fix_columns(df, logger):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    candidates = {
        "date": ["date", "order_date", "tanggal", "created_at"],
        "product_name": ["product_name", "item_name", "nama_produk"],
        "quantity": ["qty", "quantity", "jumlah"],
        "price": ["price", "unit_price", "harga"],
    }

    for target, options in candidates.items():
        if target not in df.columns:
            for opt in options:
                if opt in df.columns:
                    df.rename(columns={opt: target}, inplace=True)
                    logger.info(f"[AUTO-MAP] {opt} → {target}")
                    break

    return df


# =========================================
# VALIDATION
# =========================================
def validate_dataframe(df, logger):
    required_cols = ["date"]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        logger.error(f"[SCHEMA ERROR] Missing columns: {missing}")
        return False

    return True


# =========================================
# DATE FILTER (SMART)
# =========================================
def apply_date_filter(df, schedule_type, logger):
    if "date" not in df.columns:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    now = datetime.now()

    logger.info(f"[DEBUG] Date min: {df['date'].min()} | max: {df['date'].max()}")

    if schedule_type == "daily":
        filtered = df[df["date"].dt.date == now.date()]
    elif schedule_type == "weekly":
        filtered = df[df["date"] >= now - timedelta(days=7)]
    elif schedule_type == "monthly":
        filtered = df[df["date"] >= now - timedelta(days=30)]
    else:
        return df

    # 🔥 FALLBACK BIAR GA KOSONG
    if filtered.empty:
        logger.warning("[FILTER] Empty → fallback to ALL data")
        return df

    return filtered


# =========================================
# CLEANING PIPELINE (DEFENSIVE)
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
        except Exception:
            logger.exception(f"[ERROR] Cleaning step '{name}' failed")

    return df


# =========================================
# SAFE DF
# =========================================
def safe_df(df):
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()


# =========================================
# OUTPUT
# =========================================
def save_outputs(df, summary, top_products, low_products, client, schedule_type, logger):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("output") / client / schedule_type
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        export_to_csv(df, output_dir / f"{client}_{ts}.csv")
    except:
        logger.exception("[ERROR] CSV export failed")

    try:
        with pd.ExcelWriter(output_dir / f"{client}_{ts}.xlsx", engine="xlsxwriter") as writer:
            df.to_excel(writer, "ALL_DATA", index=False)
            pd.DataFrame([summary]).to_excel(writer, "SUMMARY", index=False)

            if not top_products.empty:
                top_products.to_excel(writer, "TOP_PRODUCTS", index=False)

            if not low_products.empty:
                low_products.to_excel(writer, "LOW_PRODUCTS", index=False)
    except:
        logger.exception("[ERROR] Excel export failed")

    logger.info(f"[OUTPUT] {output_dir}")


# =========================================
# MAIN ENGINE
# =========================================
def run_engine(config):
    client = config.get("client_name", "UNKNOWN")
    logger = setup_logger(client)

    try:
        schedule_type = config.get("schedule", {}).get("type", "daily")
        logger.info(f"[START] {client} ({schedule_type})")

        # ========================
        # LOAD
        # ========================
        df = load_all_data(config.get("data_sources", []))

        if df is None or df.empty:
            logger.error("[ERROR] No data loaded")
            return

        logger.info(f"[LOAD] Rows: {len(df)}")
        logger.info(f"[RAW COLUMNS] {list(df.columns)}")

        # ========================
        # AUTO FIX
        # ========================
        df = auto_fix_columns(df, logger)

        # ========================
        # MAPPING
        # ========================
        mapping = config.get("column_mapping", {})
        df = apply_column_mapping(df, mapping, logger)

        # 🔥 FIX DUPLICATE COLUMN
        df = df.loc[:, ~df.columns.duplicated()]

        logger.info(f"[FINAL COLUMNS] {list(df.columns)}")

        # ========================
        # DATE NORMALIZE
        # ========================
        if "date" in df.columns:
            df = normalize_date_column(df, "date", logger)

        # ========================
        # VALIDATE
        # ========================
        if not validate_dataframe(df, logger):
            return

        # ========================
        # CLEANING
        # ========================
        df = run_cleaning(df, logger)

        # ========================
        # TRANSFORM
        # ========================
        df = transform_pipeline(df)

        # ========================
        # FILTER
        # ========================
        before = len(df)
        df = apply_date_filter(df, schedule_type, logger)
        after = len(df)

        logger.info(f"[FILTER] {before} -> {after}")

        if df.empty:
            logger.warning("[WARNING] No data after filtering")
            return

        # ========================
        # ANALYTICS
        # ========================
        metrics = calculate_metrics(df)
        summary = build_summary(metrics)

        top_products = safe_df(get_top_products(df))
        low_products = safe_df(get_low_performing_products(df))

        insights = generate_insight(
            summary=summary,
            top_products=top_products.to_dict("records"),
            low_products=low_products.to_dict("records")
        )

        # ========================
        # LOG
        # ========================
        logger.info("[SUMMARY]")
        for k, v in summary.items():
            logger.info(f"{k}: {v}")

        logger.info("[INSIGHT]")
        for i in insights.get("insights", []):
            logger.info(f"- {i}")

        # ========================
        # OUTPUT
        # ========================
        save_outputs(df, summary, top_products, low_products, client, schedule_type, logger)

        logger.info(f"[DONE] {client}")

    except Exception as e:
        logger.exception(f"[FATAL] {client}: {e}")