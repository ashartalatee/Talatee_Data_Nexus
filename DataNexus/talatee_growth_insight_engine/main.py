from pathlib import Path
import argparse
import logging
import sys
from typing import List, Dict, Any

import pandas as pd

from utils.logger import setup_logger
from utils.config_loader import load_all_configs
from utils.scheduler import should_run
from utils.safe_df import ensure_df

from ingestion.load_data import load_all_sources

from cleaning.standardize import standardize_df
from cleaning.missing_handler import handle_missing_values
from cleaning.duplicate_handler import remove_duplicates
from cleaning.text_cleaner import clean_text_columns

from transform.column_mapper import map_columns
from transform.date_normalizer import normalize_dates
from transform.feature_engineering import build_features

from analysis.metrics import compute_metrics
from analysis.summary import build_summary
from analysis.insight import generate_insights

from output.exporter import export_results

from alerts.notifier import send_alert


logger = setup_logger(__name__)


def process_client(config: Dict[str, Any], debug: bool = False) -> None:
    """
    Process a single client pipeline end-to-end.
    """

    client_name = config.get("client_name", "unknown_client")
    logger.info(f"[START] Processing client: {client_name}")

    try:
        if not should_run(config):
            logger.info(f"[SKIP] Schedule not matched for {client_name}")
            return

        # ========================
        # INGESTION
        # ========================
        df = load_all_sources(config)
        df = ensure_df(df)

        if df.empty:
            logger.warning(f"[EMPTY] No data for {client_name}")
            return

        # ========================
        # CLEANING
        # ========================
        df = standardize_df(df, config)
        df = ensure_df(df)

        df = handle_missing_values(df, config)
        df = ensure_df(df)

        df = remove_duplicates(df, config)
        df = ensure_df(df)

        df = clean_text_columns(df, config)
        df = ensure_df(df)

        # ========================
        # TRANSFORM
        # ========================
        df = map_columns(df, config)
        df = ensure_df(df)

        df = normalize_dates(df, config)
        df = ensure_df(df)

        df = build_features(df, config)
        df = ensure_df(df)

        # ========================
        # ANALYSIS
        # ========================
        metrics_df = compute_metrics(df, config)
        metrics_df = ensure_df(metrics_df)

        summary_df = build_summary(metrics_df, config)
        summary_df = ensure_df(summary_df)

        insights_df = generate_insights(metrics_df, config)
        insights_df = ensure_df(insights_df)

        # ========================
        # EXPORT
        # ========================
        export_results(
            client_name=client_name,
            raw_df=df,
            metrics_df=metrics_df,
            summary_df=summary_df,
            insights_df=insights_df,
            config=config,
        )

        logger.info(f"[SUCCESS] Completed client: {client_name}")

    except Exception as e:
        logger.exception(f"[ERROR] Client failed: {client_name}")
        try:
            send_alert(client_name=client_name, message=str(e))
        except Exception:
            logger.error("[ALERT FAILED]")


def run_engine(target_client: str = None, debug: bool = False) -> None:
    """
    Main engine runner for all or specific clients.
    """

    logger.info("[ENGINE START] Talatee Data Nexus")

    try:
        configs = load_all_configs()
        if not configs:
            logger.error("[FATAL] No valid configs found")
            return

        logger.info(f"[INFO] Total configs loaded: {len(configs)}")

        for config in configs:
            client_name = config.get("client_name")

            if target_client and client_name != target_client:
                logger.info(f"[SKIP] {client_name} (not target)")
                continue

            process_client(config, debug=debug)

    except Exception as e:
        logger.exception("[FATAL ERROR] Engine crashed")
        try:
            send_alert(client_name="ENGINE", message=str(e))
        except Exception:
            logger.error("[ALERT FAILED]")


def parse_args():
    parser = argparse.ArgumentParser(description="Talatee Multi-Client Analytics Engine")

    parser.add_argument(
        "--client",
        type=str,
        help="Run specific client only",
        required=False,
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        run_engine(
            target_client=args.client,
            debug=args.debug,
        )
    except KeyboardInterrupt:
        logger.warning("[STOPPED] Interrupted by user")
        sys.exit(0)