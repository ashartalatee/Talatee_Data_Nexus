import pandas as pd
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def build_metrics_dataframe(metrics: Dict[str, Any]) -> pd.DataFrame:
    try:
        if not metrics:
            logger.warning("Empty metrics in build_metrics_dataframe")
            return pd.DataFrame()

        df = pd.DataFrame(list(metrics.items()), columns=["metric", "value"])
        return df

    except Exception as e:
        logger.error(f"Error building metrics DataFrame: {e}")
        return pd.DataFrame()


def build_insights_dataframe(insights: Dict[str, Any]) -> pd.DataFrame:
    try:
        if not insights:
            logger.warning("Empty insights in build_insights_dataframe")
            return pd.DataFrame()

        df = pd.DataFrame(list(insights.items()), columns=["insight_type", "description"])
        return df

    except Exception as e:
        logger.error(f"Error building insights DataFrame: {e}")
        return pd.DataFrame()


def merge_report(
    df: pd.DataFrame,
    metrics_df: pd.DataFrame,
    insights_df: pd.DataFrame
) -> pd.DataFrame:
    try:
        if df is None or df.empty:
            logger.warning("Empty base DataFrame in merge_report")
            return pd.DataFrame()

        report_df = df.copy()

        if not metrics_df.empty:
            for _, row in metrics_df.iterrows():
                report_df[row["metric"]] = row["value"]

        if not insights_df.empty:
            for _, row in insights_df.iterrows():
                report_df[row["insight_type"]] = row["description"]

        logger.info("Report merged successfully")
        return report_df

    except Exception as e:
        logger.error(f"Error merging report: {e}")
        return pd.DataFrame()


def run_report_builder(
    df: pd.DataFrame,
    metrics: Optional[Dict[str, Any]] = None,
    insights: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_report_builder")
            return pd.DataFrame()

        metrics_df = build_metrics_dataframe(metrics or {})
        insights_df = build_insights_dataframe(insights or {})

        report_df = df

        if config:
            if config.get("include_metrics", True):
                report_df = merge_report(report_df, metrics_df, pd.DataFrame())

            if report_df.empty:
                return report_df

            if config.get("include_insights", True):
                report_df = merge_report(report_df, pd.DataFrame(), insights_df)

            if report_df.empty:
                return report_df

        logger.info("Report building completed")
        return report_df

    except Exception as e:
        logger.error(f"Error in report builder pipeline: {e}")
        return pd.DataFrame()