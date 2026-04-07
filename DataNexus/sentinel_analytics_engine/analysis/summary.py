import pandas as pd
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def generate_basic_summary(df: pd.DataFrame) -> Dict:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in generate_basic_summary")
            return {}

        summary = {
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": list(df.columns)
        }

        return summary

    except Exception as e:
        logger.error(f"Error generating basic summary: {e}")
        return {}


def generate_null_summary(df: pd.DataFrame) -> Dict:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in generate_null_summary")
            return {}

        null_counts = df.isnull().sum().to_dict()

        return {"null_counts": null_counts}

    except Exception as e:
        logger.error(f"Error generating null summary: {e}")
        return {}


def generate_numeric_summary(df: pd.DataFrame) -> Dict:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in generate_numeric_summary")
            return {}

        numeric_df = df.select_dtypes(include="number")

        if numeric_df.empty:
            return {}

        summary_stats = numeric_df.describe().to_dict()

        return {"numeric_summary": summary_stats}

    except Exception as e:
        logger.error(f"Error generating numeric summary: {e}")
        return {}


def generate_top_categories(
    df: pd.DataFrame,
    top_n: int = 5
) -> Dict:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in generate_top_categories")
            return {}

        categorical_df = df.select_dtypes(include="object")

        results = {}

        for col in categorical_df.columns:
            try:
                top_values = df[col].value_counts().head(top_n).to_dict()
                results[col] = top_values
            except Exception as inner_e:
                logger.warning(f"Skipping column {col}: {inner_e}")

        return {"top_categories": results}

    except Exception as e:
        logger.error(f"Error generating top categories: {e}")
        return {}


def run_summary(
    df: pd.DataFrame,
    config: Optional[Dict] = None
) -> Dict:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_summary")
            return {}

        summary = {}

        if config:
            if config.get("basic"):
                summary.update(generate_basic_summary(df))

            if config.get("nulls"):
                summary.update(generate_null_summary(df))

            if config.get("numeric"):
                summary.update(generate_numeric_summary(df))

            if config.get("top_categories"):
                summary.update(
                    generate_top_categories(
                        df,
                        top_n=config.get("top_n", 5)
                    )
                )

        logger.info("Summary generated")
        return summary

    except Exception as e:
        logger.error(f"Error in summary pipeline: {e}")
        return {}