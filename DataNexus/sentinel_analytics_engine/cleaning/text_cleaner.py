import pandas as pd
import logging
import re
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)


def clean_text_basic(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
    try:
        if config.get("lowercase", False):
            series = series.str.lower()

        if config.get("trim_whitespace", False):
            series = series.str.strip()

        if config.get("remove_special_chars", False):
            series = series.apply(lambda x: re.sub(r"[^\w\s]", "", x) if isinstance(x, str) else x)

        return series

    except Exception as e:
        logger.error(f"Error in basic text cleaning: {e}")
        return series


def remove_extra_spaces(series: pd.Series) -> pd.Series:
    try:
        return series.apply(lambda x: re.sub(r"\s+", " ", x).strip() if isinstance(x, str) else x)
    except Exception as e:
        logger.error(f"Error removing extra spaces: {e}")
        return series


def normalize_text_columns(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in normalize_text_columns")
            return pd.DataFrame()

        if not config:
            return df

        target_columns = columns if columns else df.select_dtypes(include="object").columns

        for col in target_columns:
            if col in df.columns:
                df[col] = clean_text_basic(df[col], config)
                df[col] = remove_extra_spaces(df[col])

        logger.info(f"Text columns normalized: {list(target_columns)}")
        return df

    except Exception as e:
        logger.error(f"Error normalizing text columns: {e}")
        return pd.DataFrame()


def remove_empty_strings(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in remove_empty_strings")
            return pd.DataFrame()

        target_columns = columns if columns else df.select_dtypes(include="object").columns

        for col in target_columns:
            if col in df.columns:
                df[col] = df[col].replace("", None)

        logger.info("Empty strings replaced with None")
        return df

    except Exception as e:
        logger.error(f"Error removing empty strings: {e}")
        return pd.DataFrame()


def run_text_cleaning(
    df: pd.DataFrame,
    config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_text_cleaning")
            return pd.DataFrame()

        if config:
            df = normalize_text_columns(
                df,
                columns=config.get("columns", None),
                config=config
            )
            if df.empty:
                return df

            if config.get("remove_empty_strings", True):
                df = remove_empty_strings(
                    df,
                    columns=config.get("columns", None)
                )
                if df.empty:
                    return df

        logger.info("Text cleaning completed")
        return df

    except Exception as e:
        logger.error(f"Error in text cleaning pipeline: {e}")
        return pd.DataFrame()