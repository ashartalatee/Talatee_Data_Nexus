import pandas as pd
import logging
from typing import Optional, List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)


def normalize_date_column(
    df: pd.DataFrame,
    column: str,
    output_format: str = "%Y-%m-%d"
) -> pd.Series:
    try:
        if column not in df.columns:
            logger.warning(f"Column not found for date normalization: {column}")
            return pd.Series([None] * len(df))

        def parse_date(value):
            if pd.isna(value):
                return None

            if isinstance(value, (datetime, pd.Timestamp)):
                return value.strftime(output_format)

            try:
                parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
                if pd.isna(parsed):
                    return None
                return parsed.strftime(output_format)
            except Exception:
                return None

        return df[column].apply(parse_date)

    except Exception as e:
        logger.error(f"Error normalizing date column {column}: {e}")
        return pd.Series([None] * len(df))


def normalize_multiple_dates(
    df: pd.DataFrame,
    columns: List[str],
    output_format: str = "%Y-%m-%d"
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in normalize_multiple_dates")
            return pd.DataFrame()

        for col in columns:
            df[col] = normalize_date_column(df, col, output_format)

        logger.info(f"Date columns normalized: {columns}")
        return df

    except Exception as e:
        logger.error(f"Error normalizing multiple date columns: {e}")
        return pd.DataFrame()


def extract_date_features(
    df: pd.DataFrame,
    column: str,
    prefix: Optional[str] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in extract_date_features")
            return pd.DataFrame()

        if column not in df.columns:
            logger.warning(f"Column not found for feature extraction: {column}")
            return df

        prefix = prefix or column

        parsed_dates = pd.to_datetime(df[column], errors="coerce")

        df[f"{prefix}_year"] = parsed_dates.dt.year
        df[f"{prefix}_month"] = parsed_dates.dt.month
        df[f"{prefix}_day"] = parsed_dates.dt.day
        df[f"{prefix}_weekday"] = parsed_dates.dt.weekday

        logger.info(f"Date features extracted for column: {column}")
        return df

    except Exception as e:
        logger.error(f"Error extracting date features: {e}")
        return pd.DataFrame()


def run_date_normalization(
    df: pd.DataFrame,
    config: Optional[Dict] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_date_normalization")
            return pd.DataFrame()

        if config:
            df = normalize_multiple_dates(
                df,
                columns=config.get("columns", []),
                output_format=config.get("output_format", "%Y-%m-%d")
            )
            if df.empty:
                return df

            feature_columns = config.get("feature_columns", [])

            for col in feature_columns:
                df = extract_date_features(
                    df,
                    column=col,
                    prefix=col
                )
                if df.empty:
                    return df

        logger.info("Date normalization completed")
        return df

    except Exception as e:
        logger.error(f"Error in date normalization pipeline: {e}")
        return pd.DataFrame()