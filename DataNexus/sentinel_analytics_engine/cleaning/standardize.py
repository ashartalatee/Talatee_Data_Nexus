import pandas as pd
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def standardize_column_names(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in standardize_column_names")
            return pd.DataFrame()

        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        logger.info("Column names standardized")
        return df

    except Exception as e:
        logger.error(f"Error standardizing column names: {e}")
        return pd.DataFrame()


def standardize_date_column(
    df: pd.DataFrame,
    date_column: str
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in standardize_date_column")
            return pd.DataFrame()

        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

        logger.info(f"Date column standardized: {date_column}")
        return df

    except Exception as e:
        logger.error(f"Error standardizing date column: {e}")
        return pd.DataFrame()


def standardize_numeric_columns(
    df: pd.DataFrame,
    numeric_columns: Optional[list] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in standardize_numeric_columns")
            return pd.DataFrame()

        if not numeric_columns:
            return df

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info("Numeric columns standardized")
        return df

    except Exception as e:
        logger.error(f"Error standardizing numeric columns: {e}")
        return pd.DataFrame()


def standardize_text_columns(
    df: pd.DataFrame,
    config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in standardize_text_columns")
            return pd.DataFrame()

        if not config:
            return df

        text_cols = df.select_dtypes(include="object").columns

        for col in text_cols:
            if config.get("lowercase", False):
                df[col] = df[col].str.lower()

            if config.get("trim_whitespace", False):
                df[col] = df[col].str.strip()

            if config.get("remove_special_chars", False):
                df[col] = df[col].str.replace(r"[^\w\s]", "", regex=True)

        logger.info("Text columns standardized")
        return df

    except Exception as e:
        logger.error(f"Error standardizing text columns: {e}")
        return pd.DataFrame()


def run_standardization(
    df: pd.DataFrame,
    config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        df = standardize_column_names(df)
        if df.empty:
            return df

        if config:
            df = standardize_date_column(
                df,
                date_column=config.get("date_column", "date")
            )
            if df.empty:
                return df

            df = standardize_numeric_columns(
                df,
                numeric_columns=config.get("numeric_columns", [])
            )
            if df.empty:
                return df

            df = standardize_text_columns(
                df,
                config=config.get("text_cleaning", {})
            )
            if df.empty:
                return df

        logger.info("Standardization completed")
        return df

    except Exception as e:
        logger.error(f"Error in standardization pipeline: {e}")
        return pd.DataFrame()