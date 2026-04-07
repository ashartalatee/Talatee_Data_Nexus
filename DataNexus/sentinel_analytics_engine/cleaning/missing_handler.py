import pandas as pd
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def handle_missing_values(
    df: pd.DataFrame,
    fill_config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in handle_missing_values")
            return pd.DataFrame()

        if not fill_config:
            return df

        for col, value in fill_config.items():
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    df[col] = df[col].fillna(value)
                    logger.info(f"Filled {missing_count} missing values in {col} with {value}")

        return df

    except Exception as e:
        logger.error(f"Error handling missing values: {e}")
        return pd.DataFrame()


def drop_missing_rows(
    df: pd.DataFrame,
    subset: Optional[list] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in drop_missing_rows")
            return pd.DataFrame()

        before = len(df)
        df = df.dropna(subset=subset) if subset else df.dropna()
        after = len(df)

        logger.info(f"Dropped {before - after} rows with missing values")
        return df

    except Exception as e:
        logger.error(f"Error dropping missing rows: {e}")
        return pd.DataFrame()


def drop_high_null_columns(
    df: pd.DataFrame,
    threshold: float = 0.5
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in drop_high_null_columns")
            return pd.DataFrame()

        null_ratio = df.isnull().mean()
        cols_to_drop = null_ratio[null_ratio > threshold].index.tolist()

        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.info(f"Dropped columns due to high null ratio: {cols_to_drop}")

        return df

    except Exception as e:
        logger.error(f"Error dropping high null columns: {e}")
        return pd.DataFrame()


def run_missing_handling(
    df: pd.DataFrame,
    config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_missing_handling")
            return pd.DataFrame()

        if config:
            df = handle_missing_values(
                df,
                fill_config=config.get("fill_missing", {})
            )
            if df.empty:
                return df

            df = drop_high_null_columns(
                df,
                threshold=config.get("null_column_threshold", 0.5)
            )
            if df.empty:
                return df

            if config.get("drop_rows", False):
                df = drop_missing_rows(
                    df,
                    subset=config.get("drop_subset", None)
                )
                if df.empty:
                    return df

        logger.info("Missing value handling completed")
        return df

    except Exception as e:
        logger.error(f"Error in missing handling pipeline: {e}")
        return pd.DataFrame()