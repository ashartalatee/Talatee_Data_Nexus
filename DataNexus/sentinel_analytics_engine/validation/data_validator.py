import pandas as pd
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def validate_not_empty(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("DataFrame is empty")
            return pd.DataFrame()
        return df
    except Exception as e:
        logger.error(f"Error checking empty DataFrame: {e}")
        return pd.DataFrame()


def validate_no_negative_values(
    df: pd.DataFrame,
    columns: Optional[list] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("DataFrame is empty during negative value validation")
            return pd.DataFrame()

        if not columns:
            return df

        for col in columns:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    logger.warning(f"{negative_count} negative values found in column: {col}")
                    df = df[df[col] >= 0]

        return df

    except Exception as e:
        logger.error(f"Error validating negative values: {e}")
        return pd.DataFrame()


def validate_null_threshold(
    df: pd.DataFrame,
    threshold: float = 0.3
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("DataFrame is empty during null threshold validation")
            return pd.DataFrame()

        null_ratio = df.isnull().mean()
        cols_to_drop = null_ratio[null_ratio > threshold].index.tolist()

        if cols_to_drop:
            logger.warning(f"Dropping columns due to high null ratio: {cols_to_drop}")
            df = df.drop(columns=cols_to_drop)

        return df

    except Exception as e:
        logger.error(f"Error validating null threshold: {e}")
        return pd.DataFrame()


def validate_unique_keys(
    df: pd.DataFrame,
    key_column: str
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("DataFrame is empty during unique key validation")
            return pd.DataFrame()

        if key_column not in df.columns:
            logger.warning(f"Key column not found: {key_column}")
            return df

        duplicate_count = df.duplicated(subset=[key_column]).sum()

        if duplicate_count > 0:
            logger.warning(f"{duplicate_count} duplicate keys found in {key_column}")
            df = df.drop_duplicates(subset=[key_column])

        return df

    except Exception as e:
        logger.error(f"Error validating unique keys: {e}")
        return pd.DataFrame()


def run_data_validation(
    df: pd.DataFrame,
    config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        df = validate_not_empty(df)
        if df.empty:
            return df

        if config:
            df = validate_no_negative_values(
                df,
                columns=config.get("non_negative_columns", [])
            )
            if df.empty:
                return df

            df = validate_null_threshold(
                df,
                threshold=config.get("null_threshold", 0.3)
            )
            if df.empty:
                return df

            df = validate_unique_keys(
                df,
                key_column=config.get("unique_key", "order_id")
            )
            if df.empty:
                return df

        logger.info("Data validation completed")
        return df

    except Exception as e:
        logger.error(f"Error in data validation pipeline: {e}")
        return pd.DataFrame()