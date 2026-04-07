import pandas as pd
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


def map_columns(
    df: pd.DataFrame,
    column_mapping: Dict[str, str]
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in map_columns")
            return pd.DataFrame()

        if not column_mapping:
            logger.warning("No column mapping provided")
            return df

        existing_mappings = {
            src: tgt for src, tgt in column_mapping.items() if src in df.columns
        }

        missing_columns = [
            src for src in column_mapping.keys() if src not in df.columns
        ]

        if missing_columns:
            logger.warning(f"Missing columns for mapping: {missing_columns}")

        df = df.rename(columns=existing_mappings)

        logger.info(f"Columns mapped: {existing_mappings}")
        return df

    except Exception as e:
        logger.error(f"Error mapping columns: {e}")
        return pd.DataFrame()


def enforce_column_order(
    df: pd.DataFrame,
    ordered_columns: Optional[list] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in enforce_column_order")
            return pd.DataFrame()

        if not ordered_columns:
            return df

        existing_columns = [col for col in ordered_columns if col in df.columns]

        df = df[existing_columns + [col for col in df.columns if col not in existing_columns]]

        logger.info("Column order enforced")
        return df

    except Exception as e:
        logger.error(f"Error enforcing column order: {e}")
        return pd.DataFrame()


def add_missing_columns(
    df: pd.DataFrame,
    required_columns: Optional[list] = None,
    default_value=None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in add_missing_columns")
            return pd.DataFrame()

        if not required_columns:
            return df

        for col in required_columns:
            if col not in df.columns:
                df[col] = default_value
                logger.warning(f"Added missing column: {col} with default={default_value}")

        return df

    except Exception as e:
        logger.error(f"Error adding missing columns: {e}")
        return pd.DataFrame()


def run_column_mapping(
    df: pd.DataFrame,
    config: Optional[Dict] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_column_mapping")
            return pd.DataFrame()

        if config:
            df = map_columns(
                df,
                column_mapping=config.get("mapping", {})
            )
            if df.empty:
                return df

            df = add_missing_columns(
                df,
                required_columns=config.get("required_columns", []),
                default_value=config.get("default_value", None)
            )
            if df.empty:
                return df

            df = enforce_column_order(
                df,
                ordered_columns=config.get("column_order", [])
            )
            if df.empty:
                return df

        logger.info("Column mapping completed")
        return df

    except Exception as e:
        logger.error(f"Error in column mapping pipeline: {e}")
        return pd.DataFrame()