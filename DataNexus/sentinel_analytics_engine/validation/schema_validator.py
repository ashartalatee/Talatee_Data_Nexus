from typing import List, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: List[str],
    strict: bool = True
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Input DataFrame is empty during schema validation")
            return pd.DataFrame()

        if not required_columns:
            logger.warning("No required columns provided for validation")
            return df

        missing_cols = [col for col in required_columns if col not in df.columns]

        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return pd.DataFrame()

        if strict:
            extra_cols = [col for col in df.columns if col not in required_columns]
            if extra_cols:
                logger.warning(f"Extra columns detected (strict mode): {extra_cols}")

        logger.info("Schema validation passed")
        return df

    except Exception as e:
        logger.error(f"Error during schema validation: {e}")
        return pd.DataFrame()


def enforce_column_types(
    df: pd.DataFrame,
    column_types: dict
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Input DataFrame is empty during type enforcement")
            return pd.DataFrame()

        for col, dtype in column_types.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"Failed to cast column {col} to {dtype}: {e}")

        logger.info("Column type enforcement completed")
        return df

    except Exception as e:
        logger.error(f"Error during type enforcement: {e}")
        return pd.DataFrame()


def validate_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    column_types: Optional[dict] = None,
    strict: bool = True
) -> Optional[pd.DataFrame]:
    try:
        df = validate_required_columns(df, required_columns, strict)

        if df is None or df.empty:
            return pd.DataFrame()

        if column_types:
            df = enforce_column_types(df, column_types)

            if df is None or df.empty:
                return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error in schema validation pipeline: {e}")
        return pd.DataFrame()