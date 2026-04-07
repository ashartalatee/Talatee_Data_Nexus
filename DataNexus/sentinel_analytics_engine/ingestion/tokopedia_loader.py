from pathlib import Path
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def load_tokopedia_csv(path: Path) -> Optional[pd.DataFrame]:
    try:
        if not path.exists():
            logger.warning(f"Tokopedia file not found: {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        if df.empty:
            logger.warning(f"Tokopedia file is empty: {path}")
            return pd.DataFrame()

        df["source"] = "tokopedia"

        logger.info(f"Tokopedia data loaded: {path} with shape {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error loading Tokopedia CSV {path}: {e}")
        return pd.DataFrame()


def validate_tokopedia_schema(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Tokopedia DataFrame is empty during validation")
            return pd.DataFrame()

        required_columns = [
            "order_id",
            "date",
            "product_name",
            "quantity",
            "price",
            "revenue",
            "customer"
        ]

        missing_cols = [col for col in required_columns if col not in df.columns]

        if missing_cols:
            logger.error(f"Missing Tokopedia columns: {missing_cols}")
            return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error validating Tokopedia schema: {e}")
        return pd.DataFrame()


def load_and_validate_tokopedia(path: Path) -> Optional[pd.DataFrame]:
    try:
        df = load_tokopedia_csv(path)

        if df is None or df.empty:
            return pd.DataFrame()

        df = validate_tokopedia_schema(df)

        if df is None or df.empty:
            return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error in Tokopedia loader pipeline: {e}")
        return pd.DataFrame()