from pathlib import Path
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def load_shopee_csv(path: Path) -> Optional[pd.DataFrame]:
    try:
        if not path.exists():
            logger.warning(f"Shopee file not found: {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        if df.empty:
            logger.warning(f"Shopee file is empty: {path}")
            return pd.DataFrame()

        df["source"] = "shopee"

        logger.info(f"Shopee data loaded: {path} with shape {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error loading Shopee CSV {path}: {e}")
        return pd.DataFrame()


def validate_shopee_schema(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Shopee DataFrame is empty during validation")
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
            logger.error(f"Missing Shopee columns: {missing_cols}")
            return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error validating Shopee schema: {e}")
        return pd.DataFrame()


def load_and_validate_shopee(path: Path) -> Optional[pd.DataFrame]:
    try:
        df = load_shopee_csv(path)

        if df is None or df.empty:
            return pd.DataFrame()

        df = validate_shopee_schema(df)

        if df is None or df.empty:
            return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error in Shopee loader pipeline: {e}")
        return pd.DataFrame()