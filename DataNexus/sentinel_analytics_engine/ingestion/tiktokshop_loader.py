from pathlib import Path
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def load_tiktokshop_csv(path: Path) -> Optional[pd.DataFrame]:
    try:
        if not path.exists():
            logger.warning(f"TikTokShop file not found: {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        if df.empty:
            logger.warning(f"TikTokShop file is empty: {path}")
            return pd.DataFrame()

        df["source"] = "tiktokshop"

        logger.info(f"TikTokShop data loaded: {path} with shape {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error loading TikTokShop CSV {path}: {e}")
        return pd.DataFrame()


def validate_tiktokshop_schema(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("TikTokShop DataFrame is empty during validation")
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
            logger.error(f"Missing TikTokShop columns: {missing_cols}")
            return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error validating TikTokShop schema: {e}")
        return pd.DataFrame()


def load_and_validate_tiktokshop(path: Path) -> Optional[pd.DataFrame]:
    try:
        df = load_tiktokshop_csv(path)

        if df is None or df.empty:
            return pd.DataFrame()

        df = validate_tiktokshop_schema(df)

        if df is None or df.empty:
            return pd.DataFrame()

        return df

    except Exception as e:
        logger.error(f"Error in TikTokShop loader pipeline: {e}")
        return pd.DataFrame()