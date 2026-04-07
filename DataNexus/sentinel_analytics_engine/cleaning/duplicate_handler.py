import pandas as pd
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


def drop_exact_duplicates(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in drop_exact_duplicates")
            return pd.DataFrame()

        before = len(df)
        df = df.drop_duplicates()
        after = len(df)

        logger.info(f"Dropped {before - after} exact duplicate rows")
        return df

    except Exception as e:
        logger.error(f"Error dropping exact duplicates: {e}")
        return pd.DataFrame()


def drop_duplicates_by_columns(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in drop_duplicates_by_columns")
            return pd.DataFrame()

        if not subset:
            return df

        before = len(df)
        df = df.drop_duplicates(subset=subset)
        after = len(df)

        logger.info(f"Dropped {before - after} duplicates based on columns: {subset}")
        return df

    except Exception as e:
        logger.error(f"Error dropping duplicates by columns: {e}")
        return pd.DataFrame()


def mark_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in mark_duplicates")
            return pd.DataFrame()

        if not subset:
            df["is_duplicate"] = df.duplicated()
        else:
            df["is_duplicate"] = df.duplicated(subset=subset)

        duplicate_count = df["is_duplicate"].sum()
        logger.info(f"Marked {duplicate_count} duplicate rows")

        return df

    except Exception as e:
        logger.error(f"Error marking duplicates: {e}")
        return pd.DataFrame()


def run_duplicate_handling(
    df: pd.DataFrame,
    config: Optional[Dict[str, Any]] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_duplicate_handling")
            return pd.DataFrame()

        if config:
            if config.get("mark_duplicates", False):
                df = mark_duplicates(
                    df,
                    subset=config.get("subset", None)
                )
                if df.empty:
                    return df

            if config.get("drop_exact", True):
                df = drop_exact_duplicates(df)
                if df.empty:
                    return df

            if config.get("drop_by_subset", False):
                df = drop_duplicates_by_columns(
                    df,
                    subset=config.get("subset", None)
                )
                if df.empty:
                    return df

        logger.info("Duplicate handling completed")
        return df

    except Exception as e:
        logger.error(f"Error in duplicate handling pipeline: {e}")
        return pd.DataFrame()