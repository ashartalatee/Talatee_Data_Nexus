from pathlib import Path
import pandas as pd
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


def load_csv(path: Path) -> Optional[pd.DataFrame]:
    try:
        if not path.exists():
            logger.warning(f"File not found: {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)
        logger.info(f"Loaded CSV: {path} with shape {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error loading CSV {path}: {e}")
        return pd.DataFrame()


def load_sources(sources: List[Dict]) -> Optional[pd.DataFrame]:
    dataframes = []

    for source in sources:
        try:
            source_name = source.get("name", "unknown")
            source_type = source.get("type")
            source_path = source.get("path")

            if not source_type or not source_path:
                logger.warning(f"Invalid source config: {source}")
                continue

            path = Path(source_path)

            if source_type == "csv":
                df = load_csv(path)
            else:
                logger.warning(f"Unsupported source type: {source_type}")
                df = pd.DataFrame()

            if df is not None and not df.empty:
                df["source"] = source_name
                dataframes.append(df)

        except Exception as e:
            logger.error(f"Error processing source {source}: {e}")
            continue

    if not dataframes:
        logger.warning("No data loaded from any source")
        return pd.DataFrame()

    try:
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined DataFrame shape: {combined_df.shape}")
        return combined_df

    except Exception as e:
        logger.error(f"Error combining dataframes: {e}")
        return pd.DataFrame()