import logging
import pandas as pd
from typing import Dict, Any, List
from pathlib import Path

# Internal Module Imports
from utils.logger import setup_custom_logger

logger = setup_custom_logger("ingestion_loader")

class DataIngestor:
    """
    Production-grade Data Ingestor.
    Supports CSV and Excel from multiple marketplaces.
    """

    def __init__(self, ingestion_config: Dict[str, Any]):
        """
        :param ingestion_config: Dictionary containing marketplace definitions.
        """
        self.sources = ingestion_config
        logger.info(f"Ingestor initialized with {len(self.sources)} sources defined.")

    def load_all_sources(self) -> pd.DataFrame:
        """
        Iterates through all defined sources and concatenates them into one DataFrame.
        """
        all_data = []

        for mkp_name, config in self.sources.items():
            logger.info(f"Processing source: {mkp_name}")
            df = self._load_single_source(mkp_name, config)
            
            if df is not None and not df.empty:
                # Add metadata for downstream mapping
                df["_marketplace_source"] = mkp_name
                all_data.append(df)
            else:
                logger.warning(f"Source {mkp_name} returned no data.")

        if not all_data:
            logger.error("All ingestion sources failed or returned no data.")
            return pd.DataFrame()

        # Combine all sources
        final_df = pd.concat(all_data, axis=0, ignore_index=True, sort=False)
        logger.info(f"Successfully ingested total {len(final_df)} rows from {len(all_data)} sources.")
        return final_df

    def _load_single_source(self, name: str, config: Dict[str, Any]) -> pd.DataFrame:
        """Loads data based on source_type (csv/xlsx)."""
        file_path = Path(config.get("path", ""))
        source_type = config.get("source_type", "csv").lower()

        if not file_path.exists():
            logger.error(f"File not found for {name}: {file_path}")
            return pd.DataFrame()

        try:
            if source_type == "csv":
                return pd.read_csv(file_path)
            elif source_type in ["xlsx", "excel"]:
                return pd.read_excel(file_path)
            else:
                logger.error(f"Unsupported source type: {source_type}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return pd.DataFrame()