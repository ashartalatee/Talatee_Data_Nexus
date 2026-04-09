import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional

from ingestion.shopee_loader import ShopeeLoader
from ingestion.tokopedia_loader import TokopediaLoader
from ingestion.tiktokshop_loader import TikTokShopLoader
from ingestion.whatsapp_loader import WhatsAppLoader

class DataIngestor:
    """
    Orchestrates data collection from multiple sources based on client configuration.
    Ensures all ingested data is merged into a single unified DataFrame.
    """
    def __init__(self, config: Dict[str, Any], base_dir: Path, logger: logging.Logger):
        self.config = config
        self.base_dir = base_dir
        self.logger = logger
        self.client_id = config.get("client_id", "unknown")
        
        # Mapping platforms to their respective loader classes
        self.loaders = {
            "shopee": ShopeeLoader,
            "tokopedia": TokopediaLoader,
            "tiktokshop": TikTokShopLoader,
            "whatsapp": WhatsAppLoader
        }

    def run(self) -> Optional[pd.DataFrame]:
        """
        Iterates through configured sources and aggregates data.
        """
        self.logger.info(f"Starting data ingestion for client: {self.client_id}")
        
        sources: List[Dict[str, Any]] = self.config.get("ingestion", {}).get("sources", [])
        if not sources:
            self.logger.warning(f"No ingestion sources defined for {self.client_id}")
            return None

        all_dataframes: List[pd.DataFrame] = []

        for source in sources:
            source_name = source.get("source_name", "unnamed_source")
            platform = source.get("platform", "").lower()
            
            self.logger.info(f"Processing source: {source_name} ({platform})")

            try:
                loader_class = self.loaders.get(platform)
                if not loader_class:
                    self.logger.error(f"Unsupported platform '{platform}' in source {source_name}")
                    continue

                loader = loader_class(source, self.base_dir, self.logger)
                df_source = loader.load()

                if df_source is not None and not df_source.empty:
                    # Inject source metadata for traceability
                    df_source["_internal_source_name"] = source_name
                    df_source["_internal_platform"] = platform
                    all_dataframes.append(df_source)
                    self.logger.info(f"Successfully loaded {len(df_source)} rows from {source_name}")
                else:
                    self.logger.warning(f"Source {source_name} returned no data.")

            except Exception as e:
                self.logger.error(f"Critical error loading source {source_name}: {str(e)}", exc_info=True)
                continue

        if not all_dataframes:
            self.logger.error("All ingestion sources failed or returned empty data.")
            return None

        try:
            # Concatenate all sources into a single master DataFrame
            final_df = pd.concat(all_dataframes, ignore_index=True)
            self.logger.info(f"Ingestion complete. Combined records: {len(final_df)}")
            return final_df
        except Exception as e:
            self.logger.error(f"Failed to concatenate dataframes: {str(e)}")
            return None