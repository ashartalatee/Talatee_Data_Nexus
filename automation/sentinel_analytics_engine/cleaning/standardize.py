import logging
import pandas as pd
from typing import Dict, Any, Optional

from cleaning.missing_handler import MissingHandler
from cleaning.duplicate_handler import DuplicateHandler
from cleaning.text_cleaner import TextCleaner

class DataCleaner:
    """
    Orchestrates the data cleaning and standardization phase.
    Ensures data consistency across multi-marketplace sources.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.clean_cfg = config.get("cleaning", {})
        
        # Initialize sub-modules
        self.missing_handler = MissingHandler(self.clean_cfg, logger)
        self.duplicate_handler = DuplicateHandler(self.clean_cfg, logger)
        self.text_cleaner = TextCleaner(self.clean_cfg, logger)

    def process(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Executes the cleaning pipeline: Mapping -> Duplicates -> Missing -> Text.
        """
        if df is None or df.empty:
            self.logger.error("Cleaning aborted: Input DataFrame is empty or None.")
            return None

        try:
            self.logger.info("Starting data cleaning and standardization process.")

            # --- LANGKAH KRUSIAL: COLUMN MAPPING ---
            # Mengubah nama kolom mentah ke nama standar (e.g., 'No. Pesanan' -> 'order_id')
            # Dilakukan di awal agar sub-module lain mengenali nama kolom standar.
            mapping = self.config.get("transformation", {}).get("column_mapping", {})
            if mapping:
                df = df.rename(columns=mapping)
                self.logger.info("Column mapping applied. Standardized columns identified.")
            else:
                self.logger.warning("No column mapping found in transformation config.")

            # 1. Handle Duplicates
            if self.clean_cfg.get("remove_duplicates", True):
                df = self.duplicate_handler.handle(df)

            # 2. Handle Missing Values
            df = self.missing_handler.handle(df)

            # 3. Standardize Text Fields
            target_cols = self.clean_cfg.get("text_standardization", [])
            if target_cols:
                # Memastikan hanya membersihkan kolom yang memang ada di DataFrame
                existing_targets = [c for c in target_cols if c in df.columns]
                df = self.text_cleaner.clean(df, existing_targets)

            # 4. Final Case & Metadata Standardization
            # Membersihkan whitespace di semua nama kolom (mencegah error 'order_id ')
            df.columns = df.columns.str.strip()

            # Standarisasi kolom internal platform jika ada
            if "_internal_platform" in df.columns:
                df["_internal_platform"] = df["_internal_platform"].astype(str).str.lower().str.strip()

            self.logger.info(f"Cleaning complete. Remaining records: {len(df)}")
            return df

        except Exception as e:
            self.logger.critical(f"Critical failure in DataCleaner: {str(e)}", exc_info=True)
            return None