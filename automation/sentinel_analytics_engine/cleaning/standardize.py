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
        Executes the cleaning pipeline: duplicates -> missing values -> text standardization.
        """
        if df is None or df.empty:
            self.logger.error("Cleaning aborted: Input DataFrame is empty or None.")
            return None

        try:
            self.logger.info("Starting data cleaning and standardization process.")

            # 1. Handle Duplicates
            if self.clean_cfg.get("remove_duplicates", True):
                df = self.duplicate_handler.handle(df)

            # 2. Handle Missing Values
            df = self.missing_handler.handle(df)

            # 3. Standardize Text Fields
            target_cols = self.clean_cfg.get("text_standardization", [])
            if target_cols:
                df = self.text_cleaner.clean(df, target_cols)

            # 4. Final Case Standardization
            # Ensure platform names are always lowercase for downstream mapping
            if "platform" in df.columns:
                df["platform"] = df["platform"].astype(str).str.lower().str.strip()

            self.logger.info(f"Cleaning complete. Remaining records: {len(df)}")
            return df

        except Exception as e:
            self.logger.critical(f"Critical failure in DataCleaner: {str(e)}", exc_info=True)
            return None