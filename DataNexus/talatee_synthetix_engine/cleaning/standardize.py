import logging
from typing import Dict, Any, Optional
import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger
from cleaning.missing_handler import MissingHandler
from cleaning.duplicate_handler import DuplicateHandler
from cleaning.text_cleaner import TextCleaner

# Initialize Logger
logger = setup_custom_logger("data_standardizer")

class DataStandardizer:
    """
    Production-grade Data Standardizer orchestrating the cleaning pipeline.
    Standardizes raw multi-marketplace data into a unified format based on client config.
    """

    def __init__(self, cleaning_config: Dict[str, Any]):
        """
        :param cleaning_config: Dictionary containing cleaning parameters and strategies.
        """
        self.config = cleaning_config
        self.missing_handler = MissingHandler(self.config.get("handle_missing", {}))
        self.duplicate_handler = DuplicateHandler(self.config.get("drop_duplicates", True))
        self.text_cleaner = TextCleaner(self.config.get("standardize_text", {}))
        
        logger.info("DataStandardizer initialized with modular cleaning components.")

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main entry point for the cleaning pipeline.
        Sequentially applies deduplication, missing value handling, and text standardization.
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame passed to DataStandardizer. Skipping process.")
            return pd.DataFrame()

        try:
            working_df = df.copy()
            initial_rows = len(working_df)

            # 1. Handle Duplicates
            working_df = self.duplicate_handler.handle(working_df)
            logger.debug(f"Rows after duplicate handling: {len(working_df)}")

            # 2. Handle Missing Values
            working_df = self.missing_handler.handle(working_df)
            logger.debug(f"Rows after missing value handling: {len(working_df)}")

            # 3. Standardize Text Columns (Lowercasing, stripping, etc.)
            working_df = self.text_cleaner.clean(working_df)

            # 4. Enforce Data Types
            working_df = self._enforce_data_types(working_df)

            final_rows = len(working_df)
            logger.info(f"Cleaning complete. Rows: {initial_rows} -> {final_rows}")
            
            return working_df

        except Exception as e:
            logger.error(f"Critical error during data standardization: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _enforce_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures columns are in correct numeric/string formats as per config.
        """
        dtype_map = self.config.get("data_types", {})
        if not dtype_map:
            return df

        try:
            for col, dtype in dtype_map.items():
                if col in df.columns:
                    if dtype == "float64":
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
                    elif dtype == "int32":
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    elif dtype == "str":
                        df[col] = df[col].astype(str)
            return df
        except Exception as e:
            logger.warning(f"Data type enforcement partially failed: {e}")
            return df