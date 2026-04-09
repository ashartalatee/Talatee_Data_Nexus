import logging
import pandas as pd
from typing import Dict, Any, Optional

class DuplicateHandler:
    """
    Identifies and removes duplicate records from the dataset.
    Configurable to look at specific unique keys or entire rows.
    """
    def __init__(self, clean_cfg: Dict[str, Any], logger: logging.Logger):
        self.config = clean_cfg
        self.logger = logger

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes deduplication logic based on configuration.
        """
        if df is None or df.empty:
            return df

        try:
            initial_count = len(df)
            
            # Identify columns to check for duplicates
            # Default to checking all columns if no subset is provided
            subset = self.config.get("duplicate_keys")
            
            # Ensure subset columns actually exist in the dataframe
            if subset:
                subset = [col for col in subset if col in df.columns]
                if not subset:
                    subset = None
                    self.logger.warning("Duplicate keys provided in config do not exist in data. Defaulting to all columns.")

            keep_strategy = self.config.get("duplicate_keep_strategy", "first")
            
            # Perform deduplication
            df = df.drop_duplicates(subset=subset, keep=keep_strategy).copy()
            
            removed_count = initial_count - len(df)
            if removed_count > 0:
                self.logger.info(f"Removed {removed_count} duplicate records using strategy '{keep_strategy}'.")
            
            return df

        except Exception as e:
            self.logger.error(f"Error during duplicate handling: {str(e)}")
            return df