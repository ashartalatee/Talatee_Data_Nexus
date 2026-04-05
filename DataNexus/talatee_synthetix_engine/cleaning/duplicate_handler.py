import logging
from typing import Dict, Any, Union, List, Optional
import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("duplicate_handler")

class DuplicateHandler:
    """
    Production-grade handler for identifying and removing duplicate records.
    Supports specific subset columns and 'keep' strategies based on client config.
    """

    def __init__(self, config: Union[bool, Dict[str, Any]]):
        """
        :param config: Either a boolean (default settings) or a dictionary with 
                       'subset' (list of columns) and 'keep' ('first', 'last', False).
        """
        if isinstance(config, bool):
            self.enabled = config
            self.subset: Optional[List[str]] = None
            self.keep: str = "first"
        else:
            self.enabled = config.get("enabled", True)
            self.subset = config.get("subset")
            self.keep = config.get("keep", "first")

        logger.info(f"DuplicateHandler initialized. Enabled: {self.enabled}, Keep strategy: {self.keep}")

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes duplicate rows from the DataFrame if enabled.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        if not self.enabled:
            logger.debug("Duplicate removal is disabled in config. Skipping.")
            return df

        try:
            initial_count = len(df)
            
            # Validate subset columns exist in DataFrame
            active_subset = None
            if self.subset:
                active_subset = [col for col in self.subset if col in df.columns]
                if not active_subset:
                    logger.warning("None of the specified subset columns found. Defaulting to all columns.")
                    active_subset = None

            # Execute drop
            clean_df = df.drop_duplicates(subset=active_subset, keep=self.keep)
            
            removed_count = initial_count - len(clean_df)
            if removed_count > 0:
                logger.info(f"Removed {removed_count} duplicate rows using subset: {active_subset or 'all_columns'}")
            else:
                logger.debug("No duplicate rows found.")

            return clean_df

        except Exception as e:
            logger.error(f"Error while handling duplicates: {str(e)}", exc_info=True)
            return df