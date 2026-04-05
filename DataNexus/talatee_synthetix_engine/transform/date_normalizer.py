import logging
from typing import Dict, Any, Optional, Union
import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("date_normalizer")

class DateNormalizer:
    """
    Production-grade module to normalize various marketplace date formats 
    into a standardized Python datetime objects.
    Handles ISO8601, Unix Timestamps, and custom string formats.
    """

    def __init__(self, transform_config: Dict[str, Any]):
        """
        :param transform_config: Dictionary containing 'date_format' and target columns.
        """
        self.config = transform_config
        self.date_format = self.config.get("date_format", "ISO8601")
        self.target_columns = self.config.get("date_columns", ["order_date", "payment_date"])
        logger.info(f"DateNormalizer initialized. Target format: {self.date_format}")

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main entry point to convert string or numeric date columns into datetime.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        try:
            working_df = df.copy()
            
            # Find columns that actually exist in the DataFrame
            valid_cols = [col for col in self.target_columns if col in working_df.columns]
            
            if not valid_cols:
                logger.warning("No date columns found in DataFrame for normalization.")
                return working_df

            for col in valid_cols:
                logger.debug(f"Normalizing date column: {col}")
                
                # Handle Unix Timestamps (int/float)
                if self.date_format.upper() == "UNIX":
                    working_df[col] = pd.to_datetime(working_df[col], unit='s', errors='coerce')
                
                # Handle ISO8601 or Auto-inference
                elif self.date_format.upper() == "ISO8601" or self.date_format.upper() == "AUTO":
                    working_df[col] = pd.to_datetime(working_df[col], errors='coerce')
                
                # Handle Custom Format
                else:
                    working_df[col] = pd.to_datetime(
                        working_df[col], 
                        format=self.date_format, 
                        errors='coerce'
                    )

                # Defensive check for failed parsing
                na_count = working_df[col].isna().sum()
                if na_count > 0:
                    logger.warning(f"Failed to parse {na_count} dates in column '{col}'. Set to NaT.")

            logger.info(f"Date normalization completed for columns: {valid_cols}")
            return working_df

        except Exception as e:
            logger.error(f"Critical error during date normalization: {str(e)}", exc_info=True)
            return df

    def filter_by_range(self, df: pd.DataFrame, date_col: str, start: str, end: str) -> pd.DataFrame:
        """
        Helper method to filter DataFrame by a specific date range after normalization.
        """
        if date_col not in df.columns:
            return df
            
        try:
            mask = (df[date_col] >= pd.to_datetime(start)) & (df[date_col] <= pd.to_datetime(end))
            return df.loc[mask]
        except Exception as e:
            logger.error(f"Failed to filter by date range: {e}")
            return df