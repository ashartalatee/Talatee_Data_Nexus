import logging
import re
from typing import Dict, Any, List, Optional
import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("text_cleaner")

class TextCleaner:
    """
    Production-grade text cleaning module for e-commerce data.
    Handles lowercasing, whitespace stripping, and special character removal
    across multiple columns as defined in client configuration.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        :param config: Dictionary containing 'columns', 'lowercase', 
                       'strip_whitespace', and 'remove_special_chars'.
        """
        self.target_columns = config.get("columns", [])
        self.lowercase = config.get("lowercase", True)
        self.strip_whitespace = config.get("strip_whitespace", True)
        self.remove_special_chars = config.get("remove_special_chars", False)
        
        logger.info(f"TextCleaner initialized for columns: {self.target_columns}")

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies text cleaning transformations to the specified columns in the DataFrame.
        Returns a clean DataFrame safely.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        if not self.target_columns:
            logger.debug("No target columns specified for text cleaning. Skipping.")
            return df

        try:
            working_df = df.copy()
            
            # Filter for columns that actually exist in the DataFrame
            valid_cols = [col for col in self.target_columns if col in working_df.columns]
            
            for col in valid_cols:
                logger.debug(f"Cleaning text in column: {col}")
                
                # Ensure the column is treated as string and handle nulls
                working_df[col] = working_df[col].astype(str).replace('nan', '')

                if self.lowercase:
                    working_df[col] = working_df[col].str.lower()

                if self.strip_whitespace:
                    working_df[col] = working_df[col].str.strip()

                if self.remove_special_chars:
                    # Regex to keep only alphanumeric and basic spaces
                    working_df[col] = working_df[col].apply(
                        lambda x: re.sub(r'[^a-zA-Z0-9\s]', '', str(x))
                    )

            logger.info(f"Text cleaning completed for {len(valid_cols)} columns.")
            return working_df

        except Exception as e:
            logger.error(f"Error during text cleaning process: {str(e)}", exc_info=True)
            return df

    def sanitize_string(self, text: str) -> str:
        """
        Utility method for individual string sanitization outside of DataFrames.
        """
        if not isinstance(text, str):
            return ""
        
        clean_text = text
        if self.lowercase:
            clean_text = clean_text.lower()
        if self.strip_whitespace:
            clean_text = clean_text.strip()
        if self.remove_special_chars:
            clean_text = re.sub(r'[^a-zA-Z0-9\s]', '', clean_text)
            
        return clean_text