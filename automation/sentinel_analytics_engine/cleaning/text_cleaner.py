import logging
import pandas as pd
import re
from typing import Dict, Any, List, Optional

class TextCleaner:
    """
    Standardizes text fields by removing noise, extra spaces, 
    and enforcing consistent casing for marketplace data.
    """
    def __init__(self, clean_cfg: Dict[str, Any], logger: logging.Logger):
        self.config = clean_cfg
        self.logger = logger
        self.strip_whitespace = clean_cfg.get("strip_whitespace", True)
        self.remove_special_chars = clean_cfg.get("remove_special_chars", False)

    def clean(self, df: pd.DataFrame, target_columns: List[str]) -> pd.DataFrame:
        """
        Applies text cleaning transformations to the specified columns.
        """
        if df is None or df.empty:
            return df

        try:
            # Ensure only existing columns are processed
            valid_targets = [col for col in target_columns if col in df.columns]
            
            if not valid_targets:
                return df

            for col in valid_targets:
                # Convert to string and handle NaNs temporarily
                df[col] = df[col].astype(str).replace(['nan', 'None', 'NaN'], '')

                if self.strip_whitespace:
                    df[col] = df[col].apply(lambda x: " ".join(x.split()))

                if self.remove_special_chars:
                    # Keeps alphanumeric and basic punctuation
                    df[col] = df[col].apply(lambda x: re.sub(r'[^\w\s\-\.]', '', x))

                # Agency standard: Uppercase for SKU/Codes, Title Case for others
                if 'sku' in col.lower() or 'code' in col.lower():
                    df[col] = df[col].str.upper()
                else:
                    df[col] = df[col].str.title()

            self.logger.info(f"Standardized text in columns: {valid_targets}")
            return df

        except Exception as e:
            self.logger.error(f"Error during text cleaning: {str(e)}")
            return df