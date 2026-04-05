import logging
from typing import Dict, Any, List, Optional
import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("missing_handler")

class MissingHandler:
    """
    Production-grade handler for missing data (NaN/None) values.
    Supports multiple strategies: drop, fill_zero, fill_mean, or custom constants.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        :param config: Dictionary containing 'strategy', 'columns', and 'constant_value'.
        """
        # CRITICAL FIX: Store the full config to access 'constant_value' later
        self.config = config 
        self.strategy = config.get("strategy", "fill_zero")
        self.target_columns = config.get("columns", [])
        
        logger.info(f"MissingHandler initialized with strategy: {self.strategy}")

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the configured missing value strategy to the DataFrame.
        Returns a cleaned DataFrame or the original if an error occurs.
        """
        if df is None or df.empty:
            logger.warning("MissingHandler received an empty or None DataFrame.")
            return pd.DataFrame()

        try:
            working_df = df.copy()
            
            # Determine which columns to process
            cols_to_process = self.target_columns if self.target_columns else working_df.columns.tolist()
            
            # Ensure columns actually exist in DF to avoid KeyError
            valid_cols = [c for c in cols_to_process if c in working_df.columns]

            if not valid_cols:
                logger.warning("No valid columns found for missing value handling.")
                return working_df

            if self.strategy == "drop":
                before_count = len(working_df)
                working_df = working_df.dropna(subset=valid_cols)
                after_count = len(working_df)
                logger.info(f"Dropped {before_count - after_count} rows containing missing values.")

            elif self.strategy == "fill_zero":
                # Fill with 0, ensuring we don't force object type if not necessary
                for col in valid_cols:
                    fill_val = 0 if pd.api.types.is_numeric_dtype(working_df[col]) else "0"
                    working_df[col] = working_df[col].fillna(fill_val)
                logger.debug(f"Filled missing values in {valid_cols} with 0.")

            elif self.strategy == "fill_mean":
                for col in valid_cols:
                    if pd.api.types.is_numeric_dtype(working_df[col]):
                        mean_val = working_df[col].mean()
                        working_df[col] = working_df[col].fillna(mean_val)
                    else:
                        logger.warning(f"Column '{col}' is non-numeric. Skipping 'fill_mean' strategy.")
                logger.debug(f"Filled numeric missing values in target columns with mean.")

            elif self.strategy == "fill_constant":
                # Retrieve constant from stored config
                constant = self.config.get("constant_value", "Unknown")
                working_df[valid_cols] = working_df[valid_cols].fillna(constant)
                logger.debug(f"Filled missing values in {valid_cols} with constant: {constant}")

            else:
                logger.warning(f"Strategy '{self.strategy}' not recognized. No changes applied.")

            return working_df

        except Exception as e:
            logger.error(f"Error handling missing values: {str(e)}", exc_info=True)
            # Return original df as a defensive fallback
            return df