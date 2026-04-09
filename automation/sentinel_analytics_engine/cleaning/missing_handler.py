import logging
import pandas as pd
from typing import Dict, Any, Optional

class MissingHandler:
    """
    Handles null and missing values based on client-specific configurations.
    Supports 'fill' with constants and 'drop' strategies.
    """
    def __init__(self, clean_cfg: Dict[str, Any], logger: logging.Logger):
        self.config = clean_cfg.get("handle_missing", {})
        self.logger = logger

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes missing value treatment.
        """
        if df is None or df.empty:
            return df

        strategy = self.config.get("strategy", "fill").lower()
        
        try:
            if strategy == "fill":
                return self._fill_missing(df)
            elif strategy == "drop":
                return self._drop_missing(df)
            else:
                self.logger.warning(f"Unknown missing handler strategy '{strategy}'. Defaulting to fill.")
                return self._fill_missing(df)
        except Exception as e:
            self.logger.error(f"Error during missing value handling: {str(e)}")
            return df

    def _fill_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing values based on a column-to-value mapping.
        """
        fill_map = self.config.get("columns", {})
        if not fill_map:
            # Fallback: fill numeric with 0 and objects with 'Unknown'
            for col in df.columns:
                if df[col].dtype in ['int64', 'float64']:
                    df[col] = df[col].fillna(0)
                else:
                    df[col] = df[col].fillna("Unknown")
            return df

        self.logger.info(f"Filling missing values using map: {fill_map}")
        return df.fillna(value=fill_map)

    def _drop_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drops rows where specific critical columns have null values.
        """
        target_cols = self.config.get("columns", [])
        
        # If columns are a dict (from fill config), convert keys to list
        if isinstance(target_cols, dict):
            target_cols = list(target_cols.keys())
            
        initial_count = len(df)
        
        if not target_cols:
            # Drop rows where ALL values are missing
            df = df.dropna(how='all')
        else:
            # Only check presence in existing columns to avoid KeyErrors
            existing_targets = [c for c in target_cols if c in df.columns]
            df = df.dropna(subset=existing_targets)

        dropped_count = initial_count - len(df)
        if dropped_count > 0:
            self.logger.info(f"Dropped {dropped_count} rows due to missing critical data.")
            
        return df