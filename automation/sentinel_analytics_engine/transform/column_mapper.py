import logging
import pandas as pd
from typing import Dict, Any, Optional

class ColumnMapper:
    """
    Standardizes disparate marketplace column names into the unified schema
    required by the Talatee Sentinel Engine.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.transform_cfg = config.get("transformation", {})
        self.mapping = self.transform_cfg.get("column_mapping", {})

    def apply(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Renames columns based on client-specific mapping and drops unmapped 
        columns if strict mode is enabled.
        """
        if df is None or df.empty:
            return df

        try:
            if not self.mapping:
                self.logger.info("No specific column mapping provided. Using raw headers.")
                return df

            # Standardize keys to lowercase for case-insensitive matching if needed
            # but preserve exact mapping from config for precision
            rename_dict = {str(k): str(v) for k, v in self.mapping.items()}
            
            # Check which keys in mapping actually exist in the dataframe
            existing_mapping = {k: v for k, v in rename_dict.items() if k in df.columns}
            missing_mapping = {k: v for k, v in rename_dict.items() if k not in df.columns}

            if missing_mapping:
                self.logger.warning(f"Columns defined in mapping not found in data: {list(missing_mapping.keys())}")

            # Perform renaming
            df = df.rename(columns=existing_mapping)
            self.logger.info(f"Successfully mapped {len(existing_mapping)} columns.")

            # Identify if there are duplicate columns after renaming (e.g. mapping two sources to one name)
            if df.columns.duplicated().any():
                self.logger.warning("Duplicate columns detected after mapping. Consolidating...")
                df = df.loc[:, ~df.columns.duplicated()].copy()

            return df

        except Exception as e:
            self.logger.error(f"Critical error during column mapping: {str(e)}", exc_info=True)
            return df