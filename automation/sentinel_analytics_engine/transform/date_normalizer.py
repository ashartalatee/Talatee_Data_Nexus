import logging
import pandas as pd
from typing import Dict, Any, Optional

class DateNormalizer:
    """
    Standardizes transaction dates across different marketplace formats 
    and handles timezone localization for client-ready reporting.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.transform_cfg = config.get("transformation", {})
        self.target_tz = self.transform_cfg.get("timezone", "UTC")
        self.date_col = "transaction_date"

    def process(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Converts date columns to datetime objects and normalizes timezones.
        """
        if df is None or df.empty:
            return df

        if self.date_col not in df.columns:
            self.logger.warning(f"Date column '{self.date_col}' not found. Skipping normalization.")
            return df

        try:
            # 1. Convert to datetime with coercion for safety
            self.logger.info(f"Normalizing dates to {self.target_tz} timezone.")
            df[self.date_col] = pd.to_datetime(df[self.date_col], errors='coerce')

            # 2. Drop records with unparseable dates if in strict mode
            invalid_dates = df[self.date_col].isna()
            if invalid_dates.any():
                self.logger.error(f"Detected {invalid_dates.sum()} unparseable dates.")
                if self.config.get("strict_mode", True):
                    df = df.dropna(subset=[self.date_col]).copy()

            # 3. Timezone Localization & Conversion
            # Check if dates are already tz-aware
            if df[self.date_col].dt.tz is None:
                # Assume UTC if naive, then convert to target
                df[self.date_col] = df[self.date_col].dt.tz_localize('UTC').dt.tz_convert(self.target_tz)
            else:
                # Direct conversion if already aware
                df[self.date_col] = df[self.date_col].dt.tz_convert(self.target_tz)

            # 4. Remove TZ info for Excel/CSV compatibility if requested (rendering as naive local)
            # Standard agency requirement: Naive datetime representing the client's local time
            df[self.date_col] = df[self.date_col].dt.tz_localize(None)

            return df

        except Exception as e:
            self.logger.error(f"Critical failure in DateNormalizer: {str(e)}", exc_info=True)
            return df