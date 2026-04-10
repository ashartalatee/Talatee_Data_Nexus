import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

class FeatureEngineer:
    """
    Computes business-level metrics and derived features required for 
    high-level e-commerce decision-making.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.feat_cfg = config.get("transformation", {}).get("feature_engineering", {})

    def transform(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Orchestrates the calculation of derived fields like margins, taxes, 
        and time-based groupings.
        """
        if df is None or df.empty:
            return df

        try:
            self.logger.info("Starting feature engineering phase.")

            # 1. Net Revenue / GMV Logic
            if "total_price" in df.columns:
                df["is_high_value"] = df["total_price"] > df["total_price"].median()

            # 2. Margin Calculation
            if self.feat_cfg.get("calculate_margin", False):
                df = self._calculate_margins(df)

            # 3. Tax Implementation
            tax_rate = self.feat_cfg.get("tax_rate", 0.0)
            if tax_rate > 0 and "total_price" in df.columns:
                self.logger.info(f"Applying tax rate of {tax_rate}")
                df["tax_amount"] = df["total_price"] * tax_rate
                df["total_price_inc_tax"] = df["total_price"] + df["tax_amount"]

            # 4. Temporal Features
            if "transaction_date" in df.columns:
                # Pastikan kolom sudah datetime sebelum operasi .dt
                df["transaction_date"] = pd.to_datetime(df["transaction_date"])
                df["day_name"] = df["transaction_date"].dt.day_name()
                df["hour_of_day"] = df["transaction_date"].dt.hour
                df["is_weekend"] = df["transaction_date"].dt.dayofweek >= 5
                df["month_year"] = df["transaction_date"].dt.to_period('M').astype(str)

            # 5. Categorical Bucketings
            if self.feat_cfg.get("categorize_by_region", False) and "customer_city" in df.columns:
                df["region_segment"] = df["customer_city"].apply(self._map_region)

            self.logger.info("Feature engineering completed successfully.")
            return df

        except Exception as e:
            self.logger.error(f"Critical error during feature engineering: {str(e)}", exc_info=True)
            return df

    def _calculate_margins(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates profit margins if COGS (cost of goods sold) is available."""
        if all(col in df.columns for col in ["total_price", "cogs"]):
            df["profit_margin_abs"] = df["total_price"] - df["cogs"]
            df["profit_margin_pct"] = (df["profit_margin_abs"] / df["total_price"]).replace([np.inf, -np.inf], 0)
        else:
            self.logger.warning("COGS or total_price missing; skipping margin calculation.")
        return df

    def _map_region(self, city: str) -> str:
        """Helper to segment cities into business regions."""
        if not city:
            return "Unknown"
        city_lower = str(city).lower()
        if any(x in city_lower for x in ["jakarta", "bogor", "depok", "tangerang", "bekasi"]):
            return "Jabodetabek"
        if any(x in city_lower for x in ["surabaya", "malang", "sidoarjo"]):
            return "East Java"
        return "Other Regions"

# --- Penyelamat Import Error ---
# Alias agar runner.py bisa memanggil 'from ... import DataTransformer'
DataTransformer = FeatureEngineer