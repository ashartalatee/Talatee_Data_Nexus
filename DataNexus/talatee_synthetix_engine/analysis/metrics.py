import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("analysis_metrics")

class MetricsEngine:
    """
    Production-grade Metrics Engine for multi-marketplace e-commerce analytics.
    Calculates performance KPIs, growth rates, and stock-level indicators
    based on client-specific thresholds and configurations.
    """

    def __init__(self, analysis_config: Dict[str, Any]):
        """
        :param analysis_config: Dictionary containing 'metrics' list and 'thresholds'.
        """
        self.config = analysis_config
        self.metric_list = self.config.get("metrics", [])
        self.thresholds = self.config.get("thresholds", {})
        logger.info(f"MetricsEngine initialized. Requested metrics: {self.metric_list}")

    def calculate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Main orchestration for metric calculation.
        Returns a dictionary of calculated results for reporting/insights.
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame passed to MetricsEngine. Returning empty results.")
            return {}

        results = {}
        try:
            # 1. Marketplace Performance
            if "total_gmv_per_marketplace" in self.metric_list:
                results["gmv_by_marketplace"] = self._get_gmv_by_marketplace(df)

            # 2. Product Performance
            if "top_5_products" in self.metric_list:
                results["top_products"] = self._get_top_performing_products(df, limit=5)

            # 3. Efficiency Metrics
            if "avg_order_value" in self.metric_list:
                results["aov"] = self._calculate_aov(df)

            # 4. Inventory/Risk Metrics
            if "low_stock_alerts" in self.metric_list:
                results["low_stock_items"] = self._check_low_stock(df)

            # 5. Growth Metrics (requires 'order_date')
            if "mom_growth" in self.metric_list and "order_date" in df.columns:
                results["monthly_growth"] = self._calculate_mom_growth(df)

            logger.info("Metrics calculation successfully completed.")
            return results

        except Exception as e:
            logger.error(f"Critical error during metrics calculation: {str(e)}", exc_info=True)
            return results

    def _get_gmv_by_marketplace(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculates total GMV aggregated by marketplace source."""
        if "total_gmv" not in df.columns or "_marketplace_source" not in df.columns:
            return {}
        return df.groupby("_marketplace_source")["total_gmv"].sum().to_dict()

    def _get_top_performing_products(self, df: pd.DataFrame, limit: int = 5) -> pd.DataFrame:
        """Identifies top products based on total GMV."""
        if "product_name" not in df.columns or "total_gmv" not in df.columns:
            return pd.DataFrame()
        
        top_df = df.groupby("product_name")["total_gmv"].sum().sort_values(ascending=False).head(limit).reset_index()
        return top_df

    def _calculate_aov(self, df: pd.DataFrame) -> float:
        """Calculates Average Order Value (Total GMV / Total Unique Orders)."""
        gmv_col = "total_gmv" if "total_gmv" in df.columns else "price" # Fallback
        order_col = "order_id"
        
        if gmv_col not in df.columns or order_col not in df.columns:
            return 0.0
            
        total_gmv = df[gmv_col].sum()
        unique_orders = df[order_col].nunique()
        
        return float(total_gmv / unique_orders) if unique_orders > 0 else 0.0

    def _check_low_stock(self, df: pd.DataFrame) -> List[str]:
        """Identifies products falling below the configured stock threshold."""
        threshold = self.thresholds.get("low_stock", 10)
        if "stock_quantity" not in df.columns or "product_name" not in df.columns:
            return []
            
        low_stock_mask = df.groupby("product_name")["stock_quantity"].last() < threshold
        return low_stock_mask[low_stock_mask].index.tolist()

    def _calculate_mom_growth(self, df: pd.DataFrame) -> float:
        """Calculates Month-over-Month GMV growth percentage."""
        if "total_gmv" not in df.columns or "order_date" not in df.columns:
            return 0.0
            
        try:
            # Aggregate by Month
            df_monthly = df.set_index("order_date").resample('M')["total_gmv"].sum()
            if len(df_monthly) < 2:
                return 0.0
                
            current_month = df_monthly.iloc[-1]
            last_month = df_monthly.iloc[-2]
            
            if last_month == 0:
                return 0.0
                
            growth = ((current_month - last_month) / last_month) * 100
            return round(float(growth), 2)
        except Exception as e:
            logger.warning(f"MoM Growth calculation failed: {e}")
            return 0.0