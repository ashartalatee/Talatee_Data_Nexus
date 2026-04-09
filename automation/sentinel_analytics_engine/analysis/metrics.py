import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

class MetricsEngine:
    """
    Calculates key performance indicators (KPIs) and business metrics 
    for e-commerce marketplace performance evaluation.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.analysis_cfg = config.get("analysis", {})
        self.metrics_list = self.analysis_cfg.get("metrics", ["gmv", "total_orders"])

    def calculate(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Generates a summary metrics table based on configured granularity 
        (e.g., daily, weekly, monthly).
        """
        if df is None or df.empty:
            self.logger.warning("Metrics calculation skipped: DataFrame is empty.")
            return None

        try:
            self.logger.info(f"Calculating metrics: {self.metrics_list}")
            
            granularity = self.analysis_cfg.get("granularity", "daily").lower()
            group_cols = []

            # 1. Determine Time Grouping
            if "transaction_date" in df.columns:
                if granularity == "daily":
                    df["_period"] = df["transaction_date"].dt.date
                elif granularity == "weekly":
                    df["_period"] = df["transaction_date"].dt.to_period('W').apply(lambda r: r.start_time)
                elif granularity == "monthly":
                    df["_period"] = df["transaction_date"].dt.to_period('M').apply(lambda r: r.start_time)
                group_cols.append("_period")

            # 2. Add Platform Grouping for Multi-Marketplace clarity
            if "platform" in df.columns:
                group_cols.append("platform")

            if not group_cols:
                self.logger.warning("No grouping columns available. Calculating global totals.")
                return self._compute_aggregates(df)

            # 3. Perform Aggregation
            metrics_df = df.groupby(group_cols).apply(self._compute_aggregates).reset_index()
            
            # Cleanup internal column
            if "_period" in metrics_df.columns:
                metrics_df = metrics_df.rename(columns={"_period": "report_date"})

            self.logger.info(f"Metrics calculation complete. Generated {len(metrics_df)} summary rows.")
            return metrics_df

        except Exception as e:
            self.logger.error(f"Critical error in MetricsEngine: {str(e)}", exc_info=True)
            return None

    def _compute_aggregates(self, group: pd.DataFrame) -> pd.Series:
        """
        Internal logic to compute specific business KPIs for a data group.
        """
        results = {}

        # GMV (Gross Merchandise Value)
        if "gmv" in self.metrics_list and "total_price" in group.columns:
            results["gmv"] = group["total_price"].sum()

        # Total Orders
        if "total_orders" in self.metrics_list or "order_volume" in self.metrics_list:
            results["total_orders"] = group["order_id"].nunique() if "order_id" in group.columns else len(group)

        # Average Order Value (AOV)
        if "average_order_value" in self.metrics_list and "gmv" in results:
            results["aov"] = results["gmv"] / results["total_orders"] if results["total_orders"] > 0 else 0

        # SKU Performance (Top Selling SKU)
        if "sku_performance" in self.metrics_list and "sku" in group.columns:
            results["top_sku"] = group["sku"].mode().iloc[0] if not group["sku"].mode().empty else "N/A"
            results["unique_skus"] = group["sku"].nunique()

        # Quantity Sold
        if "quantity" in group.columns:
            results["units_sold"] = group["quantity"].sum()

        return pd.Series(results)