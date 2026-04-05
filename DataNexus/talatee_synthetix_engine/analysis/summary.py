import logging
from typing import Dict, Any, Optional
import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("analysis_summary")

class SummaryBuilder:
    """
    Production-grade Summary Builder.
    Aggregates metrics and raw data into high-level executive summaries 
    for multi-client reporting.
    """

    def __init__(self, analysis_config: Dict[str, Any]):
        """
        :param analysis_config: Dictionary containing summary preferences.
        """
        self.config = analysis_config
        self.include_raw_samples = self.config.get("include_samples", False)
        logger.info("SummaryBuilder initialized.")

    def build_executive_summary(self, df: pd.DataFrame, metrics: Dict[str, Any], insights: list) -> Dict[str, Any]:
        """
        Constructs a structured summary dictionary containing core KPIs,
        top performing segments, and a count of critical alerts.
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame passed to SummaryBuilder.")
            return {"status": "error", "message": "No data available"}

        try:
            summary = {
                "metadata": {
                    "total_records": len(df),
                    "unique_orders": df["order_id"].nunique() if "order_id" in df.columns else 0,
                    "marketplaces_covered": list(df["_marketplace_source"].unique()) if "_marketplace_source" in df.columns else []
                },
                "key_performance_indicators": {
                    "gmv": metrics.get("gmv_by_marketplace", {}),
                    "aov": metrics.get("aov", 0.0),
                    "growth_rate": metrics.get("monthly_growth", 0.0)
                },
                "top_line_items": metrics.get("top_products", pd.DataFrame()).to_dict(orient="records"),
                "action_items_count": len(insights),
                "critical_alerts": [i for i in insights if i.get("priority") == "HIGH"]
            }

            if self.include_raw_samples:
                summary["data_preview"] = df.head(10).to_dict(orient="records")

            logger.info("Executive summary successfully built.")
            return summary

        except Exception as e:
            logger.error(f"Error building executive summary: {str(e)}", exc_info=True)
            return {"status": "error", "detail": str(e)}

    def build_marketplace_breakdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates a granular table comparing marketplace performance side-by-side.
        """
        if "_marketplace_source" not in df.columns:
            return pd.DataFrame()

        try:
            # Aggregate core columns by marketplace
            agg_map = {
                "order_id": "nunique",
                "total_gmv": "sum",
                "quantity": "sum"
            }
            # Only use columns that exist
            existing_agg = {k: v for k, v in agg_map.items() if k in df.columns}
            
            breakdown = df.groupby("_marketplace_source").agg(existing_agg).reset_index()
            breakdown.columns = [
                "Marketplace", 
                "Total Orders", 
                "Total GMV", 
                "Items Sold"
            ][:len(breakdown.columns)]
            
            return breakdown
        except Exception as e:
            logger.error(f"Marketplace breakdown failed: {e}")
            return pd.DataFrame()