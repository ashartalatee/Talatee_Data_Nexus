import logging
import pandas as pd
from typing import Dict, Any, Optional

class InsightGenerator:
    """
    Generates automated business insights and high-level summaries 
    for client-ready e-commerce reporting.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.analysis_cfg = config.get("analysis", {})

    def generate(self, df: pd.DataFrame, metrics_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """
        Analyzes processed data and metrics to extract meaningful executive summaries.
        Returns a dictionary of insights.
        """
        if df is None or df.empty or metrics_df is None or metrics_df.empty:
            self.logger.warning("Insight generation skipped: Insufficient data.")
            return None

        try:
            self.logger.info("Generating automated business insights.")
            
            insights = {
                "executive_summary": self._create_executive_summary(metrics_df),
                "top_performers": self._get_top_performers(df),
                "platform_distribution": self._get_platform_mix(metrics_df),
                "anomalies": self._detect_basic_anomalies(metrics_df)
            }

            self.logger.info("Insight generation completed.")
            return insights

        except Exception as e:
            self.logger.error(f"Error generating insights: {str(e)}", exc_info=True)
            return None

    def _create_executive_summary(self, metrics_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculates total period performance."""
        summary = {
            "total_gmv": float(metrics_df["gmv"].sum()) if "gmv" in metrics_df.columns else 0.0,
            "total_orders": int(metrics_df["total_orders"].sum()) if "total_orders" in metrics_df.columns else 0,
            "avg_daily_gmv": float(metrics_df["gmv"].mean()) if "gmv" in metrics_df.columns else 0.0
        }
        
        if "aov" in metrics_df.columns:
            summary["avg_order_value"] = float(metrics_df["aov"].mean())
            
        return summary

    def _get_top_performers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Identifies top products and regions."""
        performers = {}
        
        if "sku" in df.columns:
            top_sku = df.groupby("sku")["total_price"].sum().nlargest(3).to_dict()
            performers["top_3_skus_by_revenue"] = top_sku

        if "region_segment" in df.columns:
            top_region = df.groupby("region_segment")["order_id"].nunique().idxmax()
            performers["top_region_by_volume"] = top_region

        return performers

    def _get_platform_mix(self, metrics_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculates revenue contribution per marketplace."""
        if "platform" in metrics_df.columns and "gmv" in metrics_df.columns:
            mix = metrics_df.groupby("platform")["gmv"].sum()
            total = mix.sum()
            return ((mix / total) * 100).round(2).to_dict()
        return {}

    def _detect_basic_anomalies(self, metrics_df: pd.DataFrame) -> List[str]:
        """Identifies significant performance dips or spikes."""
        alerts = []
        if "gmv" in metrics_df.columns and len(metrics_df) > 1:
            avg_gmv = metrics_df["gmv"].mean()
            last_gmv = metrics_df["gmv"].iloc[-1]
            
            if last_gmv < (avg_gmv * 0.5):
                alerts.append("Significant drop in GMV detected in the latest period.")
            elif last_gmv > (avg_gmv * 1.5):
                alerts.append("Significant spike in GMV detected in the latest period.")
                
        return alerts