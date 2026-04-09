import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional

class InsightEngine:
    """
    Generates actionable business insights by analyzing trends, 
    identifying correlations, and performing comparative analysis.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.insight_cfg = config.get("analysis", {}).get("insights", {})

    def extract(self, df: pd.DataFrame, metrics_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Main execution point for qualitative insight extraction.
        """
        if df is None or df.empty or metrics_df is None or metrics_df.empty:
            self.logger.warning("Insight extraction aborted: Data is insufficient.")
            return []

        insights = []

        try:
            # 1. Growth & Trend Analysis
            growth_insights = self._analyze_growth_trends(metrics_df)
            if growth_insights:
                insights.append(growth_insights)

            # 2. Marketplace Contribution Analysis
            platform_insights = self._analyze_platform_dominance(metrics_df)
            if platform_insights:
                insights.append(platform_insights)

            # 3. Product Performance Insights
            product_insights = self._analyze_product_concentration(df)
            if product_insights:
                insights.append(product_insights)

            self.logger.info(f"Generated {len(insights)} high-level insights.")
            return insights

        except Exception as e:
            self.logger.error(f"Failed to extract insights: {str(e)}", exc_info=True)
            return []

    def _analyze_growth_trends(self, metrics_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Analyzes MoM or WoW growth depending on data granularity."""
        if "gmv" not in metrics_df.columns or len(metrics_df) < 2:
            return None

        # Calculate percentage change between the last two periods
        latest_gmv = metrics_df["gmv"].iloc[-1]
        previous_gmv = metrics_df["gmv"].iloc[-2]
        
        growth_pct = ((latest_gmv - previous_gmv) / previous_gmv) * 100 if previous_gmv != 0 else 0
        
        status = "growth" if growth_pct > 0 else "decline"
        
        return {
            "type": "trend_analysis",
            "metric": "GMV",
            "value": round(growth_pct, 2),
            "label": f"{abs(round(growth_pct, 2))}% {status} compared to the previous period.",
            "severity": "high" if abs(growth_pct) > 20 else "medium"
        }

    def _analyze_platform_dominance(self, metrics_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Identifies which marketplace is driving the majority of revenue."""
        if "platform" not in metrics_df.columns or "gmv" not in metrics_df.columns:
            return None

        platform_totals = metrics_df.groupby("platform")["gmv"].sum()
        if platform_totals.empty:
            return None
            
        top_platform = platform_totals.idxmax()
        contribution_pct = (platform_totals.max() / platform_totals.sum()) * 100

        return {
            "type": "distribution_insight",
            "primary_source": top_platform,
            "contribution_pct": round(contribution_pct, 2),
            "label": f"{top_platform} is the dominant channel, contributing {round(contribution_pct, 2)}% of total revenue."
        }

    def _analyze_product_concentration(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Calculates Pareto-style insights (e.g., top 20% products driving 80% revenue)."""
        if "sku" not in df.columns or "total_price" not in df.columns:
            return None

        sku_revenue = df.groupby("sku")["total_price"].sum().sort_values(ascending=False)
        total_rev = sku_revenue.sum()
        
        if total_rev == 0:
            return None

        # Find how many SKUs make up 80% of revenue
        cumulative_rev = sku_revenue.cumsum() / total_rev
        top_performers_count = (cumulative_rev <= 0.8).sum() + 1
        sku_pct = (top_performers_count / len(sku_revenue)) * 100

        return {
            "type": "inventory_insight",
            "label": f"{round(sku_pct, 2)}% of your SKUs are generating 80% of your total revenue.",
            "top_sku": sku_revenue.index[0],
            "recommendation": "Ensure high stock levels for top-performing SKUs to avoid stockouts."
        }