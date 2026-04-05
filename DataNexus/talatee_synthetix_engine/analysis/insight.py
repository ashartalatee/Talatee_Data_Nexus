import logging
from typing import Dict, Any, List, Optional
import pandas as pd

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("analysis_insight")

class InsightGenerator:
    """
    Production-grade Insight Generator for e-commerce analytics.
    Converts raw metrics into human-readable, actionable business insights
    based on client-specific performance thresholds.
    """

    def __init__(self, analysis_config: Dict[str, Any]):
        """
        :param analysis_config: Dictionary containing 'thresholds' and 'generate_insights' flag.
        """
        self.config = analysis_config
        self.thresholds = self.config.get("thresholds", {})
        self.enabled = self.config.get("generate_insights", True)
        logger.info(f"InsightGenerator initialized. Enabled: {self.enabled}")

    def generate(self, metrics: Dict[str, Any], df: pd.DataFrame) -> List[Dict[str, str]]:
        """
        Analyzes calculated metrics and raw trends to produce actionable insights.
        Returns a list of insight dictionaries with 'type', 'message', and 'priority'.
        """
        if not self.enabled:
            logger.debug("Insight generation is disabled in config.")
            return []

        insights: List[Dict[str, str]] = []

        try:
            # 1. Growth Insights
            if "monthly_growth" in metrics:
                insights.append(self._analyze_growth(metrics["monthly_growth"]))

            # 2. GMV Concentration Insights
            if "gmv_by_marketplace" in metrics:
                insights.extend(self._analyze_marketplace_dominance(metrics["gmv_by_marketplace"]))

            # 3. Product Performance Insights
            if "top_products" in metrics:
                insights.append(self._analyze_product_concentration(metrics["top_products"], metrics.get("gmv_by_marketplace", {})))

            # 4. Inventory Risk Insights
            if "low_stock_items" in metrics:
                insights.extend(self._analyze_stock_risk(metrics["low_stock_items"]))

            # 5. Operational Health
            insights.extend(self._analyze_operational_health(df))

            # Filter out None values and sort by priority
            active_insights = [i for i in insights if i is not None]
            logger.info(f"Generated {len(active_insights)} actionable insights.")
            
            return active_insights

        except Exception as e:
            logger.error(f"Critical error generating insights: {str(e)}", exc_info=True)
            return []

    def _analyze_growth(self, growth_val: float) -> Optional[Dict[str, str]]:
        """Evaluates Month-over-Month growth performance."""
        target = self.thresholds.get("growth_target", 10.0)
        
        if growth_val > target:
            return {
                "type": "PERFORMANCE_POSITIVE",
                "message": f"Strong MoM growth of {growth_val}%, exceeding target of {target}%.",
                "priority": "LOW",
                "action": "Maintain current marketing strategy and check fulfillment capacity."
            }
        elif growth_val < 0:
            return {
                "type": "PERFORMANCE_CRITICAL",
                "message": f"Negative growth detected ({growth_val}%). Revenue is declining compared to last month.",
                "priority": "HIGH",
                "action": "Investigate SKU performance drops or marketplace algorithm changes."
            }
        return None

    def _analyze_marketplace_dominance(self, marketplace_gmv: Dict[str, float]) -> List[Dict[str, str]]:
        """Checks if the business is too dependent on a single marketplace."""
        if not marketplace_gmv:
            return []
            
        insights = []
        total_gmv = sum(marketplace_gmv.values())
        risk_threshold = self.thresholds.get("dependency_threshold", 0.7) # 70%
        
        for mkp, gmv in marketplace_gmv.items():
            share = gmv / total_gmv if total_gmv > 0 else 0
            if share > risk_threshold:
                insights.append({
                    "type": "RISK_DEPENDENCY",
                    "message": f"High revenue dependency on {mkp.upper()} ({share*100:.1f}% of total GMV).",
                    "priority": "MEDIUM",
                    "action": f"Diversify sales by increasing ad spend or promos on other platforms."
                })
        return insights

    def _analyze_product_concentration(self, top_products: pd.DataFrame, gmv_data: Dict[str, float]) -> Optional[Dict[str, str]]:
        """Detects if 80% of revenue comes from too few products (Pareto Risk)."""
        if top_products.empty or not gmv_data:
            return None
            
        total_gmv = sum(gmv_data.values())
        top_product_gmv = top_products["total_gmv"].iloc[0]
        concentration_ratio = top_product_gmv / total_gmv if total_gmv > 0 else 0
        
        if concentration_ratio > 0.4:
            return {
                "type": "PRODUCT_RISK",
                "message": f"Top product '{top_products['product_name'].iloc[0]}' contributes {concentration_ratio*100:.1f}% of total GMV.",
                "priority": "MEDIUM",
                "action": "Develop 'Hero Product 2' to mitigate risk if this item goes out of stock or faces competition."
            }
        return None

    def _analyze_stock_risk(self, low_stock_list: List[str]) -> List[Dict[str, str]]:
        """Flags inventory shortages."""
        if not low_stock_list:
            return []
            
        return [{
            "type": "INVENTORY_ALERT",
            "message": f"{len(low_stock_list)} SKUs are below safety stock levels.",
            "priority": "HIGH",
            "action": f"Restock following items immediately: {', '.join(low_stock_list[:3])}..."
        }]

    def _analyze_operational_health(self, df: pd.DataFrame) -> List[Dict[str, str]]:
        """Analyzes order status trends (e.g., cancellation rates)."""
        insights = []
        if "order_status" not in df.columns:
            return []
            
        stats = df["order_status"].value_counts(normalize=True)
        cancel_rate = stats.get("CANCELLED", 0) + stats.get("CANCELED", 0)
        
        if cancel_rate > self.thresholds.get("max_cancel_rate", 0.05):
            insights.append({
                "type": "OPERATIONAL_EFFICIENCY",
                "message": f"High cancellation rate detected ({cancel_rate*100:.1f}%).",
                "priority": "HIGH",
                "action": "Review inventory sync accuracy and warehouse response time."
            })
            
        return insights