import pandas as pd
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def revenue_insight(metrics: Dict) -> Dict:
    try:
        revenue = metrics.get("total_revenue", 0)

        if revenue == 0:
            return {"revenue_insight": "No revenue generated"}

        if revenue < 1_000_000:
            return {"revenue_insight": "Revenue is low, consider increasing traffic or pricing strategy"}

        if revenue < 10_000_000:
            return {"revenue_insight": "Revenue is moderate, optimize conversion to scale"}

        return {"revenue_insight": "Revenue is strong, focus on scaling and retention"}

    except Exception as e:
        logger.error(f"Error generating revenue insight: {e}")
        return {}


def profit_insight(metrics: Dict) -> Dict:
    try:
        profit = metrics.get("total_profit", 0)

        if profit <= 0:
            return {"profit_insight": "Business is not profitable, review costs and pricing"}

        if profit < 500_000:
            return {"profit_insight": "Profit is low, optimize operational efficiency"}

        return {"profit_insight": "Profit is healthy, consider reinvesting for growth"}

    except Exception as e:
        logger.error(f"Error generating profit insight: {e}")
        return {}


def conversion_insight(metrics: Dict) -> Dict:
    try:
        cr = metrics.get("conversion_rate", 0)

        if cr == 0:
            return {"conversion_insight": "No conversions, check funnel or tracking issues"}

        if cr < 0.01:
            return {"conversion_insight": "Conversion rate is very low, improve landing page or targeting"}

        if cr < 0.03:
            return {"conversion_insight": "Conversion rate is average, test creatives and offers"}

        return {"conversion_insight": "Conversion rate is strong, scale traffic sources"}

    except Exception as e:
        logger.error(f"Error generating conversion insight: {e}")
        return {}


def aov_insight(metrics: Dict) -> Dict:
    try:
        aov = metrics.get("aov", 0)

        if aov == 0:
            return {"aov_insight": "No AOV data available"}

        if aov < 50000:
            return {"aov_insight": "AOV is low, consider bundling or upselling"}

        if aov < 150000:
            return {"aov_insight": "AOV is moderate, test pricing strategies"}

        return {"aov_insight": "AOV is strong, maintain pricing and increase traffic"}

    except Exception as e:
        logger.error(f"Error generating AOV insight: {e}")
        return {}


def run_insight(
    df: pd.DataFrame,
    metrics: Dict,
    config: Optional[Dict] = None
) -> Dict:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_insight")
            return {}

        insights = {}

        if config:
            if config.get("revenue"):
                insights.update(revenue_insight(metrics))

            if config.get("profit"):
                insights.update(profit_insight(metrics))

            if config.get("conversion"):
                insights.update(conversion_insight(metrics))

            if config.get("aov"):
                insights.update(aov_insight(metrics))

        logger.info(f"Insights generated: {insights}")
        return insights

    except Exception as e:
        logger.error(f"Error in insight pipeline: {e}")
        return {}