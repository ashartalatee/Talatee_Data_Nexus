import pandas as pd
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


def total_revenue(df: pd.DataFrame, revenue_col: str = "revenue") -> float:
    try:
        if revenue_col not in df.columns:
            logger.warning("Revenue column missing")
            return 0.0
        return float(pd.to_numeric(df[revenue_col], errors="coerce").fillna(0).sum())
    except Exception as e:
        logger.error(f"Error calculating total revenue: {e}")
        return 0.0


def total_orders(df: pd.DataFrame, order_id_col: str = "order_id") -> int:
    try:
        if order_id_col not in df.columns:
            logger.warning("Order ID column missing")
            return 0
        return int(df[order_id_col].nunique())
    except Exception as e:
        logger.error(f"Error calculating total orders: {e}")
        return 0


def average_order_value(
    df: pd.DataFrame,
    revenue_col: str = "revenue",
    order_id_col: str = "order_id"
) -> float:
    try:
        revenue = total_revenue(df, revenue_col)
        orders = total_orders(df, order_id_col)
        return revenue / orders if orders else 0.0
    except Exception as e:
        logger.error(f"Error calculating AOV: {e}")
        return 0.0


def total_profit(
    df: pd.DataFrame,
    profit_col: str = "profit"
) -> float:
    try:
        if profit_col not in df.columns:
            logger.warning("Profit column missing")
            return 0.0
        return float(pd.to_numeric(df[profit_col], errors="coerce").fillna(0).sum())
    except Exception as e:
        logger.error(f"Error calculating total profit: {e}")
        return 0.0


def conversion_rate(
    df: pd.DataFrame,
    orders_col: str = "orders",
    visitors_col: str = "visitors"
) -> float:
    try:
        if orders_col not in df.columns or visitors_col not in df.columns:
            logger.warning("Missing columns for conversion rate")
            return 0.0

        orders = pd.to_numeric(df[orders_col], errors="coerce").fillna(0).sum()
        visitors = pd.to_numeric(df[visitors_col], errors="coerce").fillna(0).sum()

        return float(orders / visitors) if visitors else 0.0

    except Exception as e:
        logger.error(f"Error calculating conversion rate: {e}")
        return 0.0


def run_metrics(
    df: pd.DataFrame,
    config: Optional[Dict] = None
) -> Dict:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_metrics")
            return {}

        metrics = {}

        if config:
            if config.get("total_revenue"):
                metrics["total_revenue"] = total_revenue(
                    df,
                    revenue_col=config["total_revenue"].get("revenue_col", "revenue")
                )

            if config.get("total_orders"):
                metrics["total_orders"] = total_orders(
                    df,
                    order_id_col=config["total_orders"].get("order_id_col", "order_id")
                )

            if config.get("aov"):
                metrics["aov"] = average_order_value(
                    df,
                    revenue_col=config["aov"].get("revenue_col", "revenue"),
                    order_id_col=config["aov"].get("order_id_col", "order_id")
                )

            if config.get("total_profit"):
                metrics["total_profit"] = total_profit(
                    df,
                    profit_col=config["total_profit"].get("profit_col", "profit")
                )

            if config.get("conversion_rate"):
                metrics["conversion_rate"] = conversion_rate(
                    df,
                    orders_col=config["conversion_rate"].get("orders_col", "orders"),
                    visitors_col=config["conversion_rate"].get("visitors_col", "visitors")
                )

        logger.info(f"Metrics calculated: {metrics}")
        return metrics

    except Exception as e:
        logger.error(f"Error in metrics pipeline: {e}")
        return {}