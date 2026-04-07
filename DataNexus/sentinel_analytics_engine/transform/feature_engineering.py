import pandas as pd
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


def calculate_revenue(
    df: pd.DataFrame,
    price_col: str,
    quantity_col: str,
    output_col: str = "revenue"
) -> pd.DataFrame:
    try:
        if price_col not in df.columns or quantity_col not in df.columns:
            logger.warning("Missing columns for revenue calculation")
            df[output_col] = None
            return df

        df[output_col] = (
            pd.to_numeric(df[price_col], errors="coerce").fillna(0) *
            pd.to_numeric(df[quantity_col], errors="coerce").fillna(0)
        )

        return df

    except Exception as e:
        logger.error(f"Error calculating revenue: {e}")
        df[output_col] = None
        return df


def calculate_average_order_value(
    df: pd.DataFrame,
    revenue_col: str = "revenue",
    order_id_col: str = "order_id",
    output_col: str = "aov"
) -> pd.DataFrame:
    try:
        if revenue_col not in df.columns or order_id_col not in df.columns:
            logger.warning("Missing columns for AOV calculation")
            df[output_col] = None
            return df

        total_revenue = df[revenue_col].sum()
        total_orders = df[order_id_col].nunique()

        df[output_col] = total_revenue / total_orders if total_orders else 0

        return df

    except Exception as e:
        logger.error(f"Error calculating AOV: {e}")
        df[output_col] = None
        return df


def calculate_conversion_rate(
    df: pd.DataFrame,
    orders_col: str,
    visitors_col: str,
    output_col: str = "conversion_rate"
) -> pd.DataFrame:
    try:
        if orders_col not in df.columns or visitors_col not in df.columns:
            logger.warning("Missing columns for conversion rate")
            df[output_col] = None
            return df

        orders = pd.to_numeric(df[orders_col], errors="coerce").fillna(0).sum()
        visitors = pd.to_numeric(df[visitors_col], errors="coerce").fillna(0).sum()

        df[output_col] = (orders / visitors) if visitors else 0

        return df

    except Exception as e:
        logger.error(f"Error calculating conversion rate: {e}")
        df[output_col] = None
        return df


def calculate_profit(
    df: pd.DataFrame,
    revenue_col: str = "revenue",
    cost_col: str = "cost",
    output_col: str = "profit"
) -> pd.DataFrame:
    try:
        if revenue_col not in df.columns or cost_col not in df.columns:
            logger.warning("Missing columns for profit calculation")
            df[output_col] = None
            return df

        df[output_col] = (
            pd.to_numeric(df[revenue_col], errors="coerce").fillna(0) -
            pd.to_numeric(df[cost_col], errors="coerce").fillna(0)
        )

        return df

    except Exception as e:
        logger.error(f"Error calculating profit: {e}")
        df[output_col] = None
        return df


def run_feature_engineering(
    df: pd.DataFrame,
    config: Optional[Dict] = None
) -> Optional[pd.DataFrame]:
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame in run_feature_engineering")
            return pd.DataFrame()

        if config:
            if config.get("revenue"):
                df = calculate_revenue(
                    df,
                    price_col=config["revenue"].get("price_col", "price"),
                    quantity_col=config["revenue"].get("quantity_col", "quantity"),
                    output_col=config["revenue"].get("output_col", "revenue")
                )

            if df.empty:
                return df

            if config.get("profit"):
                df = calculate_profit(
                    df,
                    revenue_col=config["profit"].get("revenue_col", "revenue"),
                    cost_col=config["profit"].get("cost_col", "cost"),
                    output_col=config["profit"].get("output_col", "profit")
                )

            if df.empty:
                return df

            if config.get("aov"):
                df = calculate_average_order_value(
                    df,
                    revenue_col=config["aov"].get("revenue_col", "revenue"),
                    order_id_col=config["aov"].get("order_id_col", "order_id"),
                    output_col=config["aov"].get("output_col", "aov")
                )

            if df.empty:
                return df

            if config.get("conversion_rate"):
                df = calculate_conversion_rate(
                    df,
                    orders_col=config["conversion_rate"].get("orders_col", "orders"),
                    visitors_col=config["conversion_rate"].get("visitors_col", "visitors"),
                    output_col=config["conversion_rate"].get("output_col", "conversion_rate")
                )

            if df.empty:
                return df

        logger.info("Feature engineering completed")
        return df

    except Exception as e:
        logger.error(f"Error in feature engineering pipeline: {e}")
        return pd.DataFrame()