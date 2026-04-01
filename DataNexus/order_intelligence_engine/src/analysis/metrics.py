# =========================
#  ANALYTICS & METRICS MODULE (PRODUCTION READY)
# =========================

import pandas as pd

#  CONFIG
from config.settings import ANALYTICS_CONFIG

#  LOGGER
from utils.logger import setup_logger

logger = setup_logger("metrics")


# =========================
# 💰 CORE METRICS
# =========================

def total_revenue(df: pd.DataFrame) -> float:
    if "revenue" not in df.columns:
        return 0.0
    return df["revenue"].sum()


def total_orders(df: pd.DataFrame) -> int:
    return len(df)


def avg_order_value(df: pd.DataFrame) -> float:
    if "revenue" not in df.columns or len(df) == 0:
        return 0.0
    return df["revenue"].mean()


# =========================
# 📦 PRODUCT METRICS
# =========================

def top_products(df: pd.DataFrame, n: int = None) -> pd.DataFrame:
    """
    Top produk berdasarkan revenue
    """

    if "product_name" not in df.columns or "revenue" not in df.columns:
        return pd.DataFrame()

    n = n or ANALYTICS_CONFIG.get("top_n", 5)

    result = (
        df.groupby("product_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )

    return result


def get_top_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Semua ranking produk
    """

    if "product_name" not in df.columns or "revenue" not in df.columns:
        return pd.DataFrame()

    return (
        df.groupby("product_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )


# =========================
# 🚚 ORDER BEHAVIOR METRICS
# =========================

def bulk_order_ratio(df: pd.DataFrame) -> float:
    """
    Rasio order dalam jumlah besar
    """

    if "is_bulk_order" not in df.columns or len(df) == 0:
        return 0.0

    return df["is_bulk_order"].sum() / len(df)


# =========================
# 🧾 GENERATE SUMMARY DICT (UNTUK EXPORT/API)
# =========================

def generate_summary(df: pd.DataFrame) -> dict:
    """
    Summary untuk export / API / dashboard
    """

    summary = {
        "total_revenue": total_revenue(df),
        "total_orders": total_orders(df),
        "avg_order_value": avg_order_value(df),
        "bulk_order_ratio": bulk_order_ratio(df)
    }

    return summary


# =========================
# 🖨️ PRINT REPORT (OPTIONAL)
# =========================

def generate_report(df: pd.DataFrame) -> None:
    """
    Console report (optional, bukan core logic)
    """

    summary = generate_summary(df)
    top_prod = top_products(df)

    logger.info("===== BUSINESS REPORT =====")

    logger.info(f"Total Revenue: {summary['total_revenue']:,.0f}")
    logger.info(f"Total Orders: {summary['total_orders']}")
    logger.info(f"Avg Order Value: {summary['avg_order_value']:,.0f}")
    logger.info(f"Bulk Order Ratio: {summary['bulk_order_ratio']:.2%}")

    if not top_prod.empty:
        logger.info("Top Products:")
        for _, row in top_prod.iterrows():
            logger.info(f"{row['product_name']} -> {row['revenue']:,.0f}")

    logger.info("===== END REPORT =====")