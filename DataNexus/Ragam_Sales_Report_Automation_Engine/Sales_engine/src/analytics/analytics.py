import pandas as pd
import os
from src.utils.logger import get_logger

logger = get_logger()


# ==============================
# DAILY REVENUE
# ==============================
def daily_revenue(df):
    logger.info("Calculating daily revenue")

    result = (
        df.groupby("date")["revenue"]
        .sum()
        .reset_index()
        .sort_values("date")
    )

    return result


# ==============================
# TOP PRODUCTS
# ==============================
def top_products(df, top_n=5):
    logger.info("Calculating top products")

    result = (
        df.groupby("product")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index()
    )

    return result


# ==============================
# SOURCE PERFORMANCE
# ==============================
def source_performance(df):
    logger.info("Calculating source performance")

    result = (
        df.groupby("source")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    return result


# ==============================
# GROWTH ANALYSIS (UPGRADED)
# ==============================
def revenue_growth(df):
    logger.info("Calculating revenue growth")

    daily = daily_revenue(df)

    # ==============================
    # BASIC GROWTH
    # ==============================
    daily["prev_revenue"] = daily["revenue"].shift(1)

    daily["growth_pct"] = (
        (daily["revenue"] - daily["prev_revenue"]) /
        daily["prev_revenue"]
    ) * 100

    # ==============================
    # TREND ANALYSIS
    # ==============================
    pct_change = daily["revenue"].pct_change()

    avg_trend = pct_change.mean()
    volatility = pct_change.std()

    # ==============================
    # TREND CLASSIFICATION
    # ==============================
    if avg_trend > 0.1:
        trend_label = "UPTREND"
    elif avg_trend < -0.1:
        trend_label = "DOWNTREND"
    else:
        trend_label = "SIDEWAYS"

    # ==============================
    # VOLATILITY FLAG
    # ==============================
    high_volatility = volatility > 0.5

    # ==============================
    # BEST & WORST DAY
    # ==============================
    best_day = daily.loc[daily["revenue"].idxmax(), "date"]
    worst_day = daily.loc[daily["revenue"].idxmin(), "date"]

    summary = pd.DataFrame([{
        "avg_trend_pct": avg_trend * 100 if pd.notna(avg_trend) else 0,
        "volatility": volatility if pd.notna(volatility) else 0,
        "trend": trend_label,
        "high_volatility": high_volatility,
        "best_day": best_day,
        "worst_day": worst_day
    }])

    return daily, summary


# ==============================
# SAVE ANALYTICS
# ==============================
def save_analytics(df_dict, base_path="data/output/analytics"):
    logger.info("Saving analytics files")

    os.makedirs(base_path, exist_ok=True)

    for name, df in df_dict.items():
        path = os.path.join(base_path, f"{name}.csv")
        df.to_csv(path, index=False)

        logger.info(f"Saved {name} → {path}")


# ==============================
# FINAL ANALYTICS PIPELINE
# ==============================
def run_analytics(master_df):
    logger.warning("📊 Running Analytics Engine...")

    if master_df is None or master_df.empty:
        logger.warning("Master data kosong, analytics dilewati")
        return {}

    daily = daily_revenue(master_df)
    products = top_products(master_df)
    sources = source_performance(master_df)

    growth_detail, growth_summary = revenue_growth(master_df)

    results = {
        "daily_revenue": daily,
        "top_products": products,
        "source_performance": sources,
        "growth_detail": growth_detail,
        "growth_summary": growth_summary
    }

    save_analytics(results)

    logger.warning("📊 Analytics selesai")

    return results