import pandas as pd
import os


# ==============================
# 🔥 UPGRADE 1: DAILY REVENUE
# ==============================
def daily_revenue(df):
    print("📅 Calculating daily revenue...")

    result = (
        df.groupby("date")["revenue"]
        .sum()
        .reset_index()
        .sort_values("date")
    )

    return result


# ==============================
# 🔥 UPGRADE 2: TOP PRODUCTS
# ==============================
def top_products(df, top_n=5):
    print("🏆 Calculating top products...")

    result = (
        df.groupby("product")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index()
    )

    return result


# ==============================
# 🔥 UPGRADE 3: SOURCE PERFORMANCE
# ==============================
def source_performance(df):
    print("📊 Calculating source performance...")

    result = (
        df.groupby("source")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    return result


# ==============================
# 🔥 UPGRADE 4: GROWTH ANALYSIS
# ==============================
def revenue_growth(df):
    print("📈 Calculating revenue growth...")

    daily = daily_revenue(df)

    daily["prev_revenue"] = daily["revenue"].shift(1)
    daily["growth"] = (
        (daily["revenue"] - daily["prev_revenue"]) /
        daily["prev_revenue"]
    ) * 100

    return daily


# ==============================
# 🔥 UPGRADE 5: SAVE ANALYTICS
# ==============================
def save_analytics(df_dict, base_path="data/output/analytics"):
    print("💾 Saving analytics...")

    os.makedirs(base_path, exist_ok=True)

    for name, df in df_dict.items():
        path = f"{base_path}/{name}.csv"
        df.to_csv(path, index=False)
        print(f"✅ Saved {name} → {path}")


# ==============================
# 🔥 FINAL ANALYTICS PIPELINE
# ==============================
def run_analytics(master_df):
    print("🧠 Running analytics engine...")

    daily = daily_revenue(master_df)
    products = top_products(master_df)
    sources = source_performance(master_df)
    growth = revenue_growth(master_df)

    save_analytics({
        "daily_revenue": daily,
        "top_products": products,
        "source_performance": sources,
        "growth": growth
    })

    return {
        "daily_revenue": daily,
        "top_products": products,
        "source_performance": sources,
        "growth": growth
    }