# src/analytics/metrics.py

import pandas as pd


def total_revenue(df: pd.DataFrame) -> float:
    return df["revenue"].sum()


def total_orders(df: pd.DataFrame) -> int:
    return len(df)


def top_products(df: pd.DataFrame, n=5) -> pd.DataFrame:
    return (
        df.groupby("product_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )


def bulk_order_ratio(df: pd.DataFrame) -> float:
    if "is_bulk_order" not in df.columns:
        return 0

    return df["is_bulk_order"].sum() / len(df)


def generate_report(df: pd.DataFrame):
    print("\n📊 ===== BUSINESS REPORT =====")

    revenue = total_revenue(df)
    orders = total_orders(df)
    bulk_ratio = bulk_order_ratio(df)
    top_prod = top_products(df)

def get_top_products(df):
    return (
        df.groupby("product_name")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    print(f"\n💰 Total Revenue: {revenue:,.0f}")
    print(f"🧾 Total Orders: {orders}")
    print(f"📦 Bulk Order Ratio: {bulk_ratio:.2%}")

    print("\n🔥 Top Products:")
    print(top_prod.to_string(index=False))

    print("\n📊 ===== END REPORT =====\n")