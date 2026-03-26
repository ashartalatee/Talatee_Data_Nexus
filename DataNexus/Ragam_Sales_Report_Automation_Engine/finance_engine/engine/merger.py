import pandas as pd

def merge_data(df):
    print("\n🔗 Merging & Aggregating data...")

    df = df.copy()

    # === BASIC STATS ===
    total_revenue = df["revenue"].sum()
    total_profit = df["profit"].sum()
    total_orders = df["order_id"].nunique()

    print(f"💰 Total Revenue: {total_revenue}")
    print(f"📈 Total Profit: {total_profit}")
    print(f"📦 Total Orders: {total_orders}")

    # === PRODUCT SUMMARY ===
    product_summary = (
        df.groupby("product_name")
        .agg({
            "revenue": "sum",
            "profit": "sum",
            "quantity": "sum"
        })
        .sort_values(by="profit", ascending=False)
        .reset_index()
    )

    print("\n🏆 Top Profitable Products:")
    print(product_summary.head())

    return df, product_summary