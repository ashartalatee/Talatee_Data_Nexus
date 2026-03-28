import pandas as pd

def sales_summary(df):
    print("📊 Generating sales summary...")

    total_orders = df["order_id"].nunique()
    total_revenue = df["revenue"].sum()
    total_items = df["quantity"].sum()

    summary = pd.DataFrame({
        "metric": ["total_orders", "total_items", "total_revenue"],
        "value": [total_orders, total_items, total_revenue]
    })

    return summary


def top_products(df):
    print("🔥 Finding top products...")

    top = (
        df.groupby("product")
        .agg({
            "quantity": "sum",
            "revenue": "sum"
        })
        .sort_values(by="quantity", ascending=False)
        .reset_index()
    )

    return top


def sales_trend(df):
    print("📈 Generating sales trend...")

    trend = (
        df.groupby("date")
        .agg({
            "revenue": "sum",
            "quantity": "sum"
        })
        .reset_index()
        .sort_values(by="date")
    )

    return trend