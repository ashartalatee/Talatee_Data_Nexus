import pandas as pd

def stock_alert(df, threshold=50):
    print("📦 Checking stock levels...")

    low_stock = df[df["stock"] < threshold]

    alert = (
        low_stock.groupby("product")
        .agg({
            "stock": "min",
            "quantity": "sum"
        })
        .reset_index()
    )

    alert = alert.sort_values(by="stock")

    return alert


def stock_summary(df):
    print("📊 Generating stock summary...")

    summary = (
        df.groupby("product")
        .agg({
            "stock": "mean",
            "quantity": "sum"
        })
        .reset_index()
    )

    return summary.sort_values(by="stock")