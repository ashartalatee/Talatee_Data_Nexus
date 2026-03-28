import pandas as pd

def profit_report(df):
    print("💰 Generating profit report...")

    report = (
        df.groupby("product")
        .agg({
            "revenue": "sum",
            "cost": "sum",
            "profit": "sum",
            "quantity": "sum"
        })
        .reset_index()
    )

    # hitung margin
    report["margin"] = report["profit"] / report["revenue"]

    return report.sort_values(by="profit", ascending=False)


def loss_products(df):
    print("⚠️ Detecting loss products...")

    loss = df[df["profit"] < 0]

    summary = (
        loss.groupby("product")
        .agg({
            "profit": "sum",
            "revenue": "sum"
        })
        .reset_index()
    )

    return summary.sort_values(by="profit")