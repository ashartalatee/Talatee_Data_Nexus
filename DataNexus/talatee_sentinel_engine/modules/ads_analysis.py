import pandas as pd

def load_ads_data():
    df = pd.read_csv("data/raw/ads.csv")

    # cleaning sederhana
    df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df


def ads_performance(df):
    print("📢 Analyzing ads performance...")

    df["roas"] = df["revenue"] / df["spend"]

    performance = (
        df.groupby("campaign")
        .agg({
            "spend": "sum",
            "revenue": "sum",
            "roas": "mean"
        })
        .reset_index()
    )

    return performance.sort_values(by="roas", ascending=False)


def bad_ads(df):
    print("⚠️ Detecting bad campaigns...")

    df["roas"] = df["revenue"] / df["spend"]

    bad = df[df["roas"] < 1]

    return bad.sort_values(by="roas")