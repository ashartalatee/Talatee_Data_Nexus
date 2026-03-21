import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger("DECISION")


# ==============================
# PRODUCT RECOMMENDATION
# ==============================
def recommend_products(master_df):
    logger.info(" Generating product recommendations...")

    if master_df.empty:
        logger.warning(" Master data kosong")
        return []

    product_perf = (
        master_df.groupby("product")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    total_products = len(product_perf)

    if total_products == 0:
        return []

    split = max(1, int(total_products * 0.3))

    top = product_perf.head(split)
    low = product_perf.tail(split)

    # remove overlap
    low = low[~low["product"].isin(top["product"])]

    recs = []

    # TOP PRODUCTS
    for _, row in top.iterrows():
        recs.append({
            "type": "SCALE_PRODUCT",
            "product": row["product"],
            "revenue": row["revenue"],
            "reason": "Top 30% revenue product"
        })

    # LOW PRODUCTS
    for _, row in low.iterrows():
        recs.append({
            "type": "EVALUATE_PRODUCT",
            "product": row["product"],
            "revenue": row["revenue"],
            "reason": "Bottom 30% revenue product"
        })

    logger.info(f" Top products: {list(top['product'])}")
    logger.info(f" Low products: {list(low['product'])}")

    return recs


# ==============================
# SOURCE RECOMMENDATION
# ==============================
def recommend_source(master_df):
    logger.info(" Evaluating source performance...")

    if master_df.empty:
        logger.warning(" Master data kosong")
        return []

    source_perf = (
        master_df.groupby("source")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    if source_perf.empty:
        return []

    best = source_perf.iloc[0]
    worst = source_perf.iloc[-1]

    logger.info(f" Best source: {best['source']}")
    logger.info(f" Worst source: {worst['source']}")

    return [
        {
            "type": "FOCUS_SOURCE",
            "source": best["source"],
            "revenue": best["revenue"],
            "reason": "Highest revenue channel"
        },
        {
            "type": "IMPROVE_SOURCE",
            "source": worst["source"],
            "revenue": worst["revenue"],
            "reason": "Lowest revenue channel"
        }
    ]


# ==============================
# GROWTH RECOMMENDATION
# ==============================
def recommend_growth(data):
    logger.info(" Evaluating growth trend...")

    if data is None:
        return []

    # handle dict / dataframe
    if isinstance(data, dict) and "daily_revenue" in data:
        daily_df = data["daily_revenue"]
    else:
        daily_df = data

    if not isinstance(daily_df, pd.DataFrame):
        daily_df = pd.DataFrame(daily_df)

    if daily_df.empty:
        return []

    if "growth" not in daily_df.columns:
        daily_df["growth"] = daily_df["revenue"].pct_change() * 100

    latest = daily_df.iloc[-1]

    growth_value = latest.get("growth", 0)

    if pd.isna(growth_value):
        return []

    if growth_value < 0:
        logger.warning(f" Revenue declining: {growth_value:.2f}%")
        return [{
            "type": "WARNING",
            "message": f"Revenue down {growth_value:.2f}%"
        }]

    elif growth_value > 20:
        logger.info(f" Revenue spike: {growth_value:.2f}%")
        return [{
            "type": "OPPORTUNITY",
            "message": f"Revenue up {growth_value:.2f}%"
        }]

    logger.info(f" Revenue stable: {growth_value:.2f}%")
    return [{
        "type": "STABLE",
        "message": f"Growth {growth_value:.2f}%"
    }]


# ==============================
# PRIORITY ENGINE
# ==============================
def add_priority(decisions):
    for d in decisions:

        if d["type"] in ["WARNING"]:
            d["priority"] = "HIGH"

        elif d["type"] in ["OPPORTUNITY", "SCALE_PRODUCT"]:
            d["priority"] = "MEDIUM"

        else:
            d["priority"] = "LOW"

    return decisions


# ==============================
# ACTION ENGINE
# ==============================
def add_action(decisions):
    for d in decisions:

        if d["type"] == "SCALE_PRODUCT":
            d["action"] = "Naikkan budget ads + tambah stok + buat variasi konten"

        elif d["type"] == "EVALUATE_PRODUCT":
            d["action"] = "Evaluasi harga / positioning / pertimbangkan stop"

        elif d["type"] == "FOCUS_SOURCE":
            d["action"] = "Double down channel ini (ads + organic)"

        elif d["type"] == "IMPROVE_SOURCE":
            d["action"] = "Audit funnel, konten, dan targeting channel ini"

        elif d["type"] == "WARNING":
            d["action"] = "Cek penurunan: produk, traffic, atau campaign error"

        elif d["type"] == "OPPORTUNITY":
            d["action"] = "Scale sekarang sebelum momentum hilang"

        else:
            d["action"] = "Monitor performa"

    return decisions