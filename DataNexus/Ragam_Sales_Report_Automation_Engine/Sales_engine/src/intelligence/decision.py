import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger("DECISION")


# ==============================
# PRODUCT RECOMMENDATION
# ==============================
def recommend_products(master_df):
    logger.info("Generating product recommendations...")

    if master_df.empty:
        logger.warning("Master data kosong")
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

    for _, row in top.iterrows():
        recs.append({
            "type": "SCALE_PRODUCT",
            "product": row["product"],
            "revenue": row["revenue"],
            "reason": "Top 30% revenue product"
        })

    for _, row in low.iterrows():
        recs.append({
            "type": "EVALUATE_PRODUCT",
            "product": row["product"],
            "revenue": row["revenue"],
            "reason": "Bottom 30% revenue product"
        })

    logger.info(f"Top products: {list(top['product'])}")
    logger.info(f"Low products: {list(low['product'])}")

    return recs


# ==============================
# SOURCE RECOMMENDATION (FIXED)
# ==============================
def recommend_source(master_df):
    logger.info("Evaluating source performance...")

    if master_df.empty:
        logger.warning("Master data kosong")
        return []

    source_perf = (
        master_df.groupby("source")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    if source_perf.empty:
        return []

    # ✅ FIX: handle single source
    if len(source_perf) == 1:
        source = source_perf.iloc[0]["source"]

        logger.info("Single source detected")

        return [{
            "type": "INFO",
            "source": source,
            "revenue": source_perf.iloc[0]["revenue"],
            "reason": "Only one channel available"
        }]

    best = source_perf.iloc[0]
    worst = source_perf.iloc[-1]

    logger.info(f"Best source: {best['source']}")
    logger.info(f"Worst source: {worst['source']}")

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
# GROWTH RECOMMENDATION (UPGRADED)
# ==============================
def recommend_growth(data):
    logger.info("Evaluating growth trend...")

    if data is None:
        return []

    # ✅ Ambil growth_summary (bukan daily lagi)
    if isinstance(data, dict) and "growth_summary" in data:
        summary = data["growth_summary"]
    else:
        summary = data

    if not isinstance(summary, pd.DataFrame):
        summary = pd.DataFrame(summary)

    if summary.empty:
        return []

    row = summary.iloc[0]

    avg_trend = row.get("avg_trend_pct", 0)
    volatility = row.get("volatility", 0)
    trend = row.get("trend", "UNKNOWN")

    # ==============================
    # DECISION LOGIC (SMART)
    # ==============================
    if trend == "DOWNTREND":
        logger.warning(f"Downtrend detected: {avg_trend:.2f}%")

        return [{
            "type": "WARNING",
            "message": f"Revenue downtrend ({avg_trend:.2f}%)",
            "reason": "Consistent decline over time"
        }]

    elif volatility > 0.5:
        logger.warning("High volatility detected")

        return [{
            "type": "UNSTABLE",
            "message": "Revenue unstable (high fluctuation)",
            "reason": "Inconsistent performance"
        }]

    elif trend == "UPTREND":
        logger.info(f"Uptrend detected: {avg_trend:.2f}%")

        return [{
            "type": "OPPORTUNITY",
            "message": f"Revenue growing ({avg_trend:.2f}%)",
            "reason": "Positive growth trend"
        }]

    logger.info("Stable trend detected")

    return [{
        "type": "STABLE",
        "message": f"Trend {trend}",
        "reason": "No significant change"
    }]


# ==============================
# PRIORITY ENGINE (UPGRADED)
# ==============================
def add_priority(decisions):
    for d in decisions:

        if d["type"] in ["WARNING"]:
            d["priority"] = "HIGH"

        elif d["type"] in ["OPPORTUNITY", "SCALE_PRODUCT"]:
            d["priority"] = "MEDIUM"

        elif d["type"] in ["UNSTABLE"]:
            d["priority"] = "MEDIUM"

        else:
            d["priority"] = "LOW"

    return decisions


# ==============================
# ACTION ENGINE (UPGRADED)
# ==============================
def add_action(decisions):
    for d in decisions:

        if d["type"] == "SCALE_PRODUCT":
            d["action"] = "Naikkan budget ads + tambah stok + buat variasi konten"

        elif d["type"] == "EVALUATE_PRODUCT":
            d["action"] = "Evaluasi harga / positioning / pertimbangkan stop"

        elif d["type"] == "FOCUS_SOURCE":
            d["action"] = "Scale channel ini (ads + konten + retargeting)"

        elif d["type"] == "IMPROVE_SOURCE":
            d["action"] = "Audit funnel, CTR, dan targeting"

        elif d["type"] == "WARNING":
            d["action"] = "Cek campaign, traffic drop, dan funnel error"

        elif d["type"] == "OPPORTUNITY":
            d["action"] = "Tambah budget & scale winning ads"

        elif d["type"] == "UNSTABLE":
            d["action"] = "Stabilkan ads & optimasi funnel conversion"

        elif d["type"] == "INFO":
            d["action"] = "Tambahkan channel baru untuk perbandingan"

        else:
            d["action"] = "Monitor performa"

    return decisions