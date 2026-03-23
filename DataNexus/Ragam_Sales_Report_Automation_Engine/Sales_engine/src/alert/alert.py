import pandas as pd
import os
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger()


# ==============================
# 🔥 HELPER (REUSABLE)
# ==============================
def calculate_change(df):
    df = df.sort_values("date").copy()
    df["prev"] = df["revenue"].shift(1)
    df["change"] = ((df["revenue"] - df["prev"]) / df["prev"]) * 100
    return df


# ==============================
# 🔥 REVENUE DROP DETECTION
# ==============================
def detect_revenue_drop(df, threshold=-20):
    logger.info("Detecting revenue drop...")

    alerts = []
    df = calculate_change(df)

    for _, row in df.iterrows():
        if pd.notnull(row["change"]) and row["change"] < threshold:
            alerts.append({
                "type": "REVENUE_DROP",
                "date": row["date"],
                "metric": "revenue",
                "value": round(row["change"], 2)
            })

    logger.info(f"Revenue drop alerts: {len(alerts)}")
    return alerts


# ==============================
# 🔥 SPIKE DETECTION
# ==============================
def detect_spike(df, threshold=50):
    logger.info("Detecting revenue spike...")

    alerts = []
    df = calculate_change(df)

    for _, row in df.iterrows():
        if pd.notnull(row["change"]) and row["change"] > threshold:
            alerts.append({
                "type": "REVENUE_SPIKE",
                "date": row["date"],
                "metric": "revenue",
                "value": round(row["change"], 2)
            })

    logger.info(f"Spike alerts: {len(alerts)}")
    return alerts


# ==============================
# 🔥 TOP PRODUCT CHANGE
# ==============================
def detect_top_product_change(df):
    logger.info("Detecting top product change...")

    alerts = []

    grouped = (
        df.groupby(["date", "product"])["revenue"]
        .sum()
        .reset_index()
    )

    top_per_day = (
        grouped.sort_values(["date", "revenue"], ascending=[True, False])
        .groupby("date")
        .first()
        .reset_index()
    )

    top_per_day["prev_product"] = top_per_day["product"].shift(1)

    for _, row in top_per_day.iterrows():
        if pd.notnull(row["prev_product"]) and row["product"] != row["prev_product"]:
            alerts.append({
                "type": "TOP_PRODUCT_CHANGE",
                "date": row["date"],
                "metric": "product",
                "value": row["product"]
            })

    logger.info(f"Top product change alerts: {len(alerts)}")
    return alerts


# ==============================
# 🔥 SAVE ALERT LOG
# ==============================
def save_alerts(alerts, path="data/output/alerts/alerts_log.csv"):
    if not alerts:
        logger.info("No alerts to save")
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)

    df = pd.DataFrame(alerts)
    df["created_at"] = datetime.now()

    if os.path.exists(path):
        existing = pd.read_csv(path)
        df = pd.concat([existing, df], ignore_index=True)

    df.to_csv(path, index=False)

    logger.info(f"Alerts saved: {len(df)} total records")


# ==============================
# 🔥 NOTIFICATION SYSTEM
# ==============================
def send_notification(alerts):
    if not alerts:
        logger.info("No critical alerts")
        return

    logger.info(f"Critical alerts count: {len(alerts)}")

    for alert in alerts:
        logger.debug(alert)


# ==============================
# 🔥 FINAL ALERT PIPELINE
# ==============================
def run_alerts(master_df, daily_df):
    logger.info("Running alert engine...")

    drop_alerts = detect_revenue_drop(daily_df)
    spike_alerts = detect_spike(daily_df)
    product_alerts = detect_top_product_change(master_df)

    all_alerts = drop_alerts + spike_alerts + product_alerts

    save_alerts(all_alerts)
    send_notification(all_alerts)

    # ❗ PENTING: JANGAN ADA WARNING DI SINI
    # biar tidak double di terminal

    return all_alerts