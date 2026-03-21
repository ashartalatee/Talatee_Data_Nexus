import os
import pandas as pd
from datetime import datetime

from src.intelligence.decision import (
    recommend_products,
    recommend_source,
    recommend_growth,
    add_priority,
    add_action
)

from src.utils.logger import setup_logger

logger = setup_logger("INTELLIGENCE")


# ==============================
# CONFIG
# ==============================
OUTPUT_DIR = "data/output/intelligence"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==============================
# NORMALIZE DATA (WAJIB)
# ==============================
def normalize_decisions(decisions):
    normalized = []

    for d in decisions:
        normalized.append({
            "priority": d.get("priority", ""),
            "type": d.get("type", ""),
            "product": d.get("product", ""),
            "source": d.get("source", ""),
            "revenue": d.get("revenue", ""),
            "message": d.get("message", ""),
            "action": d.get("action", ""),
            "reason": d.get("reason", "")
        })

    return normalized


# ==============================
# SAVE CSV (FINAL - SINGLE FILE)
# ==============================
def save_csv(decisions):
    path = os.path.join(OUTPUT_DIR, "final_report.csv")

    df = pd.DataFrame(decisions)

    columns_order = [
        "priority",
        "type",
        "product",
        "source",
        "revenue",
        "message",
        "action",
        "reason"
    ]

    df = df.reindex(columns=columns_order)

    df.to_csv(path, index=False)

    logger.info(f" FINAL CSV saved → {path}")


# ==============================
# SAVE HUMAN REPORT
# ==============================
def save_report(decisions):
    path = os.path.join(OUTPUT_DIR, "final_report.txt")

    with open(path, "w", encoding="utf-8") as f:
        f.write("=== BUSINESS INTELLIGENCE REPORT ===\n")
        f.write(f"Generated: {datetime.now()}\n\n")

        for d in decisions:
            f.write(f"[{d['priority']}] {d['type']}\n")

            if d["product"]:
                f.write(f"Product : {d['product']}\n")

            if d["source"]:
                f.write(f"Source  : {d['source']}\n")

            if d["message"]:
                f.write(f"Insight : {d['message']}\n")

            f.write(f"Action  : {d['action']}\n")
            f.write(f"Reason  : {d['reason']}\n")
            f.write("-" * 50 + "\n")

    logger.info(f" FINAL REPORT saved → {path}")


# ==============================
# MAIN ENGINE
# ==============================
def run_intelligence(master_df, analytics):

    logger.info(" Running FINAL Intelligence Engine...")

    if master_df is None or master_df.empty:
        logger.warning(" Master data kosong, engine dihentikan")
        return []

    # ======================
    # STEP 1 — DECISIONS
    # ======================
    product_decision = recommend_products(master_df)
    source_decision = recommend_source(master_df)
    growth_decision = recommend_growth(analytics)

    all_decisions = product_decision + source_decision + growth_decision

    if not all_decisions:
        logger.warning(" Tidak ada decision dihasilkan")
        return []

    # ======================
    # STEP 2 — ENRICHMENT
    # ======================
    all_decisions = add_priority(all_decisions)
    all_decisions = add_action(all_decisions)

    # ======================
    # STEP 3 — NORMALIZE
    # ======================
    all_decisions = normalize_decisions(all_decisions)

    # ======================
    # STEP 4 — SAVE OUTPUT
    # ======================
    save_csv(all_decisions)
    save_report(all_decisions)

    # ======================
    # STEP 5 — LOG FINAL
    # ======================
    logger.info(" FINAL DECISIONS:")
    for d in all_decisions:
        logger.info(d)

    return all_decisions