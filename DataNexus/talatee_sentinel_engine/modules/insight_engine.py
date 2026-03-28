def generate_insight(sales_df, profit_df, ads_df, stock_df):
    insights = []

    # =========================
    # SALES INSIGHT
    # =========================
    top_product = sales_df.iloc[0]["product"]
    insights.append(f"🔥 Produk terlaris saat ini adalah {top_product}. Fokuskan stok dan promosi di sini.")

    # =========================
    # PROFIT INSIGHT
    # =========================
    best_profit = profit_df.iloc[0]["product"]
    insights.append(f"💰 Produk paling menguntungkan adalah {best_profit}. Ini kandidat utama untuk scale.")

    # =========================
    # ADS INSIGHT
    # =========================
    best_campaign = ads_df.iloc[0]["campaign"]
    insights.append(f"📢 Campaign terbaik adalah {best_campaign}. Pertimbangkan menaikkan budget.")

    bad_campaigns = ads_df[ads_df["roas"] < 1]
    if len(bad_campaigns) > 0:
        insights.append(f"⚠️ Ada {len(bad_campaigns)} campaign yang merugi. Segera evaluasi atau matikan.")

    # =========================
    # STOCK INSIGHT
    # =========================
    if len(stock_df) > 0:
        critical_product = stock_df.iloc[0]["product"]
        insights.append(f"📦 Produk {critical_product} stoknya hampir habis. Segera restock.")

    return insights


def save_insight(insights):
    with open("outputs/insight/insight_report.txt", "w") as f:
        for ins in insights:
            f.write(ins + "\n")

    print("🧠 Insight report generated!")