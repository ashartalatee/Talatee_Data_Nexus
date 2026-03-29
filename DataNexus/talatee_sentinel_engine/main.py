from modules.loader import load_all_data
from modules.cleaner import clean_data
from modules.sales_analysis import sales_summary, top_products, sales_trend
from modules.profit_analysis import profit_report, loss_products
import os
from modules.ads_analysis import load_ads_data, ads_performance, bad_ads
from modules.operational_analysis import stock_alert, stock_summary
from modules.insight_engine import generate_insight, save_insight
from modules.report_engine import generate_report, save_report

def main():
    print("🚀 Talatee Marketplace Intelligence Engine Start\n")

    try:
        # =========================
        # 1. LOAD DATA
        # =========================
        print("📥 STEP 1: Loading Data...")
        df = load_all_data()

        if df.empty:
            raise ValueError("❌ Data kosong! Periksa folder data/raw")

        print(f"✅ Total Raw Rows: {len(df)}")

        # =========================
        # 2. CLEAN DATA
        # =========================
        print("\n🧹 STEP 2: Cleaning Data...")
        clean_df = clean_data(df)

        if clean_df.empty:
            raise ValueError("❌ Data setelah cleaning kosong!")

        print(f"✅ Total Clean Rows: {len(clean_df)}")

        # =========================
        # 3. SAVE CLEAN DATA
        # =========================
        print("\n💾 STEP 3: Saving Clean Data...")

        clean_dir = "data/clean"
        os.makedirs(clean_dir, exist_ok=True)

        clean_path = os.path.join(clean_dir, "clean_sales_final.csv")
        clean_df.to_csv(clean_path, index=False)

        print(f"✅ Clean data saved: {clean_path}")

        # =========================
        # 4. BASIC VALIDATION
        # =========================
        print("\n🔍 STEP 4: Business Validation...")

        total_revenue = clean_df["revenue"].sum()
        total_orders = clean_df["order_id"].nunique()
        total_products = clean_df["product"].nunique()

        print(f"💰 Total Revenue : {total_revenue:,.0f}")
        print(f"📦 Total Orders  : {total_orders}")
        print(f"🛍️ Total Products: {total_products}")

        # =========================
        # 5. SALES ANALYSIS
        # =========================
        print("\n📊 STEP 5: Sales Analysis...")

        summary = sales_summary(clean_df)
        top = top_products(clean_df)
        trend = sales_trend(clean_df)

        # =========================
        # PROFIT ANALYSIS
        # =========================
        profit = profit_report(clean_df)
        loss = loss_products(clean_df)

        profit.to_csv("outputs/reports/profit_report.csv", index=False)
        loss.to_csv("outputs/reports/loss_products.csv", index=False)

        print("\n💰 Profit analysis done!")

        # =========================
        # ADS ANALYSIS
        # =========================
        ads_df = load_ads_data()

        ads_perf = ads_performance(ads_df)
        bad = bad_ads(ads_df)

        ads_perf.to_csv("outputs/reports/ads_performance.csv", index=False)
        bad.to_csv("outputs/reports/bad_ads.csv", index=False)

        print("\n📢 Ads analysis done!")

        # =========================
        # OPERATIONAL ANALYSIS
        # =========================
        stock_alert_df = stock_alert(clean_df)
        stock_summary_df = stock_summary(clean_df)

        stock_alert_df.to_csv("outputs/alerts/stock_alert.csv", index=False)
        stock_summary_df.to_csv("outputs/alerts/low_stock_summary.csv", index=False)

        print("\n📦 Operational analysis done!")

        # =========================
        # INSIGHT ENGINE
        # =========================
        insights = generate_insight(
            sales_summary, 
            profit_summary, 
            ads_perf, 
            stock_alert_df
        )

        save_insight(insights)

        # =========================
        # FINAL REPORT
        # =========================
        report = generate_report(insights)
        save_report(report)

        # =========================
        # 6. SAVE REPORTS
        # =========================
        print("\n💾 STEP 6: Saving Reports...")

        report_dir = "outputs/reports"
        os.makedirs(report_dir, exist_ok=True)

        summary_path = os.path.join(report_dir, "sales_summary.csv")
        top_path = os.path.join(report_dir, "top_products.csv")
        trend_path = os.path.join(report_dir, "sales_trend.csv")

        summary.to_csv(summary_path, index=False)
        top.to_csv(top_path, index=False)
        trend.to_csv(trend_path, index=False)

        print(f"✅ Summary saved : {summary_path}")
        print(f"✅ Top products  : {top_path}")
        print(f"✅ Sales trend   : {trend_path}")

        # =========================
        # 7. QUICK INSIGHT (AUTO PRINT)
        # =========================
        print("\n🧠 STEP 7: Quick Insights...")

        best_product = top.iloc[0]

        print(f"🔥 Best Seller: {best_product['product']}")
        print(f"   Qty Sold  : {best_product['quantity']}")
        print(f"   Revenue   : {best_product['revenue']:,.0f}")

        print("\n🚀 ENGINE PHASE 1 + SALES ANALYSIS COMPLETE")

    except Exception as e:
        print("\n🚨 ERROR TERJADI:")
        print(e)


if __name__ == "__main__":
    main()