from src.loader import load_all_data
from src.validator import validate_data
from src.cleaner import clean_data
from src.transformer import transform_data
from src.aggregator import daily_summary
from src.analyzer import top_products
from src.reporter import save_report

def main():
    print("🚀 Talatee Engine Start")

    try:
        # =========================
        # 1. LOAD DATA
        # =========================
        df = load_all_data()
        print("\n📥 Data Loaded:", df.shape)

        # =========================
        # 2. VALIDATE
        # =========================
        df = validate_data(df)

        # =========================
        # 3. CLEAN
        # =========================
        df = clean_data(df)
        print("\n🧹 After Cleaning:", df.shape)

        # =========================
        # 4. TRANSFORM
        # =========================
        df = transform_data(df)
        print("\n🔄 After Transform:", df.shape)

        # Preview dataset final
        print("\n📦 Final Dataset Preview:")
        print(df.head())

        # =========================
        # 5. SAVE FINAL DATASET
        # =========================
        save_report(df, "data/processed/final_dataset.csv")

        # =========================
        # 6. DAILY AGGREGATION
        # =========================
        summary = daily_summary(df)

        # Urutkan berdasarkan tanggal
        summary = summary.sort_values(by='date')

        print("\n📊 Daily Summary Preview:")
        print(summary.head())

        # Simpan hasil
        save_report(summary, "output/daily/daily_summary.csv")

        # =========================
        # 7. PRODUCT ANALYSIS (🔥 HARI 12)
        # =========================
        top = top_products(df, top_n=10)

        print("\n🛍️ Top Products Preview:")
        print(top)

        # Simpan hasil
        save_report(top, "output/product/top_products.csv")

        # =========================
        # 8. QUICK INSIGHT
        # =========================
        print("\n📈 Insight Harian:")
        print(summary.describe())

        print("\n📊 Insight Produk:")
        print(top.describe())

        print("\n✅ ENGINE HARI 12 SELESAI (PRODUCT ANALYTICS READY)")

    except Exception as e:
        print("❌ Error:", e)


if __name__ == "__main__":
    main()