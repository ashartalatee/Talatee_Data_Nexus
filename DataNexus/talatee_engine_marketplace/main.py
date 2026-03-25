# main.py — Hari 8 Final Version

import pandas as pd
from src.loader import load_all_data
from src.validator import validate_data
from src.cleaner import clean_data
from src.transformer import transform_data
from src.aggregator import daily_summary
from src.analyzer import top_products
from src.reporter import save_report


def main():
    print("=" * 50)
    print("🚀 Talatee Engine Start (Multi-Source Mode) — Hari 8 Final")
    print("=" * 50)

    try:
        # =========================
        # 📦 LOAD DATA
        # =========================
        df = load_all_data()

        if df.empty:
            raise ValueError("Data kosong! Cek folder data/raw")

        print("\n📦 Data berhasil dimuat")
        print(df.head())

        print("\n📊 Jumlah data per source:")
        if 'source' in df.columns:
            print(df['source'].value_counts())
        else:
            print("⚠️ Kolom 'source' tidak ada, pastikan file CSV ada kolom source")

        # =========================
        # 🔍 VALIDATION
        # =========================
        print("\n🔍 Validating data...")
        df = validate_data(df)

        # =========================
        # 🧹 CLEANING
        # =========================
        print("\n🧹 Cleaning data...")
        df = clean_data(df)

        # =========================
        # 🔄 TRANSFORM & STANDARDIZATION
        # =========================
        print("\n🔄 Transforming & standardizing data...")
        df = transform_data(df)

        # Tambah kolom tambahan siap analisis
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['day_of_week'] = df['date'].dt.day_name()

        print("\n✅ Data after transform:")
        print(df.head())

        # =========================
        # 📊 AGGREGATION
        # =========================
        print("\n📊 Generating daily summary...")
        summary = daily_summary(df)
        print(summary.head())

        # =========================
        # 🧠 ANALYSIS
        # =========================
        print("\n🧠 Analyzing top products...")
        top = top_products(df)
        print("\n🏆 Top 5 Products:")
        print(top.head())

        # =========================
        # 💾 SAVE OUTPUT
        # =========================
        output_path = "output/daily/daily_summary.csv"
        save_report(summary, output_path)
        print(f"\n💾 Report saved to: {output_path}")

        print("\n✅ Pipeline berhasil jalan (Multi Source) — Hari 8 Final")
        print("=" * 50)

    except FileNotFoundError:
        print("\n❌ ERROR: Folder atau file tidak ditemukan")
        print("👉 Pastikan folder data/raw berisi file CSV")

    except pd.errors.EmptyDataError:
        print("\n❌ ERROR: File CSV kosong")

    except Exception as e:
        print("\n❌ Unexpected Error:", e)


if __name__ == "__main__":
    main()