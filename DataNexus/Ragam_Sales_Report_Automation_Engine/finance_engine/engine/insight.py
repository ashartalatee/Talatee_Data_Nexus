def generate_insight(product_summary):
    print("\n🧠 Generating business insights...")

    df = product_summary.copy()

    # Sort by profit
    df = df.sort_values(by="profit", ascending=False).reset_index(drop=True)

    total_products = len(df)

    top_n = int(total_products * 0.3)
    bottom_n = int(total_products * 0.3)

    df["decision"] = "EVALUATE"

    # Assign decision
    df.loc[:top_n, "decision"] = "SCALE"
    df.loc[total_products - bottom_n:, "decision"] = "STOP"

    # Tambahkan rekomendasi
    def get_action(decision):
        if decision == "SCALE":
            return "Naikkan budget ads + tambah stok + buat variasi konten"
        elif decision == "STOP":
            return "Stop produk atau ganti strategi"
        else:
            return "Evaluasi harga, positioning, dan market"

    df["action"] = df["decision"].apply(get_action)

    print("✅ Insight selesai")

    print("\n📊 Decision Summary:")
    print(df[["product_name", "profit", "decision"]].head(10))

    return df