def calculate_finance(df):
    print("\n💰 Calculating finance metrics...")

    df = df.copy()

    # === 1. REVENUE ===
    df["revenue"] = df["price"] * df["quantity"]

    # === 2. NET REVENUE ===
    df["net_revenue"] = df["revenue"] - df["fee"]

    # === 3. PROFIT (sementara = net) ===
    df["profit"] = df["net_revenue"]

    # === 4. PROFIT STATUS ===
    df["profit_status"] = df["profit"].apply(
        lambda x: "PROFIT" if x > 0 else "LOSS"
    )

    print("✅ Finance calculation selesai")

    return df