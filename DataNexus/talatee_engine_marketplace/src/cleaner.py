def clean_data(df):
    print("\n🧹 CLEANING DATA")

    # ===== 1. Hapus missing =====
    before = len(df)
    df = df.dropna()
    after = len(df)

    print(f"Removed missing rows: {before - after}")

    # ===== 2. Hapus duplicate =====
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)

    print(f"Removed duplicate rows: {before - after}")

    return df