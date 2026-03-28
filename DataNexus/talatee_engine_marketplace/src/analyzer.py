import pandas as pd

def top_products(df, top_n=10):
    print("\n🧠 Analyzing top products...")

    # ✅ Validasi kolom wajib
    required_columns = ['product', 'revenue', 'quantity']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Kolom '{col}' tidak ditemukan")

    # ✅ Grouping + aggregation
    result = (
        df.groupby('product')
        .agg({
            'revenue': 'sum',
            'quantity': 'sum'
        })
        .reset_index()
    )

    # ✅ Hindari pembagian nol
    result['avg_price'] = result.apply(
        lambda x: x['revenue'] / x['quantity'] if x['quantity'] > 0 else 0,
        axis=1
    )

    # ✅ Sorting (produk terbaik di atas)
    result = result.sort_values(by='revenue', ascending=False)

    # ✅ Ranking
    result['rank'] = range(1, len(result) + 1)

    # ✅ Ambil Top N
    top_result = result.head(top_n)

    print(f"\n🏆 Top {top_n} Products:")
    print(top_result)

    return top_result