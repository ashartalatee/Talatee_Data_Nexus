def transform_data(df):
    print("\n🔄 TRANSFORMING DATA")

    # ===== STANDARD COLUMN MAPPING =====
    column_mapping = {
        'product_name': 'product',
        'item': 'product',

        'create_time': 'date',
        'order_date': 'date',

        'total_price': 'revenue',
        'amount': 'revenue',

        'qty': 'quantity'
    }

    df = df.rename(columns=column_mapping)

    # ===== PILIH KOLOM PENTING =====
    selected_columns = ['date', 'product', 'quantity', 'revenue', 'source']

    df = df[[col for col in selected_columns if col in df.columns]]

    return df