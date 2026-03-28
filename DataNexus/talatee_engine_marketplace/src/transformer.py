import pandas as pd

def transform_data(df):
    print("\n🔄 FINAL TRANSFORM")

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

    # ===== PILIH KOLOM =====
    selected_columns = ['date', 'product', 'quantity', 'revenue', 'source']
    df = df[[col for col in selected_columns if col in df.columns]]

    # ===== FORMAT DATE =====
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # ===== FORMAT NUMERIC =====
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

    # ===== DROP DATA INVALID =====
    df = df.dropna(subset=['date', 'product', 'quantity', 'revenue'])

    # ===== TAMBAHAN FITUR =====
    df['day_of_week'] = df['date'].dt.day_name()

    return df