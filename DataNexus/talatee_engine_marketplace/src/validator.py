import pandas as pd

def validate_data(df):
    print("\n🔍 VALIDATION REPORT")

    # ===== 1. Missing values =====
    print("\nMissing Values:")
    print(df.isnull().sum())

    # ===== 2. Data types =====
    print("\nData Types:")
    print(df.dtypes)

    # ===== 3. Duplicate =====
    print("\nDuplicate Rows:", df.duplicated().sum())

    # ===== 4. DATE VALIDATION =====
    print("\n📅 Date Validation")

    if 'date' in df.columns:
        df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')
        invalid_date = df['date_parsed'].isnull().sum()
        print(f"Invalid date rows: {invalid_date}")

    # ===== 5. NUMERIC VALIDATION =====
    print("\n💰 Numeric Validation")

    numeric_cols = ['price', 'quantity', 'revenue']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

            invalid = df[col].isnull().sum()
            negative = (df[col] < 0).sum()

            print(f"{col}: invalid={invalid}, negative={negative}")

    # ===== 6. BASIC SHAPE =====
    print("\nData Shape:", df.shape)

    return df