from engine.loader import load_data
from engine.validator import validate_data
from engine.cleaner import clean_data
from engine.transformer import transform_data

def main():
    print("🚀 Finance Engine Starting...")

    df = load_data()

    clean_df, error_df = validate_data(df)

    cleaned_df = clean_data(clean_df)

    transformed_df = transform_data(cleaned_df)

    print("\n📊 Transformed Data Preview:")
    print(transformed_df.head())

    # Save output
    transformed_df.to_csv("data/output/transformed_data.csv", index=False)
    error_df.to_csv("data/output/error_rows.csv", index=False)

if __name__ == "__main__":
    main()