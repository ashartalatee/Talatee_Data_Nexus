from engine.loader import load_data
from engine.validator import validate_data
from engine.cleaner import clean_data
from engine.transformer import transform_data
from engine.standardizer import standardize_data
from engine.finance import calculate_finance
from engine.merger import merge_data
from engine.insight import generate_insight

def main():
    print("🚀 Finance Engine Starting...")

    df = load_data()

    clean_df, error_df = validate_data(df)

    cleaned_df = clean_data(clean_df)

    transformed_df = transform_data(cleaned_df)

    standardized_df = standardize_data(transformed_df)

    finance_df = calculate_finance(standardized_df)

    final_df, product_summary = merge_data(finance_df)

    insight_df = generate_insight(product_summary)

    print("\n📊 Final Insight Preview:")
    print(insight_df.head())

    # Save output
    final_df.to_csv("data/output/final_data.csv", index=False)
    product_summary.to_csv("data/output/product_summary.csv", index=False)
    insight_df.to_csv("data/output/final_report.csv", index=False)
    error_df.to_csv("data/output/error_rows.csv", index=False)

if __name__ == "__main__":
    main()