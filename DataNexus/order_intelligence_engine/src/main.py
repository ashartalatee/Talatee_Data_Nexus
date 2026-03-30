from ingestion.load_data import load_all_data
from cleaning.standardize import standardize_columns, validate_columns

def main():
    print("Running Data Pipeline...")

    df = load_all_data()

    print("\n[STEP] Standardizing columns...")
    df = standardize_columns(df)

    df = validate_columns(df)

    print("\n=== AFTER STANDARDIZATION ===")
    print(df.head())

    print("\nColumns:", df.columns.tolist())

if __name__ == "__main__":
    main()