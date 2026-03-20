from src.utils.config import load_config
from src.ingestion.loader import load_all_data
from src.processing.validator import validate_all
from src.processing.cleaner import clean_all
from src.processing.standardization import standardize_all
from src.processing.merger import merge_all
from src.analytics.analytics import run_analytics   # ✅ NEW

def main():
    print("🚀 MSIDE Engine Starting...")

    config = load_config()

    dfs = load_all_data(config)
    validated = validate_all(dfs)
    cleaned = clean_all(validated)
    standardized = standardize_all(cleaned)

    master_df = merge_all(standardized)

    # 🔥 DAY 9
    analytics = run_analytics(master_df)

    print("\n📊 ANALYTICS READY")
    print("Top Products:")
    print(analytics["top_products"])

if __name__ == "__main__":
    main()