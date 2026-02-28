from extractor.fetcher import fetch_page
from extractor.parser import parse_quotes
from transformer.cleaner import remove_duplicates
from transformer.normalizer import normalize_quotes_data
from config.settings import START_PAGE, END_PAGE
from datetime import datetime
import os
import json


def get_run_id():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def main():
    print("=== Clinic Data Engine Started ===")

    run_id = get_run_id()

    raw_path = f"data/raw/{run_id}"
    processed_path = f"data/processed/{run_id}"

    os.makedirs(raw_path, exist_ok=True)
    os.makedirs(processed_path, exist_ok=True)

    all_data = []

    # ======================
    # FETCH & PARSE LOOP
    # ======================
    for page in range(START_PAGE, END_PAGE + 1):
        html = fetch_page(page)

        if not html:
            print(f"Skipping page {page} (fetch failed)")
            continue

        # Save raw HTML
        raw_file_path = f"{raw_path}/raw_page_{page}.html"
        with open(raw_file_path, "w", encoding="utf-8") as f:
            f.write(html)

        parsed_data = parse_quotes(html)
        print(f"Page {page}: {len(parsed_data)} items")

        all_data.extend(parsed_data)

    total_raw_items = len(all_data)
    print(f"\nTotal collected (raw): {total_raw_items} items")

    # ======================
    # DATA CLEANING
    # ======================
    print("\nRunning Deduplication...")

    clean_data = remove_duplicates(
        all_data,
        key_fields=["quote", "author"]
    )

    total_clean_items = len(clean_data)

    print(f"Before dedup: {total_raw_items}")
    print(f"After dedup : {total_clean_items}")

    # ======================
    # NORMALIZATION
    # ======================
    print("\nRunning Normalization...")

    normalized_data = normalize_quotes_data(clean_data)

    total_normalized_items = len(normalized_data)

    print(f"Total normalized items: {total_normalized_items}")

    # ======================
    # SAVE CLEANED DATA
    # ======================
    cleaned_file_path = f"{processed_path}/cleaned_quotes.json"

    with open(cleaned_file_path, "w", encoding="utf-8") as f:
        json.dump(normalized_data, f, indent=4)

    # ======================
    # SAVE METADATA
    # ======================
    metadata = {
        "run_id": run_id,
        "start_page": START_PAGE,
        "end_page": END_PAGE,
        "total_raw_items": total_raw_items,
        "total_clean_items": total_clean_items,
        "total_normalized_items": total_normalized_items,
        "normalization_applied": True,
        "raw_folder": raw_path,
        "processed_folder": processed_path,
        "status": "success"
    }

    metadata_path = f"{processed_path}/metadata.json"

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)

    print("\nRun ID:", run_id)
    print("=== Process Finished Successfully ===")


if __name__ == "__main__":
    main()