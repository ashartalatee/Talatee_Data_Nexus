from extractor.fetcher import fetch_page
from extractor.parser import parse_quotes
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

    for page in range(START_PAGE, END_PAGE + 1):
        html = fetch_page(page)

        if not html:
            print(f"Skipping page {page} (fetch failed)")
            continue

        # Save raw HTML per page
        raw_file_path = f"{raw_path}/raw_page_{page}.html"
        with open(raw_file_path, "w", encoding="utf-8") as f:
            f.write(html)

        parsed_data = parse_quotes(html)
        print(f"Page {page}: {len(parsed_data)} items")

        all_data.extend(parsed_data)

    total_items = len(all_data)
    print(f"\nTotal collected: {total_items} items")

    # Save combined processed data
    processed_file_path = f"{processed_path}/all_quotes.json"
    with open(processed_file_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=4)

    # Save metadata
    metadata = {
        "run_id": run_id,
        "start_page": START_PAGE,
        "end_page": END_PAGE,
        "total_items": total_items
    }

    metadata_path = f"{processed_path}/metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)

    print("Run ID:", run_id)
    print("=== Process Finished ===")


if __name__ == "__main__":
    main()