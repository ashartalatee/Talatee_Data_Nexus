import os
import json
from extractor.fetcher import fetch_page
from extractor.parser import parse_quotes

def main():
    print("=== Clinic Data Engine Started ===")

    html = fetch_page()

    if html:
        # pastikan folder ada
        os.makedirs("data/raw", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # save raw
        with open("data/raw/raw_page_1.html", "w", encoding="utf-8") as f:
            f.write(html)

        parsed_data = parse_quotes(html)

        print(f"Parsed {len(parsed_data)} items")

        for item in parsed_data[:2]:
            print(item)

        # save processed
        with open("data/processed/parsed_page_1.json", "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, indent=4)

    print("=== Process Finished ===")

if __name__ == "__main__":
    main()