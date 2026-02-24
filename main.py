import os
from extractor.fetcher import DataFetcher


def save_raw_html(content: str, filename: str):
    os.makedirs("data/raw", exist_ok=True)

    file_path = os.path.join("data/raw", filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"[INFO] HTML saved to {file_path}")


def main():
    # Ganti dengan target real nanti (sementara test dulu)
    url = "https://example.com"

    fetcher = DataFetcher(url)
    html = fetcher.fetch()

    if html:
        print("[INFO] Preview HTML (first 300 chars):")
        print(html[:300])

        save_raw_html(html, "page_1.html")
    else:
        print("[FAILED] Could not fetch HTML")


if __name__ == "__main__":
    main()