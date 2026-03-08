import argparse

from framework.config_loader import Config
from scrapers.registry import SCRAPER_REGISTRY


def main():

    parser = argparse.ArgumentParser(
        description="Talatee Data Automation Engine"
    )

    parser.add_argument(
        "scraper",
        type=str,
        help="Scraper name to run"
    )

    parser.add_argument(
        "--pages",
        type=int,
        default=Config.DEFAULT_PAGES,
        help="Number of pages to scrape",
    )

    args = parser.parse_args()

    scraper_name = args.scraper

    if scraper_name not in SCRAPER_REGISTRY:

        print("\nAvailable scrapers:")

        for name in SCRAPER_REGISTRY:
            print(f" - {name}")

        raise ValueError(f"\nScraper '{scraper_name}' not found")

    scraper_class = SCRAPER_REGISTRY[scraper_name]

    scraper = scraper_class()

    scraper.run(pages=args.pages)


if __name__ == "__main__":
    main()