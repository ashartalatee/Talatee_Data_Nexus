import argparse
from scrapers.quotes.quotes_scraper import QuotesScraper
from framework.config_loader import Config


def main():
    parser = argparse.ArgumentParser(description="Data Automation Engine")
    parser.add_argument(
        "--pages",
        type=int,
        default=Config.DEFAULT_PAGES,
        help="Number of pages to scrape",
    )

    args = parser.parse_args()

    scraper = QuotesScraper()
    scraper.run(pages=args.pages)


if __name__ == "__main__":
    main()