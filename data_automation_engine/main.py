from scrapers.quotes.quotes_scraper import QuotesScraper


if __name__ == "__main__":
    scraper = QuotesScraper()
    scraper.run(pages=10)