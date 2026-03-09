from scrapers.quotes.quotes_scraper import QuotesScraper
from scrapers.sources.news_scraper import NewsScraper


SCRAPER_REGISTRY = {
    "quotes": QuotesScraper,
    "news": NewsScraper,
}