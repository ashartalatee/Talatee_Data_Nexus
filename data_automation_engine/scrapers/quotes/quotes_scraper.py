from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

from scrapers.base.base_scraper import BaseScraper
from scrapers.base.pagination import Pagination


class QuotesScraper(BaseScraper):

    BASE_URL = "http://quotes.toscrape.com/page/{}/"

    def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        quotes = []

        for quote_block in soup.find_all("div", class_="quote"):
            text = quote_block.find("span", class_="text").get_text(strip=True)
            author = quote_block.find("small", class_="author").get_text(strip=True)

            tags = [
                tag.get_text(strip=True)
                for tag in quote_block.find_all("a", class_="tag")
            ]

            quotes.append({
                "text": text,
                "author": author,
                "tags": ", ".join(tags)
            })

        return quotes


    def run(self, pages=10):

        all_quotes = []

        paginator = Pagination(self.BASE_URL)
        urls = paginator.build_urls(pages)

        for i, url in enumerate(urls, start=1):

            self.logger.info(f"Scraping page {i}")

            try:
                response = self.fetch(url)

                quotes = self.parse(response)

                if not quotes:
                    self.logger.warning(f"No quotes found on page {i}. Stopping.")
                    break

                all_quotes.extend(quotes)

            except Exception as e:
                self.logger.error(f"Failed on page {i}: {e}")
                continue

        self.save(all_quotes)


    def save(self, data):

        project_root = Path(__file__).resolve().parents[2]

        dataset_path = project_root / "datasets"
        dataset_path.mkdir(parents=True, exist_ok=True)

        file_path = dataset_path / "quotes.csv"

        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)

        self.logger.info(f"Data saved to {file_path}")