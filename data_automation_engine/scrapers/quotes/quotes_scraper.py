from bs4 import BeautifulSoup
import pandas as pd
import os

from scrapers.base.base_scraper import BaseScraper


class QuotesScraper(BaseScraper):

    BASE_URL = "http://quotes.toscrape.com/page/{}/"

    def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        quotes = []

        for quote_block in soup.find_all("div", class_="quote"):
            text = quote_block.find("span", class_="text").get_text(strip=True)
            author = quote_block.find("small", class_="author").get_text(strip=True)
            tags = [tag.get_text(strip=True) for tag in quote_block.find_all("a", class_="tag")]

            quotes.append({
                "text": text,
                "author": author,
                "tags": ", ".join(tags)
            })

        return quotes

    def run(self, pages=10):
        all_quotes = []

        for page in range(1, pages + 1):
            print(f"[INFO] Scraping page {page}")
            url = self.BASE_URL.format(page)
            response = self.fetch(url)
            quotes = self.parse(response)
            all_quotes.extend(quotes)

        self.save(all_quotes)

    def save(self, data):
        os.makedirs("data_automation_engine/datasets", exist_ok=True)
        df = pd.DataFrame(data)
        df.to_csv("data_automation_engine/datasets/quotes.csv", index=False)
        print("[SUCCESS] Data saved to datasets/quotes.csv")