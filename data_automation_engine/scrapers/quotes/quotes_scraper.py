from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import json

from scrapers.base.base_scraper import BaseScraper
from scrapers.base.pagination import Pagination

from cleaning.data_cleaner import DataCleaner
from cleaning.data_validator import DataValidator


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

        self.logger.info(f"Starting scraper for {pages} pages")

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

        self.logger.info(f"Total quotes collected: {len(all_quotes)}")

        # =========================
        # VALIDATION
        # =========================
        validated_data = DataValidator.validate(all_quotes)

        # =========================
        # CLEANING
        # =========================
        clean_df = DataCleaner.clean_quotes(validated_data)

        # =========================
        # SAVE
        # =========================
        self.save(clean_df)


    def save(self, df):

        project_root = Path(__file__).resolve().parents[2]

        dataset_path = project_root / "datasets"
        dataset_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        csv_file = dataset_path / f"quotes_{timestamp}.csv"
        json_file = dataset_path / f"quotes_{timestamp}.json"

        # Save CSV
        df.to_csv(csv_file, index=False)

        # Save JSON
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(df.to_dict(orient="records"), f, indent=2, ensure_ascii=False)

        self.logger.info(f"CSV saved → {csv_file}")
        self.logger.info(f"JSON saved → {json_file}")