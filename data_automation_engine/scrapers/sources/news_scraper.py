import requests
from bs4 import BeautifulSoup
import csv


class NewsScraper:

    BASE_URL = "https://www.antaranews.com/nasional"

    def fetch_page(self, page: int):

        url = f"{self.BASE_URL}?page={page}"

        response = requests.get(url, timeout=10)

        response.raise_for_status()

        return response.text


    def parse(self, html):

        soup = BeautifulSoup(html, "html.parser")

        articles = soup.select("h3 a")

        results = []

        for article in articles:

            title = article.text.strip()
            link = article["href"]

            results.append({
                "title": title,
                "link": link
            })

        return results


    def save_dataset(self, data):

        with open(
            "datasets/news_raw.csv",
            "w",
            newline="",
            encoding="utf-8"
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=["title", "link"]
            )

            writer.writeheader()
            writer.writerows(data)


    def run(self, pages=1):

        all_data = []

        for page in range(1, pages + 1):

            html = self.fetch_page(page)

            data = self.parse(html)

            all_data.extend(data)

            print(f"Page {page} scraped: {len(data)} items")

        self.save_dataset(all_data)

        print(f"\nTotal data scraped: {len(all_data)}")

        return all_data