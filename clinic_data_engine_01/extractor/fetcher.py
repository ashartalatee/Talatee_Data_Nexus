import requests


class DataFetcher:
    def __init__(self, url: str):
        self.url = url

    def fetch(self) -> str | None:
        try:
            print(f"[INFO] Fetching URL: {self.url}")

            response = requests.get(self.url, timeout=10)

            if response.status_code == 200:
                print("[SUCCESS] Data fetched successfully")
                return response.text
            else:
                print(f"[ERROR] Failed with status code: {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            print("[ERROR] Request timeout")
            return None

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Request failed: {e}")
            return None