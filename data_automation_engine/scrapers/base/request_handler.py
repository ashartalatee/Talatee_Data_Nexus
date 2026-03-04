import requests
import time


class RequestHandler:
    def __init__(self, retries=3, timeout=10):
        self.retries = retries
        self.timeout = timeout

    def get(self, url):
        attempt = 0

        while attempt < self.retries:
            try:
                response = requests.get(url, timeout=self.timeout)

                if response.status_code == 200:
                    return response
                else:
                    print(f"[WARNING] Status Code: {response.status_code}")

            except requests.RequestException as e:
                print(f"[ERROR] Attempt {attempt + 1} failed: {e}")

            attempt += 1
            time.sleep(2)

        raise Exception("Max retries reached.")