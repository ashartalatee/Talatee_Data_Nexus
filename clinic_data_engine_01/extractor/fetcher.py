import requests
from config.settings import BASE_URL, HEADERS, TIMEOUT

def fetch_page(page):
    url = BASE_URL.format(page)

    try:
        print(f"Fetching page {page}...")
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error on page {page}: {e}")
        return None