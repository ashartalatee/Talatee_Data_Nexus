import requests
from config.settings import BASE_URL, HEADERS, TIMEOUT
from core.logger import log


def fetch_page():
    try:
        log("Sending HTTP request...")
        response = requests.get(BASE_URL, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()

        log(f"Success! Status code: {response.status_code}")
        return response.text

    except requests.exceptions.RequestException as e:
        log(f"Request failed: {e}")
        return None