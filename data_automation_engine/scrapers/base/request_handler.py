import requests

class RequestHandler:
    @staticmethod
    def get(url: str, timeout: int = 10):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Request error: {e}")
            return None