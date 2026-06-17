import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from abc import ABC, abstractmethod

logger = logging.getLogger("TalateeEngine")

class BaseExtractor(ABC):
    def __init__(self, timeouts: tuple = (3.05, 10), max_retries: int = 3):
        self.timeouts = timeouts  # (connect timeout, read timeout)
        self.session = requests.Session()
        
        # Konfigurasi Exponential Backoff otomatis
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=2,  # Menunggu 2s, 4s, 8s antara percobaan ulang
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _safe_get(self, url: str, headers: dict = None, cookies: dict = None) -> dict:
        """
        Melakukan HTTP GET request dengan proteksi timeout dan penanganan error terisolasi.
        """
        try:
            response = self.session.get(
                url, 
                headers=headers, 
                cookies=cookies, 
                timeout=self.timeouts
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP Error terjadi saat mengambil data: {http_err}")
            raise http_err
        except requests.exceptions.ConnectionError as conn_err:
            logger.error(f"Network Connection Error terjadi: {conn_err}")
            raise conn_err
        except requests.exceptions.Timeout as timeout_err:
            logger.error(f"Request Timeout tercapai: {timeout_err}")
            raise timeout_err

    @abstractmethod
    def fetch_product_details(self, target_id: str) -> dict:
        """Wajib diimplementasikan oleh setiap subclass (Shopee, Tokopedia, TikTok)"""
        pass