import requests
import time
import logging

from framework.config_loader import Config
from framework.logger import setup_logger

logger = setup_logger(__name__, getattr(logging, Config.LOG_LEVEL))


class RequestHandler:
    def __init__(self):
        self.retries = Config.REQUEST_RETRIES
        self.timeout = Config.REQUEST_TIMEOUT
        self.delay = Config.RATE_LIMIT_DELAY

    def get(self, url):
        for attempt in range(1, self.retries + 1):
            try:
                logger.info(f"Requesting URL: {url}")
                response = requests.get(url, timeout=self.timeout)

                if response.status_code == 200:
                    time.sleep(self.delay)
                    return response

                logger.warning(f"Status {response.status_code} on {url}")

            except requests.RequestException as e:
                logger.error(f"Attempt {attempt} failed: {e}")

            time.sleep(self.delay)

        raise Exception(f"Max retries reached for {url}")