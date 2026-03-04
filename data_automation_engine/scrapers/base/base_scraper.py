from scrapers.base.request_handler import RequestHandler
from framework.logger import setup_logger


class BaseScraper:
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
        self.request_handler = RequestHandler()

    def fetch(self, url):
        return self.request_handler.get(url)

    def parse(self, response):
        raise NotImplementedError

    def save(self, data):
        raise NotImplementedError