from .request_handler import RequestHandler


class BaseScraper:
    def __init__(self):
        self.request_handler = RequestHandler()

    def fetch(self, url):
        return self.request_handler.get(url)

    def parse(self, response):
        raise NotImplementedError

    def save(self, data):
        raise NotImplementedError