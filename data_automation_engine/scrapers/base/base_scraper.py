from abc import ABC, abstractmethod
class BaseScraper(ABC):
    def __init__(self, base_url: str):
        self.base_url = base_url

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def parse(self, html):
        pass

    @abstractmethod
    def run(self):
        pass