class Pagination:

    def __init__(self, base_url):
        self.base_url = base_url

    def build_urls(self, pages=1):
        urls = []

        for page in range(1, pages + 1):
            url = self.base_url.format(page)
            urls.append(url)

        return urls