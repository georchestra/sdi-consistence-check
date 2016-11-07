from urllib.parse import urlparse


class Credentials:
    def __init__(self):
        self._credentials = {}

    def add(self, site, username, password):
        self._credentials[site] = (username, password)

    def addFromUrl(self, url, username, password):
        u = urlparse(url)
        self.add(u.hostname, username, password)

    def get(self, site):
        try:
            return self._credentials[site]
        except KeyError:
            return (None, None)

    def getFromUrl(self, url):
        u = urlparse(url)
        return self.get(u.hostname)