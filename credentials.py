import os
from urllib.parse import urlparse


class Credentials:
    def __init__(self, logger=None):
        """
        Loads the credentials file, which consists of a text file
        formatted with "hostname username password" and whose default location is set to
        ~/.sdichecker.
        """
        self._credentials = {}

        try:
            with open(os.getenv("HOME") + "/.sdichecker") as file:
                for line in file:
                    try:
                        (hostname, user, password) = line.rstrip("\n").split(" ", 3)
                        self.add(hostname, user, password)
                    except ValueError:
                        pass
        except FileNotFoundError:
            if logger is not None:
                logger.info("No ~/.sdichecker file found, skipping credentials definition.")
            pass

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