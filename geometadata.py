import xml.etree.ElementTree as etree
from owslib.iso import MD_Metadata
from owslib.util import openURL
from requests import HTTPError


class GeoMetadata:

    def __init__(self, mdUrl):
        self.md = None
        self.errorMsg = None
        try:
            rawMd = openURL(mdUrl)
            content = rawMd.read()
            self.md = MD_Metadata(etree.fromstring(content))
        except HTTPError as e:
            if e.response.status_code == 404:
                self.errorMsg = "Metadata not found"
            else:
                self.errorMsg = "Unable to retrieve the metadata: %s" % e.strerror
        except etree.ParseError as e:
            self.errorMsg = "Unable to parse the metadata: %s" %e.msg

    def getMetadata(self):
        return self.md