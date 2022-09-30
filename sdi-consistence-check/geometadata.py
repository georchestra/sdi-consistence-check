import xml.etree.ElementTree as etree

from owslib.iso import MD_Metadata
from owslib.util import openURL
from requests import HTTPError

from credentials import Credentials
from inconsistency import GsToGnMetadataInvalidInconsistency


class GeoMetadata:

    def __init__(self, mdUrl, mdFormat, creds = Credentials()):
        self.md = None
        self.errorMsg = None
        try:
            (username, password) = creds.getFromUrl(mdUrl)
            rawMd = openURL(mdUrl, username=username, password=password)
            content = rawMd.read()
            if mdFormat == "text/xml":
                self.md = MD_Metadata(etree.fromstring(content))
        except HTTPError as e:
            raise GsToGnMetadataInvalidInconsistency(mdUrl,
                                               "'%s' Metadata not found (HTTP %s): %s"
                                                     % (mdFormat, e.response.status_code, str(e)))
        except Exception as e:
            raise GsToGnMetadataInvalidInconsistency(mdUrl,
                                               "Unable to parse the %s metadata: %s" % (mdFormat, str(e)))

    def getMetadata(self):
        return self.md