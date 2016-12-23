from urllib.request import urlopen, Request
import base64
from owslib.iso import MD_Metadata
from owslib.etree import etree

from inconsistency import GsMetadataMissingInconsistency, GsToGnMetadataInvalidInconsistency


def find_data_metadata(resource, credentials):
    """
    Retrieves and parse a remote metadata, given a gsconfig object (resource or layergroup).
    :param resource: an object from the gsconfig python library (either a resource or a layergroup)
    :param credentials: an object that store credential for various OGC services
    :return: a tuple (url, parsed metadata).
    """
    if resource.metadata_links is None:
        raise GsMetadataMissingInconsistency("%s:%s" % (resource.workspace.name, resource.name))
    for mime_type, md_format, url in resource.metadata_links:
        if mime_type == "text/xml" and md_format == "ISO19115:2003":
            # disable certificate verification
            # ctx = ssl.create_default_context()
            # ctx.check_hostname = False
            # ctx.verify_mode = ssl.CERT_NONE
            req = Request(url)
            username, password = credentials.getFromUrl(url)
            if username is not None:
                base64string = base64.b64encode(('%s:%s' % (username, password)).encode())
                authheader =  "Basic %s" % base64string.decode()
                req.add_header("Authorization", authheader)
            try:
                with urlopen(req) as fhandle:
                    return (url, MD_Metadata(etree.parse(fhandle)))
            except Exception as e:
                raise GsToGnMetadataInvalidInconsistency(url, str(e),
                                                         layer_name="%s:%s" % (resource.workspace.name, resource.name))

    raise GsMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)