import configparser
import ssl
from time import strftime, localtime
from urllib.request import urlopen, Request
import base64
from owslib.iso import MD_Metadata
from owslib.etree import etree

from inconsistency import GsMetadataMissingInconsistency, GsToGnMetadataInvalidInconsistency

def load_workspaces_mapping(file="./template/workspaces-mapping.ini.example"):
    """
    Reads the given file as a workspace mapping INI file
    :param file:
    :return: a dictionary which represents the loaded INI file
    """
    config = configparser.ConfigParser()
    config.read(file)
    ret = {}
    for elem in config.sections():
        ret[elem] = {
            'title_wms': config.get(elem, "title_wms"),
            'abstract_wms': config.get(elem, "abstract_wms"),
            'title_wfs': config.get(elem, "title_wfs"),
            'abstract_wfs': config.get(elem, "abstract_wfs")
        }
    return ret

def find_data_metadata(resource, credentials, no_ssl_check=False):
    """
    Retrieves and parse a remote metadata, given a gsconfig object (resource or layergroup).
    :param resource: an object from the gsconfig python library (either a resource or a layergroup)
    :param credentials: an object that store credential for various OGC services
    :param no_ssl_check: boolean indicating if SSL certificate check should be deactivated (False by default)
    :return: a tuple (url, parsed metadata).
    """
    if resource.metadata_links is None:
        raise GsMetadataMissingInconsistency("%s:%s" % (resource.workspace.name, resource.name))
    for mime_type, md_format, url in resource.metadata_links:
        if mime_type == "text/xml" and md_format == "ISO19115:2003":
            # disable certificate verification
            ctx = None
            if no_ssl_check:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            req = Request(url)
            username, password = credentials.getFromUrl(url)
            if username is not None:
                base64string = base64.b64encode(('%s:%s' % (username, password)).encode())
                authheader =  "Basic %s" % base64string.decode()
                req.add_header("Authorization", authheader)
            try:
                with urlopen(req, context=ctx) as fhandle:
                    return (url, MD_Metadata(etree.parse(fhandle)))
            except Exception as e:
                raise GsToGnMetadataInvalidInconsistency(url, str(e),
                                                         layer_name="%s:%s" % (resource.workspace.name, resource.name))

    raise GsMetadataMissingInconsistency(resource.workspace.name + ":" + resource.name)


def print_report(logger, errors):
    logger.info("\nProcessing ended, here is a summary of the collected errors:")
    if len(errors) == 0:
        logger.info("No error")
    else:
        for err in errors:
            logger.info("* %s", err)
    logger.info("\nEnd time: %s", strftime("%Y-%m-%d %H:%M:%S", localtime()))