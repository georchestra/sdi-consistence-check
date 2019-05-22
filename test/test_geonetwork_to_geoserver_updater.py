import warnings
from xml.etree import ElementTree as etree

from owslib.iso import MD_Metadata

import GeonetworkToGeoserverUpdater

def testExtractAttribution():
    warnings.simplefilter("ignore", category=FutureWarning)
    with open("./test/md-rm.xml") as f:
        mdxml = etree.fromstring(f.read())
    md = MD_Metadata(md=mdxml)
    assert(GeonetworkToGeoserverUpdater.extract_attribution(md) == "source : Comités de secteur - Rennes Métropole")
