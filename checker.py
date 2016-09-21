#!/usr/bin/env python3

import os, sys

from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService
from owslib.iso import MD_Metadata
from owslib.util import openURL
import xml.etree.ElementTree as etree
from requests import HTTPError

from inconsistency import MetadataInvalidInconsistency, MetadataMissingInconsistency


class OwsServer:
    """
    Class which manages the consumption of OWS servers (WMS,WFS)
    """
    def __init__(self, gsurl, wms=True):
        if wms:
            self._ows = WebMapService(gsurl)
        else:
            self._ows = WebFeatureService(gsurl)
        self.populateLayers()


    def populateLayers(self):
        self.layersByWorkspace = {}
        for content in self._ows.contents:
            # if the workspace is not guessable from the layer name,
            # skip it.
            try:
                (workspace, layer) = content.split(":", maxsplit=1)
                try:
                    self.layersByWorkspace[workspace].append(layer)
                except KeyError:
                    self.layersByWorkspace[workspace] = [layer]
            except ValueError:
                pass


    def getMetadataUrls(self, layerName):
        l = self._ows[layerName]
        return set([ i['url'] for i in l.metadataUrls ])


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


if __name__ == "__main__":
    # wms_service_url = os.getenv('WMS_SERVICE')
    wms_service_url = 'https://www.pigma.org/geoserver/ows'
    if not wms_service_url:
        sys.exit("Missing WMS_SERVICE environment variable")
    print("Querying %s ..." % wms_service_url)
    service = OwsServer(wms_service_url, wms=False)
    inconsistencies = []

    for workspace, layers in service.layersByWorkspace.items():
        for layer in layers:
            fqLayerName = "%s:%s" % (workspace, layer)
            mdUrls = service.getMetadataUrls(fqLayerName)
            if len(mdUrls) == 0:
                inconsistencies.append(MetadataMissingInconsistency(fqLayerName))
                continue
            for mdUrl in mdUrls:
                gmd = GeoMetadata(mdUrl)
                if gmd.errorMsg is not None:
                    inconsistencies.append(MetadataInvalidInconsistency(fqLayerName, mdUrl))
    print("Finished integrity check against WMS GetCapabilities")
    totalLayers = sum(len(v) for k,v in service.layersByWorkspace.items())
    print("%d layers parsed" % totalLayers)
    print("%d inconsistencies found" % len(inconsistencies))

