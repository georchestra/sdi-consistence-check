#!/usr/bin/env python3

from owslib.wms import WebMapService
from owslib.iso import MD_Metadata
from owslib.util import openURL
import xml.etree.ElementTree as etree

from requests import HTTPError


class OwsServer:

    def __init__(self, gsUrl):
        self._wms = WebMapService(gsUrl)
        self.populateLayers()


    def populateLayers(self):
        self.layersByWorkspace = {}
        for content in self._wms.contents:
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
        l = self._wms[layerName]
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


class Inconsistency:
    pass


class MetadataInconsistency(Inconsistency):
    def __init__(self, layerName, mdUrl):
        self.mdUrl = mdUrl
        self.layerName = layerName


    def __str__(self):
        return "Metadata %s not found or invalid for layer %s" % self.layerName, self.mdUrl


class MetadataMissingInconsistency(Inconsistency):
    def __init__(self, layerName):
        self.layerName = layerName

    def __str__(self):
        return "No metadata defined for layer %s" % self.layerName


if __name__ == "__main__":
    pigma = OwsServer("https://www.pigma.org/geoserver/wms")
    inconsistencies = []

    for workspace, layers in pigma.layersByWorkspace.items():
        for layer in layers:
            fqLayerName = "%s:%s" % (workspace, layer)
            mdUrls = pigma.getMetadataUrls(fqLayerName)
            if len(mdUrls) == 0:
                inconsistencies.append(MetadataMissingInconsistency(fqLayerName))
                continue
            for mdUrl in mdUrls:
                gmd = GeoMetadata(mdUrl)
                if gmd.errorMsg is not None:
                    inconsistencies.append(MetadataInconsistency(fqLayerName, mdUrl))
    print("Finished integrity check against WMS GetCapabilities")
    totalLayers = sum(len(v) for k,v in pigma.layersByWorkspace.items())
    print("%d layers parsed" % totalLayers)
    print("%d inconsistencies found" % len(inconsistencies))

