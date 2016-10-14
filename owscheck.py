import logging
from logging import Logger
from urllib.parse import urlparse

from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService

from credentials import Credentials
from geometadata import GeoMetadata
from inconsistency import *


class OwsServer:
    """
    Class which manages the consumption of OWS servers (WMS,WFS).
    """
    def __init__(self, gsurl, wms=True, creds = Credentials()):
        """
        constructor.

        :param gsurl (string): url to the OWS service endpoint, no query_string parameters are needed,
        :param wms (boolean): true if the service is a WMS one, false for WFS.
        :param creds (Credentials): an optional Credentials provider

        """
        u = urlparse(gsurl)
        (username, password) = creds.get(u.hostname)
        if wms:
            self._ows = WebMapService(gsurl, username=username, password=password, version="1.3.0")
        else:
            self._ows = WebFeatureService(gsurl, username=username, password=password, version="1.1.0")
        self._populateLayers()

    def _populateLayers(self):
        """
        populates the layersByWorkspace property, by consuming the GetCapabilities response.
        """
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

    def getMetadatas(self, layerName):
        """
        Given a layer name, returns the associated metadatas.

        :param layerName (string): the layer name
        :return: a set of tuples containing metadata URLs and format.
        """
        l = self._ows[layerName]
        return set([(i['format'], i['url']) for i in l.metadataUrls])


class OwsChecker:
    """
    Class which actually checks a OWS server.
    """
    logger = logging.getLogger("owschecker")

    def __init__(self, serviceUrl, wms=True, creds = Credentials()):
        self._inconsistencies = []

        try:
            self._service = OwsServer(serviceUrl, wms, creds)
        except BaseException as e:
            raise UnparseableGetCapabilitiesInconsistency(serviceUrl, str(e))

        for workspace, layers in self._service.layersByWorkspace.items():
            for layer in layers:
                fqLayerName = "%s:%s" % (workspace, layer)
                mdUrls = self._service.getMetadatas(fqLayerName)
                if len(mdUrls) == 0:
                    self._inconsistencies.append(MetadataMissingInconsistency(fqLayerName))
                    continue
                for (mdFormat, mdUrl) in mdUrls:
                    try:
                        GeoMetadata(mdUrl, mdFormat, creds=creds)
                    except MetadataInvalidInconsistency as e:
                        e.layerName = fqLayerName
                        self._inconsistencies.append(e)
        self.logger.info("Finished integrity check against WMS GetCapabilities")


    def getReport(self):
        totalLayers = sum(len(v) for k, v in self._service.layersByWorkspace.items())
        self.logger.info("%d layers parsed" % totalLayers)
        self.logger.info("%d inconsistencies found" % len(self._inconsistencies))

    def getInconsistencies(self):
        return self._inconsistencies
