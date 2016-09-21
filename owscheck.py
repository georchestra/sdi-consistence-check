import logging
from logging import Logger

from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService

from GeoMetadata import GeoMetadata
from inconsistency import MetadataMissingInconsistency, MetadataInvalidInconsistency


class OwsServer:
    """
    Class which manages the consumption of OWS servers (WMS,WFS).
    """
    def __init__(self, gsurl, wms=True):
        """
        constructor.

        :param gsurl (string): url to the OWS service endpoint, no query_string parameters are needed,
        :param wms (boolean): true if the service is a WMS one, false for WFS.

        """
        if wms:
            self._ows = WebMapService(gsurl)
        else:
            self._ows = WebFeatureService(gsurl)
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

    def getMetadataUrls(self, layerName):
        """
        Given a layer name, returns the associated metadata URLs.

        :param layerName (string): the layer name
        :return: a set of metadata URL.
        """
        l = self._ows[layerName]
        return set([ i['url'] for i in l.metadataUrls ])

class OwsChecker:
    """
    Class which actually checks a OWS server.
    """
    logger = logging.getLogger("owschecker")

    def __init__(self, serviceUrl, wms=True):
        self._service = OwsServer(serviceUrl, wms)
        self._inconsistencies = []

        for workspace, layers in self._service.layersByWorkspace.items():
            for layer in layers:
                fqLayerName = "%s:%s" % (workspace, layer)
                mdUrls = self._service.getMetadataUrls(fqLayerName)
                if len(mdUrls) == 0:
                    self._inconsistencies.append(MetadataMissingInconsistency(fqLayerName))
                    continue
                for mdUrl in mdUrls:
                    gmd = GeoMetadata(mdUrl)
                    if gmd.errorMsg is not None:
                        self._inconsistencies.append(MetadataInvalidInconsistency(fqLayerName, mdUrl))
        self.logger.info("Finished integrity check against WMS GetCapabilities")


    def getReport(self):
        totalLayers = sum(len(v) for k, v in self._service.layersByWorkspace.items())
        self.logger.info("%d layers parsed" % totalLayers)
        self.logger.info("%d inconsistencies found" % len(self._inconsistencies))
