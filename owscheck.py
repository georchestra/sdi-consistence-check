import base64
import logging
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import requests
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

    def getLayer(self, name):
        return self._ows[name]


class CachedOwsServices:

    def __init__(self, credentials = Credentials(), disable_ssl=False):
        self._servers = { "wms" : {} , "wfs" : {} }
        self._credentials = credentials
        self._disable_ssl = disable_ssl

    def checkWfsLayer(self, url, name):
        self._checkLayer(url, name, is_wms=False)

    def checkWmsLayer(self, url, name):
        self._checkLayer(url, name, is_wms=True)

    def _check_legit_getcapabilities_url(self, url, name, is_wms):
        auth = None
        if self._credentials is not None:
            (username, password) = self._credentials.getFromUrl(url)
            if username is not None and password is not None:
                auth = (username,password)
        resp = requests.get(url, auth=auth, verify=not self._disable_ssl)
        str_url = resp.text
        first_tag = ET.fromstring(str_url).tag.lower()
        if (first_tag.endswith("wms_capabilities" if is_wms else "wfs_capabilities")):
            pass
        else:
            raise GnToGsInvalidCapabilitiesUrl(layer_name=name, layer_url=url, is_wms=is_wms)

    def _checkLayer(self, url, name, is_wms):
        servers_cache = self._servers["wms" if is_wms else "wfs"]
        if url not in servers_cache.keys():
           self._check_legit_getcapabilities_url(url, name, is_wms)
           try:
                servers_cache[url] = OwsServer(url, is_wms, creds=self._credentials)
           except BaseException as ex:
                raise GnToGsOtherError(layer_name=name,
                                       layer_url=url,
                                       exc=ex)
        try:
            servers_cache[url].getLayer(name)
        except KeyError:
            raise GnToGsLayerNotFoundInconsistency(layer_name=name, layer_url=url, msg="Layer not found on GS")


class OwsChecker:
    """
    Class which actually checks a OWS server.
    """
    logger = logging.getLogger("owschecker")

    def __init__(self, serviceUrl, wms=True, creds = Credentials()):
        self._inconsistencies = []
        self._layer_names = []
        try:
            self._service = OwsServer(serviceUrl, wms, creds)
        except BaseException as e:
            raise UnparseableGetCapabilitiesInconsistency(serviceUrl, str(e))

        layer_idx = 0
        for workspace, layers in self._service.layersByWorkspace.items():
            for layer in layers:
                fqLayerName = "%s:%s" % (workspace, layer)
                mdUrls = self._service.getMetadatas(fqLayerName)
                if len(mdUrls) == 0:
                    self._inconsistencies.append(GsMetadataMissingInconsistency(fqLayerName, layer_idx))
                    continue
                for (mdFormat, mdUrl) in mdUrls:
                    try:
                        GeoMetadata(mdUrl, mdFormat, creds=creds)
                    except GsToGnMetadataInvalidInconsistency as e:
                        e.layer_name = fqLayerName
                        e.layer_index = layer_idx
                        self._inconsistencies.append(e)
                self._layer_names.append(fqLayerName)
                layer_idx += 1


    def get_inconsistencies(self):
        return self._inconsistencies

    def get_service(self):
        return self._service

    def get_layer_names(self):
        return self._layer_names
