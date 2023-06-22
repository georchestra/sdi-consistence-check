import logging
import os
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from math import copysign

import requests
from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService
from owslib.util import ServiceException

from credentials import Credentials
from geometadata import GeoMetadata
from inconsistency import *


class OwsServer:
    """
    Class which manages the consumption of OWS servers (WMS,WFS).
    """
    def __init__(self, gsurl, wms = True, creds = Credentials(), timeout=30):
        """
        constructor.

        :param gsurl (string): url to the OWS service endpoint, no query_string parameters are needed,
        :param wms (boolean): true if the service is a WMS one, false for WFS.
        :param creds (Credentials): an optional Credentials provider

        """
        u = urlparse(gsurl)
        (username, password) = creds.get(u.hostname)
        if wms:
            self._ows = WebMapService(gsurl, username=username,
                                      password=password, version="1.3.0",
                                      timeout=timeout)
        else:
            self._ows = WebFeatureService(gsurl, username=username,
                                          password=password, version="1.1.0",
                                          timeout=timeout)
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
                try:
                    self.layersByWorkspace[None].append(content)
                except KeyError:
                    self.layersByWorkspace[None] = [content]
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
        try:
            return self._ows[name]
        # Not found ? try without workspace
        except KeyError:
            if ":" in name:
                (_, layername) = name.split(":")
                return self._ows[layername]

class CachedOwsServices:

    def __init__(self, credentials = Credentials(), disable_ssl=False, timeout=30):
        self._servers = { "wms" : {} , "wfs" : {} }
        self._credentials = credentials
        self._disable_ssl = disable_ssl
        self._timeout = timeout

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
        resp = requests.get(url, auth=auth, verify=not self._disable_ssl,
                            timeout=self._timeout)
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
           except Exception as ex:
                raise GnToGsOtherError(layer_name=name,
                                       layer_url=url,
                                       exc=ex)
        try:
            servers_cache[url].getLayer(name)
        except KeyError as ex:
            raise GnToGsLayerNotFoundInconsistency(layer_name=name, layer_url=url, msg="Layer not found on GS")


class OwsChecker:
    """
    Class which actually checks a OWS server.
    """
    logger = logging.getLogger("owschecker")

    def __init__(self, serviceUrl, wms=True, creds=Credentials(), checkLayers = False, timeout=30):
        self._inconsistencies = []
        self._layer_names = []
        self.wms = wms
        try:
            self._service = OwsServer(serviceUrl, wms, creds, timeout=timeout)
        except Exception as e:
            raise UnparseableGetCapabilitiesInconsistency(serviceUrl, str(e))

        layer_idx = 0
        for workspace, layers in self._service.layersByWorkspace.items():
            for layer in layers:
                if workspace is not None:
                    fqLayerName = "%s:%s" % (workspace, layer)
                else:
                    fqLayerName = layer
                self._layer_names.append(fqLayerName)

                if checkLayers:
                    # depending on OWS type, we'll have to check a different URL
                    # either a GetMap or a GetFeature
                    l = self._service.getLayer(fqLayerName)
                    if self._service._ows.identification.type == "WMS":
                        try:
                            a = self._service._ows.getmap(layers=[fqLayerName],
                                srs='EPSG:4326',
                                format='image/png',
                                size=(10,10),
                                bbox=self._reduced_bbox(l.boundingBoxWGS84))
                        except ServiceException as e:
                            e.layer_name = fqLayerName
                            e.layer_index = layer_idx
                            self._inconsistencies.append(e)
                    else:
                        try:
                            a = self._service._ows.getfeature(typename=fqLayerName,
                                srsname=l.crsOptions[0],
                                bbox=self._reduced_bbox(l.boundingBoxWGS84),
                                maxfeatures=1)
                        except ServiceException as e:
                            e.layer_name = fqLayerName
                            e.layer_index = layer_idx
                            self._inconsistencies.append(e)

                mdUrls = self._service.getMetadatas(fqLayerName)
                if len(mdUrls) == 0:
                    self._inconsistencies.append(GsMetadataMissingInconsistency(fqLayerName, layer_idx))
                    layer_idx += 1
                    continue
                for (mdFormat, mdUrl) in mdUrls:
                    try:
                        GeoMetadata(mdUrl, mdFormat, creds=creds)
                    except GsToGnMetadataInvalidInconsistency as e:
                        e.layer_name = fqLayerName
                        e.layer_index = layer_idx
                        self._inconsistencies.append(e)
                layer_idx += 1

    def get_inconsistencies(self):
        return self._inconsistencies

    def get_service(self):
        return self._service

    def get_layer_names(self):
        return self._layer_names

    def _reduced_bbox(self, bbox):
        xmin, ymin, xmax, ymax = bbox
        return [xmin+0.49*(xmax-xmin),
             ymin+0.49*(ymax-ymin),
             xmax-0.49*(xmax-xmin),
             ymax-0.49*(ymax-ymin)]

