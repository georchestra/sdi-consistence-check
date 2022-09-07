import logging
import re
import warnings
import xml.etree.ElementTree as ET
from contextlib import suppress
from urllib.parse import urlparse

from owslib.csw import CatalogueServiceWeb, namespaces
from owslib.fes import PropertyIsEqualTo, Not, Or, And
from owslib.util import ServiceException

from credentials import Credentials
from inconsistency import Inconsistency, GnToGsNoGetCapabilitiesUrl
from owscheck import CachedOwsServices


class CSWQuerier:
    max_records = 100
    is_dataset = PropertyIsEqualTo("Type", "dataset")
    is_service = PropertyIsEqualTo("Type", "service")
    non_harvested = PropertyIsEqualTo("isHarvested", "false")

    protocol_regexp = re.compile("^OGC:(?P<type>WMS|WFS)(?:-(?P<version>\d+(?:\.\d+)*)(?:-[\w-]+)?)?$", re.IGNORECASE)

    def __init__(self, url, credentials=Credentials(),
                 cached_ows_services=None, logger=None, timeout=30):
        (username, password) = credentials.getFromUrl(url)
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger("cswquerier")
            self.logger.addHandler(logging.NullHandler())
        self.owsServices = cached_ows_services or CachedOwsServices(credentials=credentials, timeout=timeout)
        try:
            self.csw = CatalogueServiceWeb(url, username=username, password=password)
        except Exception as ex:
            raise ServiceException(ex)
        self.mds_not_parsable = []
        self.reset()

    def reset(self):
        self.start = 0
        self.md_count = -1

    def get_dataset_records(self, constraints=[]):
        self.csw.getrecords2(
            constraints=[And(constraints + [self.is_dataset])] if constraints else [self.is_dataset],
            esn='full',
            startposition=self.start,
            maxrecords=self.max_records,
        )
        self.logger.debug(
            "CSWQuerier.get_records() results : %s (start=%s, max=%s)",
            self.csw.results,
            self.start,
            self.max_records,
        )
        self.start += self.csw.results['returned']
        return self.csw.records

    def get_md(self, uuid):
        return self.csw.records[uuid]

    def get_service_mds(self, constraints=[]):
        # do not take care of FutureWarnings issued by OWSLib
        with warnings.catch_warnings():
            self.csw.getrecords2(
                constraints=[And(constraints + [self.is_dataset])] if constraints else [self.is_service],
                esn='full',
                outputschema=namespaces['gmd'],
                startposition=0,
                maxrecords=1000000,
            )
            return self.csw.records


    def get_data_mds(self, constraints=[]):
        self.csw.getrecords2(
            constraints=[And(constraints + [self.is_dataset])] if constraints else [self.is_dataset],
            esn='full',
            outputschema=namespaces['gmd'],
            startposition=0,
            maxrecords=1000000,
        )
        return self.csw.records

    def get_all_records(self, constraints=[]):
        """
        Gets all records, also managing the pagination against the remote CSW server.
        :param constraint: the constraint array to be passed to OWSLib getrecords2.
        :return: a hashmap with UUID as key, the parsed metadata as value.
        """
        startpos = 0
        mds = {}
        while True:
            self.csw.getrecords2(
                constraints,
                esn='full',
                outputschema=namespaces['gmd'],
                startposition=startpos,
                maxrecords=self.max_records,
            )
            for uuid in self.csw.records:
                mds[uuid] = self.csw.records[uuid]
            startpos = len(mds) + 1
            if startpos > self.csw.results['matches']:
                break
        return mds

    def check_service_md(self, mds, mdd, geoserver_to_check=[]):
        warnings.simplefilter("ignore")

        # check if this is an interesting service md (contains "coupledResource" or "operatesOn" tag)
        if len(mds.serviceidentification.operateson) == 0:
            # raise error ?
            return

        self.logger.info("\nData metadata: uuid %s \"%s\"", mdd.identifier, mdd.identification.title)
        self.logger.info("Service metadata: uuid %s \"%s\"", mds.identifier, mds.identification.title)

        # retrieve geoserver base URL (getCapabilities)
        url = None
        for op in mds.serviceidentification.operations:
            if op['name'] == "GetCapabilities":
                url = op['connectpoint'][0].url
                protocol = op['connectpoint'][0].protocol

        if url is None:
            raise GnToGsNoGetCapabilitiesUrl(mds.identifier, mdd.identifier)

        url_parsed = urlparse(url)
        if url_parsed.hostname not in geoserver_to_check:
            self.logger.info("\tSkipping : geoserver : %s not in authorized list (%s)" % (url_parsed.hostname, url))
            # do not raise error, since the underlying WxS server is not in the list of checked geoserver
            return

        # try to read protocol
        # TODO: which protocol to check ? is there a convention at Rennes-metropole on this ?
        matches = self.protocol_regexp.match(protocol)
        if matches is None:
            self.logger.error("Invalid protocol : %s " % protocol)
            # raise error ?
            return

        type = matches.group("type")
        version = matches.group("version")
        self.logger.debug("Server Type: %s Version: %s URL: %s" % (type, version, url))

        root = ET.fromstring(mds.xml.decode())
        xpath = ".//{http://www.isotc211.org/2005/srv}coupledResource"
        # xpath = ".//{http://www.isotc211.org/2005/srv}operatesOn"
        res = root.findall(xpath)
        for r in res:
            operation_name = identifier = layer_name = None
            with suppress(AttributeError):
                operation_name = r.find(
                    ".//{http://www.isotc211.org/2005/srv}operationName/{http://www.isotc211.org/2005/gco}CharacterString").text
                identifier = r.find(
                    ".//{http://www.isotc211.org/2005/srv}identifier/{http://www.isotc211.org/2005/gco}CharacterString").text
                layer_name = r.find(".//{http://www.isotc211.org/2005/gco}ScopedName").text
            if identifier is not None and layer_name is not None:
                self.logger.debug("\tcoupledRessources:")
                self.logger.debug("\tOperation : %s" % operation_name)
                self.logger.debug("\tidentifier : %s" % identifier)
                self.logger.debug("\tLayer Name: %s" % layer_name)
                self.logger.debug("")

                try:
                    if type.lower() == "wms":
                        self.owsServices.checkWmsLayer(url, layer_name)
                    elif type.lower() == "wfs":
                        self.owsServices.checkWfsLayer(url, layer_name)
                    else:
                        raise Inconsistency("Invalid service type : %s" % type)
                except Inconsistency as e:
                    e.md_uuid = mds.identifier
                    raise e
